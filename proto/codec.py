"""
proto/codec.py — Betclic gRPC-web pure codec layer.

No network I/O. Handles:
  - Protobuf encoding (varint, field_int, field_str, grpc_frame)
  - Protobuf decoding (read_varint, decode_proto)
  - Frame analysis (analyze_frame)
  - Clean JSON extraction (build_clean_json)
  - Live state extraction (extract_live_state)
  - gRPC response parsing (parse_grpc_response)
  - Match listing parser (parse_matches_frame)
  - Payload builders (payload_live_count, payload_get_match, payload_list_matches)
"""


import struct, sys, json, requests
import urllib3
from typing import Any

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ──────────────────────────────────────────────────────────────────────────────
# KNOWN SCHEMA (from screenshot + URL reverse-engineering)
# ──────────────────────────────────────────────────────────────────────────────

MARKET_CODES = {
    "ca_ftb_goa":    "Goals Over/Under",
    "ca_ftb_goalm":  "Goals Method",
    "ca_ftb_cshcp":  "Correct Score / Handicap",
    "ca_ftb_prp":    "Player Props",
    "ca_ftb_gsc":    "Goal Scorers",
    "ca_ftb_rslt":   "Match Result (1X2)",
}

# Market filter keys for --market param (maps short alias → proto field 3 value)
# Betclic uses these as the 3rd field in GetMatchWithNotification payload
# to request only a specific market category.
MARKET_FILTERS = {
    "top":    "ca_ftb_top",    # Top / featured markets (1X2, O/U 2.5, ...)
    "rslt":   "ca_ftb_rslt",   # Match result (1X2)
    "gsc":    "ca_ftb_gsc",    # Goal scorers (buteurs)
    "goa":    "ca_ftb_goa",    # Goals Over/Under
    "goal":   "ca_ftb_goa",    # alias for goa
    "cshcp":  "ca_ftb_cshcp",  # Correct score + handicap
}

CONTESTANT_IDS = {
    "5438414804710380J": "Bayern Munich",
    "5438444252606822R": "Atalanta",
}

# Match URL format: https://www.betclic.fr/football-sfootball/<comp>-c<N>/<slug>-m<MATCH_ID>

# Known football competition codes (field 2 in GetMatchesBySportWithNotifications)
# Discovered by capturing network requests while scrolling the Betclic football page
FOOTBALL_COMPETITIONS = [
    "ftb_rsm",    # probably top/featured matches
    # add more as discovered — or use None to request all at once
]
MATCH_URL_BASE = "https://www.betclic.fr/football-sfootball"

# ──────────────────────────────────────────────────────────────────────────────
# PROTOBUF ENCODER
# ──────────────────────────────────────────────────────────────────────────────

def varint(v: int) -> bytes:
    buf = b''
    while True:
        t = v & 0x7F
        v >>= 7
        buf += bytes([t | (0x80 if v else 0x00)])
        if not v:
            break
    return buf

def field_int(fn: int, v: int) -> bytes:
    return varint((fn << 3) | 0) + varint(v)

def field_str(fn: int, v: str) -> bytes:
    enc = v.encode('utf-8')
    return varint((fn << 3) | 2) + varint(len(enc)) + enc

def grpc_frame(proto: bytes) -> bytes:
    """Wrap protobuf in a gRPC-web uncompressed data frame."""
    return b'\x00' + struct.pack('>I', len(proto)) + proto

# ──────────────────────────────────────────────────────────────────────────────
# PROTOBUF DECODER
# ──────────────────────────────────────────────────────────────────────────────

def read_varint(data: bytes, pos: int) -> tuple[int, int]:
    result, shift = 0, 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return result, pos


def decode_proto(data: bytes, depth: int = 0) -> dict:
    """Recursive best-effort protobuf decoder."""
    fields: dict[int, list] = {}
    pos = 0
    while pos < len(data):
        try:
            tag, pos = read_varint(data, pos)
            fn = tag >> 3
            wt = tag & 7
        except Exception:
            break
        try:
            if wt == 0:          # varint
                val, pos = read_varint(data, pos)
                fields.setdefault(fn, []).append(val)

            elif wt == 2:        # length-delimited
                ln, pos = read_varint(data, pos)
                raw = data[pos:pos+ln]; pos += ln
                try:
                    text = raw.decode('utf-8')
                    # Treat as nested proto if it contains binary control bytes
                    if any(c < 0x09 for c in raw) and depth < 6:
                        raise ValueError
                    fields.setdefault(fn, []).append(text)
                except (UnicodeDecodeError, ValueError):
                    try:
                        sub = decode_proto(raw, depth + 1)
                        fields.setdefault(fn, []).append(sub if sub else raw.hex())
                    except Exception:
                        fields.setdefault(fn, []).append(raw.hex())

            elif wt == 1:        # 64-bit fixed (double or int64)
                raw8 = data[pos:pos+8]; pos += 8
                val_d = struct.unpack('<d', raw8)[0]
                val_i = struct.unpack('<Q', raw8)[0]   # unsigned int view
                fields.setdefault(fn, []).append(('fixed64', val_d, val_i))

            elif wt == 5:        # 32-bit fixed (float or int)
                raw4 = data[pos:pos+4]; pos += 4
                val_f = struct.unpack('<f', raw4)[0]
                val_i = struct.unpack('<I', raw4)[0]   # unsigned int view
                # Store the float; keep raw int for fallback
                fields.setdefault(fn, []).append(('fixed32', val_f, val_i))

            else:
                break
        except Exception:
            break
    return fields


def annotate(v: Any) -> str:
    """Return a human-readable annotation for known values."""
    if not isinstance(v, str):
        return ""
    for code, label in MARKET_CODES.items():
        if code in v:
            return f"  ← {label}"
    for cid, name in CONTESTANT_IDS.items():
        if cid in v:
            return f"  ← {name}"
    return ""


def pretty(obj: Any, indent: int = 0) -> None:
    pad = "  " * indent
    if isinstance(obj, dict):
        for k, vals in obj.items():
            for v in vals:
                ann = annotate(v)
                if isinstance(v, (dict, list)):
                    print(f"{pad}[field {k}]")
                    pretty(v, indent + 1)
                else:
                    print(f"{pad}[field {k}]  {v!r}{ann}")
    elif isinstance(obj, list):
        for item in obj:
            pretty(item, indent)
    else:
        print(f"{pad}{obj!r}")


# ──────────────────────────────────────────────────────────────────────────────
# SMART ANALYZER — extracts odds, markets, scores from raw proto tree
# ──────────────────────────────────────────────────────────────────────────────

def walk(obj: Any, path: str, results: list) -> None:
    """Recursively walk a decoded proto tree, collecting all leaf values."""
    if isinstance(obj, dict):
        for k, vals in obj.items():
            for v in vals:
                walk(v, f"{path}.{k}", results)
    elif isinstance(obj, list):
        for item in obj:
            walk(item, path, results)
    else:
        results.append((path, obj))


def is_odd(v: Any) -> bool:
    """Detect if a value looks like a betting odd (1.01 – 100.0)."""
    try:
        f = float(v) if not isinstance(v, (int, float)) else v
        return 1.001 <= f <= 999.0 and isinstance(v, (int, float)) and not isinstance(v, bool)
    except Exception:
        return False


def analyze_frame(data: bytes) -> None:
    """
    Parse a raw proto frame and print a structured summary:
    - Match metadata
    - Markets + selections + odds
    - Scores / live data
    - Unknown binary fields (for further reverse engineering)
    """
    d = decode_proto(data)
    leaves: list[tuple[str, Any]] = []
    walk(d, "root", leaves)

    # ── Separate by type ──────────────────────────────────────────────────────
    strings  = [(p, v) for p, v in leaves if isinstance(v, str)]
    numbers  = [(p, v) for p, v in leaves if isinstance(v, int) and not isinstance(v, bool)]
    def looks_like_hex(s: str) -> bool:
        return len(s) >= 20 and all(c in '0123456789abcdef' for c in s)

    hexvals  = [(p, v) for p, v in leaves if isinstance(v, str) and looks_like_hex(v)]
    strings  = [(p, v) for p, v in strings if not looks_like_hex(v)]

    # ── Markets & selections ──────────────────────────────────────────────────
    markets_found = {}
    contestants_found = {}
    for p, v in strings:
        for code, label in MARKET_CODES.items():
            if code in v:
                markets_found[code] = label
        for cid, name in CONTESTANT_IDS.items():
            if cid in v:
                contestants_found[cid] = name

    # ── Potential odds (floats encoded as int * 1000 or direct) ──────────────
    # gRPC often sends odds as int * 1000 (e.g. 1850 = 1.850)
    potential_odds = []
    for p, v in numbers:
        if 1001 <= v <= 100000:   # range 1.001 – 100.000 as millodds
            potential_odds.append((p, v, v / 1000))
        elif 101 <= v <= 9999 and v % 5 == 0:  # centodds (1.01 – 99.99)
            potential_odds.append((p, v, v / 100))

    # Also check fixed32/fixed64 floats stored as tuples (wire type 1 & 5)
    floats = [(p, v) for p, v in leaves if isinstance(v, tuple) and v[0] in ('fixed32', 'fixed64')]
    for p, tup in floats:
        fval = tup[1]
        if isinstance(fval, float) and 1.001 <= fval <= 100.0 and fval == fval:  # not NaN
            potential_odds.append((p, f"float={fval:.4f}", fval))

    # ── Timestamps ────────────────────────────────────────────────────────────
    timestamps = [(p, v) for p, v in numbers if 1700000000 < v < 2000000000]

    # ── PRINT REPORT ──────────────────────────────────────────────────────────
    print("\n" + "━"*60)
    print("  FRAME ANALYSIS")
    print("━"*60)

    print(f"\n{'─'*40}")
    print("  CONTESTANTS DETECTED")
    print(f"{'─'*40}")
    if contestants_found:
        for cid, name in contestants_found.items():
            print(f"  {name}  (id: {cid})")
    else:
        print("  (none matched — new match?)")

    print(f"\n{'─'*40}")
    print("  MARKETS DETECTED")
    print(f"{'─'*40}")
    if markets_found:
        for code, label in markets_found.items():
            print(f"  {code:20s}  →  {label}")
    else:
        print("  (none matched)")

    print(f"\n{'─'*40}")
    print("  ALL STRINGS  (market codes / labels / IDs)")
    print(f"{'─'*40}")
    for p, v in strings:
        if len(v) > 2:   # skip single chars
            ann = annotate(v)
            print(f"  {p:40s}  {v!r}{ann}")

    print(f"\n{'─'*40}")
    print("  POTENTIAL ODDS  (int ÷ 1000)")
    print(f"{'─'*40}")
    seen_odds = set()
    for p, raw, odd in sorted(potential_odds, key=lambda x: x[2]):
        if odd not in seen_odds:
            seen_odds.add(odd)
            print(f"  {odd:.3f}   (raw={raw})   @ {p}")

    print(f"\n{'─'*40}")
    print("  TIMESTAMPS")
    print(f"{'─'*40}")
    if timestamps:
        import datetime
        for p, v in timestamps:
            dt = datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc)
            print(f"  {dt.isoformat()}  @ {p}")
    else:
        print("  (none detected)")

    print(f"\n{'─'*40}")
    print("  ALL INTEGERS  (scores / IDs / flags / counts)")
    print(f"{'─'*40}")
    for p, v in sorted(numbers, key=lambda x: x[1]):
        print(f"  {v:>12}   @ {p}")

    print(f"\n{'─'*40}")
    print("  FIXED FLOATS  (wire type 1 & 5 — potential odds as float32/float64)")
    print(f"{'─'*40}")
    seen_f = set()
    for p, tup in floats:
        fval = tup[1]
        if fval == fval and abs(fval) < 1e10:  # skip NaN and huge values
            key = round(fval, 4)
            if key not in seen_f:
                seen_f.add(key)
                print(f"  {tup[0]:8s}  {fval:>12.4f}   raw_int={tup[2]}   @ {p}")

    print(f"\n{'─'*40}")
    print(f"  BINARY BLOBS  ({len(hexvals)} fields — need further decoding)")
    print(f"{'─'*40}")
    for p, v in hexvals[:10]:   # show max 10
        print(f"  {p:40s}  {v[:80]}{'...' if len(v)>80 else ''}")

    print("\n" + "━"*60 + "\n")

# ──────────────────────────────────────────────────────────────────────────────
# CLEAN JSON BUILDER  (--clean mode)
# Extracts structured market/odds data from the raw decoded proto tree.
#
# Key proto structure (reverse-engineered from betclic4.py output):
#   root.1.1          → match level
#     .2              → match name (string)
#     .3              → match datetime (string)
#     .1              → match_id (int64)
#     .8              → competition info  (.2 = name)
#     .12 (repeated)  → team  (.3 = name, .2 = int id)
#     .11 (repeated)  → market wrapper
#       .3            → market detail
#         .2          → market name
#         .16 (rep.)  → simple selection  (.10 = name, .12 = fixed64 odd)   [1X2 type]
#         .10         → selection group   (.1.1.10 = name, .1.1.12 = odd)   [O/U, etc.]
#         .11 (rep.)  → player group      (.1 = team, .2.10 = name, .2.12 = odd)
#         .13 (rep.)  → sub-market (same structure as market detail)
# ──────────────────────────────────────────────────────────────────────────────

def _get(d: dict, *fields, default=None):
    """Navigate a proto dict by successive field numbers, return first value found."""
    cur = d
    for f in fields:
        if not isinstance(cur, dict):
            return default
        vals = cur.get(f, [])
        if not vals:
            return default
        cur = vals[0]
    return cur if cur is not None else default


def _getall(d: dict, field: int) -> list:
    """Return all values for a field in a proto dict (empty list if missing)."""
    if not isinstance(d, dict):
        return []
    return d.get(field, [])


def _as_float_odd(val) -> float | None:
    """Return a float odd from a fixed64/fixed32 tuple or plain number, or None."""
    if isinstance(val, tuple) and val[0] in ('fixed32', 'fixed64'):
        f = val[1]
        if isinstance(f, float) and 1.001 <= f <= 1000.0 and f == f:  # not NaN
            return round(f, 4)
    elif isinstance(val, (int, float)) and not isinstance(val, bool):
        if 1.001 <= float(val) <= 1000.0:
            return round(float(val), 4)
    return None


def _extract_odd(d: dict, field: int = 12) -> float | None:
    """Get the first valid odd from all values of a field."""
    for v in _getall(d, field):
        o = _as_float_odd(v)
        if o is not None:
            return o
    return None


def _first_string(d: dict, *fields) -> str | None:
    """Return first string value found across given fields."""
    for f in fields:
        for v in _getall(d, f):
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _extract_selections(mkt: dict) -> list[dict]:
    """
    Extract all selections (name + odd) from a market detail dict.
    Handles three sub-structures:
      - field 16: simple repeated selections (1X2, HT result, etc.)
      - field 10: grouped selections (O/U, Double chance, score gap, etc.)
      - field 11: player groups (goal scorers)
    """
    sels = []

    # ── Type A: field 16 (simple, like 1X2) ───────────────────────────────────
    for sel in _getall(mkt, 16):
        if not isinstance(sel, dict):
            continue
        name = _first_string(sel, 10, 11)
        odd  = _extract_odd(sel, 12)
        if name or odd is not None:
            sels.append({"name": name, "odd": odd})

    # ── Type B: field 10 → 1 → 1 (grouped, like O/U, double chance) ──────────
    for top in _getall(mkt, 10):
        if not isinstance(top, dict):
            continue
        for grp in _getall(top, 1):
            if not isinstance(grp, dict):
                continue
            for sel in _getall(grp, 1):
                if not isinstance(sel, dict):
                    continue
                name = _first_string(sel, 10, 11)
                odd  = _extract_odd(sel, 12)
                if name or odd is not None:
                    sels.append({"name": name, "odd": odd})

    # ── Type C: field 11 (player groups: team → players) ─────────────────────
    for team_grp in _getall(mkt, 11):
        if not isinstance(team_grp, dict):
            continue
        team_name = _first_string(team_grp, 1)
        for player in _getall(team_grp, 2):
            if not isinstance(player, dict):
                continue
            name = _first_string(player, 10, 11)
            odd  = _extract_odd(player, 12)
            if name or odd is not None:
                entry = {"name": name, "odd": odd}
                if team_name:
                    entry["team"] = team_name
                sels.append(entry)

    return sels


def _parse_market(mkt_detail: dict) -> dict | None:
    """
    Parse a single market detail dict.
    Returns {"name": ..., "selections": [...], "sub_markets": [...]} or None if empty.
    """
    name = _first_string(mkt_detail, 2, 3)
    if not name:
        return None

    sels = _extract_selections(mkt_detail)

    # Sub-markets (field 13, same structure as a market detail)
    sub_markets = []
    for sub in _getall(mkt_detail, 13):
        if not isinstance(sub, dict):
            continue
        parsed_sub = _parse_market(sub)
        if parsed_sub:
            sub_markets.append(parsed_sub)

    market = {"name": name, "selections": sels}
    if sub_markets:
        market["sub_markets"] = sub_markets

    if not sels and not sub_markets:
        return None
    return market


def build_clean_json(data: bytes) -> dict:
    """
    Decode a raw proto frame and return a clean JSON-ready dict with:
      match_id, match, date, competition, teams, markets (name + selections + odds).
    """
    d = decode_proto(data)

    # Navigate to root.1.1
    outer = _get(d, 1)
    match_level = _get(outer, 1) if isinstance(outer, dict) else None
    if not isinstance(match_level, dict):
        return {"error": "navigation failed at root.1.1"}

    # ── Match metadata ─────────────────────────────────────────────────────────
    match_id   = _get(match_level, 1)   # int64
    match_name = _first_string(match_level, 2)
    match_date = _first_string(match_level, 3)

    comp_info  = _get(match_level, 8)
    comp_name  = _first_string(comp_info, 2) if isinstance(comp_info, dict) else None

    # ── Teams (field 12 repeated) ──────────────────────────────────────────────
    teams = []
    seen_team_ids = set()
    for team in _getall(match_level, 12):
        if not isinstance(team, dict):
            continue
        team_name = _first_string(team, 3, 4)
        team_id   = _get(team, 2)   # int64
        if team_id and team_id not in seen_team_ids:
            seen_team_ids.add(team_id)
            teams.append({"name": team_name, "id": team_id})

    # ── Markets ────────────────────────────────────────────────────────────────
    # Structure: root.1.1.11 (wrapper) -> field 3 REPEATED (one dict per market)
    # _get() only returns the first field-3, so we iterate _getall() here.
    markets = []
    seen_market_names = set()

    for mkt_wrapper in _getall(match_level, 11):
        if not isinstance(mkt_wrapper, dict):
            continue
        for mkt_detail in _getall(mkt_wrapper, 3):
            if not isinstance(mkt_detail, dict):
                continue
            parsed = _parse_market(mkt_detail)
            if parsed and parsed["name"] not in seen_market_names:
                seen_market_names.add(parsed["name"])
                markets.append(parsed)

    return {
        "match_id": match_id,
        "match":    match_name,
        "date":     match_date,
        "competition": comp_name,
        "teams":    teams,
        "markets":  markets,
    }


# ──────────────────────────────────────────────────────────────────────────────
# LIVE STATE EXTRACTOR
# Reads match status, score, period and minute directly from stream frames.
#
# Why we need this:
#   GetMatchesBySportWithNotifications (the listing) sets field 6 = is_live
#   based on a server-side cache that lags real match state by minutes.
#   GetMatchWithNotification (the stream) carries the authoritative live state
#   in every frame it pushes — this is what we parse here.
#
# Known fields at root.1.1 (match_level):
#   1  = match_id        (confirmed)
#   2  = match_name      (confirmed)
#   3  = date            (confirmed)
#   6  = is_live flag    (confirmed from listing proto — same schema)
#   8  = competition     (confirmed)
#   11 = market wrappers (confirmed)
#   12 = teams           (confirmed)
#
# Score / period live data are in an unmapped sub-message at match_level.
# We scan candidate fields (4,5,7,9,10,13,14,15) looking for:
#   - A nested dict containing two integers in [0, 30]  → home/away score
#   - An integer in [0, 120]                            → match minute
#   - A string matching known French period names       → period
# ──────────────────────────────────────────────────────────────────────────────

# Known Betclic period label strings (French)
_PERIOD_STRINGS = {
    "1ère mi-temps":   "1H",
    "1ere mi-temps":   "1H",
    "première mi-temps": "1H",
    "mi-temps":        "HT",
    "2ème mi-temps":   "2H",
    "2eme mi-temps":   "2H",
    "deuxième mi-temps": "2H",
    "prolongation":    "ET",
    "prolongations":   "ET",
    "tirs au but":     "PEN",
    "terminé":         "FT",
    "termine":         "FT",
    "à venir":         "NS",
}


def _walk_for_live(obj: Any, results: dict, depth: int = 0) -> None:
    """
    Recursively walk a decoded proto dict.
    Collects:
      - integers in [0, 30]  as score candidates
      - integers in [1, 120] as minute candidates (overlaps score, filtered later)
      - strings matching known period labels
    """
    if depth > 8:
        return
    if isinstance(obj, dict):
        ints_here = []
        for k, vals in obj.items():
            for v in vals:
                if isinstance(v, int) and not isinstance(v, bool):
                    if 0 <= v <= 30:
                        ints_here.append(v)
                    if 1 <= v <= 120 and 'minute_candidates' not in results:
                        results.setdefault('minute_candidates', []).append(v)
                elif isinstance(v, str):
                    low = v.strip().lower()
                    if low in _PERIOD_STRINGS and not results.get('period'):
                        results['period']       = _PERIOD_STRINGS[low]
                        results['period_raw']   = v.strip()
                elif isinstance(v, (dict, list)):
                    _walk_for_live(v, results, depth + 1)

        # Two integers at the same dict level in [0, 30] → likely home/away score
        if len(ints_here) == 2 and 'score_home' not in results:
            results['score_home'] = ints_here[0]
            results['score_away'] = ints_here[1]
        elif len(ints_here) >= 2 and 'score_home' not in results:
            # Take first two if there are more
            results['score_home'] = ints_here[0]
            results['score_away'] = ints_here[1]

    elif isinstance(obj, list):
        for item in obj:
            _walk_for_live(item, results, depth)


def extract_live_state(data: bytes) -> dict:
    """
    Parse a raw GetMatchWithNotification proto frame and return the live state:

    {
        "is_live":    bool,
        "score_home": int | None,
        "score_away": int | None,
        "period":     str | None,   # "1H" | "HT" | "2H" | "ET" | "PEN" | "FT"
        "minute":     int | None,
    }

    All fields are always present; unknown fields are None.

    Called by betclic_feed.py on every stream frame so _meta stays authoritative.
    """
    result: dict = {
        "is_live":    False,
        "score_home": None,
        "score_away": None,
        "period":     None,
        "minute":     None,
    }

    try:
        d = decode_proto(data)

        # Navigate to match_level (root.1.1)
        outer       = _get(d, 1)
        match_level = _get(outer, 1) if isinstance(outer, dict) else None
        if not isinstance(match_level, dict):
            return result

        # ── is_live: field 6 at match_level ──────────────────────────────────
        # Confirmed same field number as GetMatchesBySportWithNotifications.
        # Value 1 = live, absent = prematch.
        live_val = _get(match_level, 6)
        result["is_live"] = bool(live_val)

        # ── Score / period / minute: scan unmapped sub-messages ───────────────
        # We skip the confirmed fields (1,2,3,8,11,12) and scan the rest.
        KNOWN_FIELDS = {1, 2, 3, 8, 11, 12}
        live_candidates: dict = {}

        for field_num, vals in match_level.items():
            if field_num in KNOWN_FIELDS:
                continue
            for v in vals:
                if isinstance(v, dict):
                    _walk_for_live(v, live_candidates)
                elif isinstance(v, int) and not isinstance(v, bool):
                    # Direct integer at match_level (e.g. field 6 = is_live already read)
                    pass

        # Assign score if found
        if 'score_home' in live_candidates:
            result["score_home"] = live_candidates["score_home"]
            result["score_away"] = live_candidates["score_away"]

        # Assign period if found
        if 'period' in live_candidates:
            result["period"] = live_candidates["period"]
            # If we have a period, match is definitely live
            if live_candidates["period"] not in ("FT", "NS"):
                result["is_live"] = True

        # Minute: pick the most plausible candidate from minute_candidates.
        # Scores are 0-30; minutes are 1-120.
        # We exclude values that already appear as scores.
        scores = {result["score_home"], result["score_away"]} - {None}
        minute_candidates = [
            v for v in live_candidates.get("minute_candidates", [])
            if v not in scores and v > 0
        ]
        if minute_candidates:
            # Prefer values > 30 (unambiguously minute, not score)
            above_30 = [v for v in minute_candidates if v > 30]
            result["minute"] = above_30[0] if above_30 else minute_candidates[-1]

    except Exception:
        pass  # always return partial result rather than raising

    return result


# ──────────────────────────────────────────────────────────────────────────────
# gRPC RESPONSE PARSER
# ──────────────────────────────────────────────────────────────────────────────

def parse_grpc_response(raw: bytes) -> tuple[list[bytes], dict]:
    messages, trailers = [], {}
    i = 0
    while i < len(raw):
        if len(raw) - i < 5:
            break
        flags  = raw[i]
        length = struct.unpack('>I', raw[i+1:i+5])[0]
        i += 5
        chunk = raw[i:i+length]; i += length
        if flags & 0x80:    # trailer frame
            for line in chunk.decode('utf-8', errors='replace').strip().split('\r\n'):
                if ':' in line:
                    k, v = line.split(':', 1)
                    trailers[k.strip()] = v.strip()
        else:
            messages.append(chunk)
    return messages, trailers

# ──────────────────────────────────────────────────────────────────────────────
# PAYLOAD BUILDERS
# ──────────────────────────────────────────────────────────────────────────────

def payload_live_count(locale: str = "fr") -> bytes:
    """
    GetLiveCount
    Confirmed: field 1 = locale string
    Returns: {field_1: N}  where N = number of live matches
    """
    return grpc_frame(field_str(1, locale))


def payload_get_match(match_id: int, locale: str = "fr", market_code: str | None = None) -> bytes:
    """
    GetMatchWithNotification
    CONFIRMED payload structure:
      field 1 = int64 match_id
      field 2 = string locale
      field 3 = string market_code  (optional — filters to one market category)

    Examples:
      field 3 = "ca_ftb_top"    → top/featured markets only
      field 3 = "ca_ftb_rslt"   → match result (1X2) only
      field 3 = "ca_ftb_gsc"    → goal scorers only
      field 3 = "ca_ftb_goa"    → goals over/under only
      field 3 = "ca_ftb_cshcp"  → correct score + handicap only
      (omit field 3)            → all markets
    """
    proto = field_int(1, match_id) + field_str(2, locale)
    if market_code:
        proto += field_str(3, market_code)
    return grpc_frame(proto)

def payload_list_matches(sport: str = "football", competition: str | None = None,
                         locale: str = "fr", offset: int = 0) -> bytes:
    """
    GetMatchesBySportWithNotifications payload.
    CONFIRMED from Linux curl captures (exact unicode bytes):

      field 1 = sport        "football"
      field 2 = competition  "ftb_rsm" (or locale "fr" if no competition filter)
      field 3 = locale       "fr"  (only present when field2 = competition)
      field 4 = offset       0 (omitted on first page), 20, 40, 60...
      field 5 = 40           constant (subscription/display window, always 40)

    Page size = 20 matches per request.
    Paginate by incrementing field4 by 20 until response is empty.

    Without competition filter → gets ALL football matches:
      field1="football"  field2="fr"  [field4=offset]  field5=40

    With competition filter → gets matches for that competition only:
      field1="football"  field2="ftb_rsm"  field3="fr"  [field4=offset]  field5=40
    """
    if competition:
        proto = (field_str(1, sport) + field_str(2, competition)
                 + field_str(3, locale))
    else:
        proto = field_str(1, sport) + field_str(2, locale)

    if offset > 0:
        proto += field_int(4, offset)   # omit field4 on first page (offset=0)
    proto += field_int(5, 40)           # always 40
    return grpc_frame(proto)


def parse_matches_frame(data: bytes) -> list[dict]:
    """
    Decode a GetMatchesBySportWithNotifications response frame.
    Returns list of {match_id, match, date, competition, teams, live}.

    CONFIRMED structure from --dump-matches:
      root[1]         = sport wrapper
        [1]           = competition code string
        [2] repeated  = market type definitions
        [3] repeated  = MATCHES (x40 per page)  ← here
          [1]         = match_id (int64)
          [2]         = match name
          [3]         = date ISO string
          [6]         = is_live (1 = live, absent = prematch)
          [7]         = match position/number
          [8]         = competition info
            [1]       = comp_id
            [2]       = comp_name
          [12] x2     = teams
            [2]       = team_id
            [3]       = team full name
            [4]       = team short name
        [4]           = sport label "Football"
        [5]           = total match count (e.g. 648)
    """
    d = decode_proto(data)
    matches = []

    # Navigate root[1] → sport wrapper
    sport_wrapper = _get(d, 1)
    if not isinstance(sport_wrapper, dict):
        return matches

    # Total available (field 5) — useful for knowing how many pages to fetch
    total_available = _get(sport_wrapper, 5) or 0

    # Matches are in field 3 (repeated)
    for match in _getall(sport_wrapper, 3):
        if not isinstance(match, dict):
            continue

        match_id   = _get(match, 1)
        match_name = _first_string(match, 2)
        match_date = _first_string(match, 3)
        is_live    = bool(_get(match, 6))  # present=1 if live, absent if prematch

        comp_info  = _get(match, 8)
        comp_name  = _first_string(comp_info, 2) if isinstance(comp_info, dict) else None

        teams = []
        seen_ids = set()
        for team in _getall(match, 12):
            if not isinstance(team, dict):
                continue
            team_id   = _get(team, 2)
            team_name = _first_string(team, 3, 4)  # field3=full, field4=short
            if team_id and team_id not in seen_ids:
                seen_ids.add(team_id)
                teams.append({"name": team_name, "id": team_id})

        if not match_id:
            continue

        matches.append({
            "match_id":    match_id,
            "match":       match_name,
            "date":        match_date,
            "competition": comp_name,
            "live":        is_live,
            "teams":       teams,
        })

    return matches, total_available


