"""
proto/codec.py — pure protobuf encode/decode for the Betclic gRPC-web API.

Zero I/O. No imports outside stdlib. Safe to import from anywhere.
"""
from __future__ import annotations
import struct
from typing import Any


# ── Protobuf encoder ───────────────────────────────────────────────────────────

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


# ── Protobuf decoder ───────────────────────────────────────────────────────────

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
                    if any(c < 0x09 for c in raw) and depth < 6:
                        raise ValueError
                    fields.setdefault(fn, []).append(text)
                except (UnicodeDecodeError, ValueError):
                    try:
                        sub = decode_proto(raw, depth + 1)
                        fields.setdefault(fn, []).append(sub if sub else raw.hex())
                    except Exception:
                        fields.setdefault(fn, []).append(raw.hex())

            elif wt == 1:        # 64-bit fixed
                raw8 = data[pos:pos+8]; pos += 8
                val_d = struct.unpack('<d', raw8)[0]
                val_i = struct.unpack('<Q', raw8)[0]
                fields.setdefault(fn, []).append(('fixed64', val_d, val_i))

            elif wt == 5:        # 32-bit fixed
                raw4 = data[pos:pos+4]; pos += 4
                val_f = struct.unpack('<f', raw4)[0]
                val_i = struct.unpack('<I', raw4)[0]
                fields.setdefault(fn, []).append(('fixed32', val_f, val_i))

            else:
                break
        except Exception:
            break
    return fields


# ── gRPC response parser ──────────────────────────────────────────────────────

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


# ── Payload builders ──────────────────────────────────────────────────────────

def payload_live_count(locale: str = "fr") -> bytes:
    return grpc_frame(field_str(1, locale))


def payload_get_match(match_id: int, locale: str = "fr", market_code: str | None = None) -> bytes:
    proto = field_int(1, match_id) + field_str(2, locale)
    if market_code:
        proto += field_str(3, market_code)
    return grpc_frame(proto)


def payload_list_matches(sport: str = "football", competition: str | None = None,
                         locale: str = "fr", offset: int = 0) -> bytes:
    if competition:
        proto = (field_str(1, sport) + field_str(2, competition)
                 + field_str(3, locale))
    else:
        proto = field_str(1, sport) + field_str(2, locale)

    if offset > 0:
        proto += field_int(4, offset)
    proto += field_int(5, 40)
    return grpc_frame(proto)


# ── Proto tree navigation helpers ─────────────────────────────────────────────

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
        if isinstance(f, float) and 1.001 <= f <= 1000.0 and f == f:
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


# ── Market/selection extraction ───────────────────────────────────────────────

def _extract_selections(mkt: dict) -> list[dict]:
    sels = []

    # Type A: field 16 (simple, like 1X2)
    for sel in _getall(mkt, 16):
        if not isinstance(sel, dict):
            continue
        name = _first_string(sel, 10, 11)
        odd  = _extract_odd(sel, 12)
        if name or odd is not None:
            sels.append({"name": name, "odd": odd})

    # Type B: field 10 → 1 → 1 (grouped, like O/U)
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

    # Type C: field 11 (player groups: team → players)
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
    name = _first_string(mkt_detail, 2, 3)
    if not name:
        return None

    sels = _extract_selections(mkt_detail)

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

    outer = _get(d, 1)
    match_level = _get(outer, 1) if isinstance(outer, dict) else None
    if not isinstance(match_level, dict):
        return {"error": "navigation failed at root.1.1"}

    match_id   = _get(match_level, 1)
    match_name = _first_string(match_level, 2)
    match_date = _first_string(match_level, 3)

    comp_info  = _get(match_level, 8)
    comp_name  = _first_string(comp_info, 2) if isinstance(comp_info, dict) else None

    teams = []
    seen_team_ids = set()
    for team in _getall(match_level, 12):
        if not isinstance(team, dict):
            continue
        team_name = _first_string(team, 3, 4)
        team_id   = _get(team, 2)
        if team_id and team_id not in seen_team_ids:
            seen_team_ids.add(team_id)
            teams.append({"name": team_name, "id": team_id})

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


# ── Live state extractor ──────────────────────────────────────────────────────

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

        if len(ints_here) == 2 and 'score_home' not in results:
            results['score_home'] = ints_here[0]
            results['score_away'] = ints_here[1]
        elif len(ints_here) >= 2 and 'score_home' not in results:
            results['score_home'] = ints_here[0]
            results['score_away'] = ints_here[1]

    elif isinstance(obj, list):
        for item in obj:
            _walk_for_live(item, results, depth)


def extract_live_state(data: bytes) -> dict:
    """Parse a raw GetMatchWithNotification proto frame and return live state."""
    result: dict = {
        "is_live":    False,
        "score_home": None,
        "score_away": None,
        "period":     None,
        "minute":     None,
    }

    try:
        d = decode_proto(data)

        outer       = _get(d, 1)
        match_level = _get(outer, 1) if isinstance(outer, dict) else None
        if not isinstance(match_level, dict):
            return result

        live_val = _get(match_level, 6)
        result["is_live"] = bool(live_val)

        KNOWN_FIELDS = {1, 2, 3, 8, 11, 12}
        live_candidates: dict = {}

        for field_num, vals in match_level.items():
            if field_num in KNOWN_FIELDS:
                continue
            for v in vals:
                if isinstance(v, dict):
                    _walk_for_live(v, live_candidates)
                elif isinstance(v, int) and not isinstance(v, bool):
                    pass

        if 'score_home' in live_candidates:
            result["score_home"] = live_candidates["score_home"]
            result["score_away"] = live_candidates["score_away"]

        if 'period' in live_candidates:
            result["period"] = live_candidates["period"]
            if live_candidates["period"] not in ("FT", "NS"):
                result["is_live"] = True

        scores = {result["score_home"], result["score_away"]} - {None}
        minute_candidates = [
            v for v in live_candidates.get("minute_candidates", [])
            if v not in scores and v > 0
        ]
        if minute_candidates:
            above_30 = [v for v in minute_candidates if v > 30]
            result["minute"] = above_30[0] if above_30 else minute_candidates[-1]

    except Exception:
        pass

    return result


# ── Match listing parser ──────────────────────────────────────────────────────

def parse_matches_frame(data: bytes) -> tuple[list[dict], int]:
    """
    Decode a GetMatchesBySportWithNotifications response frame.
    Returns (matches, total_available).
    """
    d = decode_proto(data)
    matches = []

    sport_wrapper = _get(d, 1)
    if not isinstance(sport_wrapper, dict):
        return matches, 0

    total_available = _get(sport_wrapper, 5) or 0

    for match in _getall(sport_wrapper, 3):
        if not isinstance(match, dict):
            continue

        match_id   = _get(match, 1)
        match_name = _first_string(match, 2)
        match_date = _first_string(match, 3)
        is_live    = bool(_get(match, 6))

        comp_info  = _get(match, 8)
        comp_name  = _first_string(comp_info, 2) if isinstance(comp_info, dict) else None

        teams = []
        seen_ids = set()
        for team in _getall(match, 12):
            if not isinstance(team, dict):
                continue
            team_id   = _get(team, 2)
            team_name = _first_string(team, 3, 4)
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
