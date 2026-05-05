"""
cli/commands.py — CLI debug tools for the Betclic gRPC-web API.

Never imported by production code (feed, server).
"""
from __future__ import annotations
import sys, json, struct, concurrent.futures

from proto.codec import (
    decode_proto, build_clean_json,
    payload_live_count, payload_get_match, payload_list_matches,
    parse_matches_frame,
)
from proto.client import (
    BASE, HEADERS, api_call,
    fetch_first_frame, fetch_first_frame_raw,
)

MARKET_CODES = {
    "ca_ftb_goa":    "Goals Over/Under",
    "ca_ftb_goalm":  "Goals Method",
    "ca_ftb_cshcp":  "Correct Score / Handicap",
    "ca_ftb_prp":    "Player Props",
    "ca_ftb_gsc":    "Goal Scorers",
    "ca_ftb_rslt":   "Match Result (1X2)",
}

MARKET_FILTERS = {
    "top":    "ca_ftb_top",
    "rslt":   "ca_ftb_rslt",
    "gsc":    "ca_ftb_gsc",
    "goa":    "ca_ftb_goa",
    "goal":   "ca_ftb_goa",
    "cshcp":  "ca_ftb_cshcp",
}

CONTESTANT_IDS = {
    "5438414804710380J": "Bayern Munich",
    "5438444252606822R": "Atalanta",
}

FOOTBALL_COMPETITIONS = [
    "ftb_rsm",
]
MATCH_URL_BASE = "https://www.betclic.fr/football-sfootball"


# ── Analysis helpers ──────────────────────────────────────────────────────────

def annotate(v) -> str:
    if not isinstance(v, str):
        return ""
    for code, label in MARKET_CODES.items():
        if code in v:
            return f"  ← {label}"
    for cid, name in CONTESTANT_IDS.items():
        if cid in v:
            return f"  ← {name}"
    return ""


def pretty(obj, indent: int = 0) -> None:
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


def walk(obj, path: str, results: list) -> None:
    if isinstance(obj, dict):
        for k, vals in obj.items():
            for v in vals:
                walk(v, f"{path}.{k}", results)
    elif isinstance(obj, list):
        for item in obj:
            walk(item, path, results)
    else:
        results.append((path, obj))


def is_odd(v) -> bool:
    try:
        f = float(v) if not isinstance(v, (int, float)) else v
        return 1.001 <= f <= 999.0 and isinstance(v, (int, float)) and not isinstance(v, bool)
    except Exception:
        return False


def analyze_frame(data: bytes) -> None:
    d = decode_proto(data)
    leaves: list[tuple[str, any]] = []
    walk(d, "root", leaves)

    strings  = [(p, v) for p, v in leaves if isinstance(v, str)]
    numbers  = [(p, v) for p, v in leaves if isinstance(v, int) and not isinstance(v, bool)]

    def looks_like_hex(s: str) -> bool:
        return len(s) >= 20 and all(c in '0123456789abcdef' for c in s)

    hexvals  = [(p, v) for p, v in leaves if isinstance(v, str) and looks_like_hex(v)]
    strings  = [(p, v) for p, v in strings if not looks_like_hex(v)]

    markets_found = {}
    contestants_found = {}
    for p, v in strings:
        for code, label in MARKET_CODES.items():
            if code in v:
                markets_found[code] = label
        for cid, name in CONTESTANT_IDS.items():
            if cid in v:
                contestants_found[cid] = name

    potential_odds = []
    for p, v in numbers:
        if 1001 <= v <= 100000:
            potential_odds.append((p, v, v / 1000))
        elif 101 <= v <= 9999 and v % 5 == 0:
            potential_odds.append((p, v, v / 100))

    floats = [(p, v) for p, v in leaves if isinstance(v, tuple) and v[0] in ('fixed32', 'fixed64')]
    for p, tup in floats:
        fval = tup[1]
        if isinstance(fval, float) and 1.001 <= fval <= 100.0 and fval == fval:
            potential_odds.append((p, f"float={fval:.4f}", fval))

    timestamps = [(p, v) for p, v in numbers if 1700000000 < v < 2000000000]

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
        if len(v) > 2:
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
        if fval == fval and abs(fval) < 1e10:
            key = round(fval, 4)
            if key not in seen_f:
                seen_f.add(key)
                print(f"  {tup[0]:8s}  {fval:>12.4f}   raw_int={tup[2]}   @ {p}")

    print(f"\n{'─'*40}")
    print(f"  BINARY BLOBS  ({len(hexvals)} fields — need further decoding)")
    print(f"{'─'*40}")
    for p, v in hexvals[:10]:
        print(f"  {p:40s}  {v[:80]}{'...' if len(v)>80 else ''}")

    print("\n" + "━"*60 + "\n")


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_live_count(locale: str = "fr"):
    print("=== GetLiveCount ===")
    msgs, _ = api_call("GetLiveCount", payload_live_count(locale))
    for m in msgs:
        d = decode_proto(m)
        count = d.get(1, [None])[0]
        print(f"\n  ✓ Live matches: {count}")


def cmd_match(match_id: int, locale: str = "fr"):
    print(f"=== GetMatchWithNotification — match {match_id} ===")
    frame = fetch_first_frame(match_id, locale)
    if not frame:
        print("  [no frame received]")
        return
    print(f"\n--- Frame ({len(frame)} bytes) ---")
    analyze_frame(frame)


def cmd_inspect(hex_str: str):
    raw = bytes.fromhex(hex_str.replace(" ", ""))
    print(f"=== Inspect ({len(raw)} bytes) ===")
    msgs, trailers = parse_grpc_response(raw)
    print(f"  Trailers: {trailers}")
    for i, m in enumerate(msgs):
        print(f"\n--- Message {i} ({len(m)} bytes) ---")
        analyze_frame(m)


def cmd_dump_raw(match_id: int, locale: str = "fr"):
    import requests as _requests
    p = payload_get_match(match_id, locale)
    r = _requests.post(f"{BASE}/GetMatchWithNotification", headers=HEADERS, data=p,
                       timeout=(10, 30), stream=True)
    raw = b""
    try:
        for chunk in r.iter_content(chunk_size=4096):
            raw += chunk
            if len(raw) >= 5:
                length = struct.unpack('>I', raw[1:5])[0]
                if len(raw) >= 5 + length + 5:
                    break
    except Exception:
        pass
    print(raw.hex())


def cmd_clean(match_id: int, locale: str = "fr", output_file: str | None = None,
              market_code: str | None = None):
    frame = fetch_first_frame(match_id, locale, market_code)
    if not frame:
        print(json.dumps({"error": "no frame received"}))
        return

    result = build_clean_json(frame)
    formatted = json.dumps(result, ensure_ascii=False, indent=2)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(formatted)
        print(f"  → Written to {output_file}")
        n_markets = len(result.get("markets", []))
        n_sels    = sum(len(m.get("selections", [])) for m in result.get("markets", []))
        print(f"  → {n_markets} markets, {n_sels} selections total")
    else:
        print(formatted)


def cmd_stream(match_id: int, locale: str = "fr", max_frames: int = 10, clean: bool = False):
    import requests as _requests
    p = payload_get_match(match_id, locale)
    print(f"=== STREAM — match {match_id} (Ctrl+C to stop) ===")
    r = _requests.post(f"{BASE}/GetMatchWithNotification", headers=HEADERS, data=p,
                       timeout=(10, 120), stream=True)
    print(f"HTTP {r.status_code}\n")

    buf = b""
    frame_count = 0
    try:
        for chunk in r.iter_content(chunk_size=1024):
            buf += chunk
            while len(buf) >= 5:
                flags  = buf[0]
                length = struct.unpack('>I', buf[1:5])[0]
                if len(buf) < 5 + length:
                    break
                frame = buf[5:5+length]
                buf   = buf[5+length:]
                if flags & 0x80:
                    trailer = frame.decode('utf-8', errors='replace')
                    print(f"[TRAILER] {trailer}")
                else:
                    frame_count += 1
                    if clean:
                        result = build_clean_json(frame)
                        print(f"\n# FRAME {frame_count}  ({len(frame)} bytes)")
                        print(json.dumps(result, ensure_ascii=False, indent=2))
                    else:
                        print(f"\n{'='*60}")
                        print(f"FRAME #{frame_count}  ({len(frame)} bytes)")
                        print(f"{'='*60}")
                        analyze_frame(frame)
                    if frame_count >= max_frames:
                        print(f"\n[Stopped after {max_frames} frames]")
                        return
    except KeyboardInterrupt:
        print(f"\n[Interrupted — received {frame_count} frames]")


def cmd_list_matches(competition: str | None = None, locale: str = "fr",
                     live_only: bool = False, output_file: str | None = None):
    all_matches: list[dict] = []
    seen_ids: set = set()
    offset = 0
    page = 0

    total_available = None

    while True:
        payload = payload_list_matches("football", competition, locale, offset)
        frame = fetch_first_frame_raw(payload)
        if not frame:
            print(f"  [page {page}] no frame received — stopping", file=sys.stderr)
            break

        page_matches, total_avail = parse_matches_frame(frame)
        if total_available is None and total_avail:
            total_available = total_avail
            print(f"  [total available on server: {total_available}]",
                  file=sys.stderr)

        new_matches = [m for m in page_matches if m["match_id"] not in seen_ids]

        if not new_matches:
            break

        for m in new_matches:
            seen_ids.add(m["match_id"])
            all_matches.append(m)

        print(f"  [page {page}] offset={offset} → {len(new_matches)} matches (total {len(all_matches)})",
              file=sys.stderr)

        if len(page_matches) < 20:
            break

        if total_available and len(all_matches) >= total_available:
            break

        offset += 20
        page += 1

    if live_only:
        all_matches = [m for m in all_matches if m["live"]]

    live_count = sum(1 for m in all_matches if m["live"])
    result = {
        "total":       len(all_matches),
        "live":        live_count,
        "prematch":    len(all_matches) - live_count,
        "competition": competition or "all",
        "matches":     all_matches,
    }

    formatted = json.dumps(result, ensure_ascii=False, indent=2)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(formatted)
        print(f"  → {len(all_matches)} matches ({live_count} live, {result['prematch']} prematch) → {output_file}")
    else:
        print(formatted)


def cmd_batch_odds(matched_file: str, output_file: str, locale: str = "fr",
                   workers: int = 8, market_code: str = "ca_ftb_rslt"):
    with open(matched_file, encoding="utf-8") as f:
        data = json.load(f)

    matched = data.get("matched", data) if isinstance(data, dict) else data
    match_ids = list({m["betclic"]["match_id"] for m in matched
                      if m.get("betclic", {}).get("match_id")})
    print(f"  Fetching 1X2 odds for {len(match_ids)} matches (workers={workers})...",
          file=sys.stderr)

    odds_db = {}
    errors  = 0

    def fetch_one(match_id):
        try:
            frame = fetch_first_frame(match_id, locale, market_code)
            if not frame:
                return match_id, None
            result = build_clean_json(frame)
            for mkt in result.get("markets", []):
                sels = mkt.get("selections", [])
                if len(sels) >= 2:
                    odds = {s["name"]: s["odd"] for s in sels if s.get("odd")}
                    if odds:
                        return match_id, odds
            return match_id, None
        except Exception:
            return match_id, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_one, mid): mid for mid in match_ids}
        done = 0
        for fut in concurrent.futures.as_completed(futures):
            mid, odds = fut.result()
            done += 1
            if odds:
                odds_db[str(mid)] = odds
            else:
                errors += 1
            if done % 20 == 0 or done == len(match_ids):
                print(f"  [{done}/{len(match_ids)}] {len(odds_db)} ok, {errors} failed",
                      file=sys.stderr)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(odds_db, f, ensure_ascii=False, indent=2)
    print(f"  → {len(odds_db)} matches with odds → {output_file}")


# ── Help ──────────────────────────────────────────────────────────────────────

HELP = """
Betclic gRPC-web API Client

Commands:
  --live-count                                    GetLiveCount
  --list-matches [--comp <code>] [--live]         List all football matches with IDs
  --list-matches [--out file.json]                Save match list to file
  --match <id>                                    Raw debug frame analysis
  --clean <id> [--market <key>] [--out file.json] Clean JSON output (markets + odds)
  --stream <id> [--frames N] [--clean]            Stream live updates
  --dump-raw <id>                                 Print raw hex of first frame
  --inspect <hex>                                 Decode any raw hex response
  --locale <loc>                                  Set locale (default: fr)

Market filter keys for --market (requests only that category from the API):
  top     → ca_ftb_top    — top/featured markets (1X2, O/U 2.5, ...)
  rslt    → ca_ftb_rslt   — match result 1X2 only
  gsc     → ca_ftb_gsc    — goal scorers (buteurs)
  goa     → ca_ftb_goa    — goals over/under
  cshcp   → ca_ftb_cshcp  — correct score + handicap
  (none)  → all markets

Examples:
  python -m cli.commands --list-matches
  python -m cli.commands --list-matches --live
  python -m cli.commands --list-matches --comp ftb_rsm
  python -m cli.commands --list-matches --out matches.json
  python -m cli.commands --clean 950041120653312
  python -m cli.commands --clean 950041120653312 --market top
"""


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or "--help" in args:
        print(HELP)
        sys.exit(0)

    locale = "fr"
    if "--locale" in args:
        i = args.index("--locale")
        locale = args[i+1]

    if "--live-count" in args:
        cmd_live_count(locale)

    elif "--clean" in args:
        i = args.index("--clean")
        match_id = int(args[i+1])
        out_file = None
        if "--out" in args:
            j = args.index("--out")
            out_file = args[j+1]
        market_code = None
        if "--market" in args:
            j = args.index("--market")
            key = args[j+1]
            market_code = MARKET_FILTERS.get(key, key)
            if key not in MARKET_FILTERS:
                print(f"  [warning] unknown market key '{key}' — using raw value '{key}'")
                print(f"  known keys: {', '.join(MARKET_FILTERS.keys())}")
        cmd_clean(match_id, locale, out_file, market_code)

    elif "--match" in args:
        i = args.index("--match")
        match_id = int(args[i+1])
        cmd_match(match_id, locale)

    elif "--stream" in args:
        i = args.index("--stream")
        match_id = int(args[i+1])
        max_frames = 10
        if "--frames" in args:
            j = args.index("--frames")
            max_frames = int(args[j+1])
        clean_mode = "--clean" in args
        cmd_stream(match_id, locale, max_frames, clean_mode)

    elif "--list-matches" in args:
        comp = None
        if "--comp" in args:
            j = args.index("--comp")
            comp = args[j+1]
        live_only = "--live" in args
        out_file = None
        if "--out" in args:
            j = args.index("--out")
            out_file = args[j+1]
        cmd_list_matches(comp, locale, live_only, out_file)

    elif "--dump-matches" in args:
        comp = None
        if "--comp" in args:
            j = args.index("--comp"); comp = args[j+1]
        payload = payload_list_matches("football", comp, locale, 0)
        frame = fetch_first_frame_raw(payload)
        if not frame:
            print("no frame"); sys.exit(1)
        print("Frame size:", len(frame), "bytes\n")
        d = decode_proto(frame)
        def show(d, indent=0):
            prefix = "  " * indent
            if isinstance(d, dict):
                for k, v in d.items():
                    if isinstance(v, list):
                        print(f"{prefix}[{k}] x{len(v)}")
                        for i, item in enumerate(v[:3]):
                            show(item, indent+1)
                        if len(v) > 3:
                            print(f"{prefix}  ... ({len(v)-3} more)")
                    elif isinstance(v, dict):
                        print(f"{prefix}[{k}] dict")
                        show(v, indent+1)
                    else:
                        val_str = repr(v)[:80]
                        print(f"{prefix}[{k}] = {val_str}")
            else:
                print(f"{prefix}{repr(d)[:80]}")
        show(d)

    elif "--batch-odds" in args:
        i = args.index("--batch-odds")
        matched_file = args[i+1]
        out_file = args[args.index("--out")+1] if "--out" in args else "betclic_odds.json"
        w = int(args[args.index("--workers")+1]) if "--workers" in args else 8
        cmd_batch_odds(matched_file, out_file, locale, workers=w)

    elif "--dump-raw" in args:
        i = args.index("--dump-raw")
        match_id = int(args[i+1])
        cmd_dump_raw(match_id, locale)

    elif "--inspect" in args:
        i = args.index("--inspect")
        cmd_inspect(args[i+1])

    else:
        print(HELP)
