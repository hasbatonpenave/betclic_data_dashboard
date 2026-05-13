"""
scripts/betclic3_cli.py — Betclic command-line interface.

Usage:
  python scripts/betclic3_cli.py --live-count
  python scripts/betclic3_cli.py --clean <match_id>
  python scripts/betclic3_cli.py --stream <match_id> --clean --frames 5
  python scripts/betclic3_cli.py --list-matches --live
  python scripts/betclic3_cli.py --help
"""

import sys
import json

from proto.codec import (
    decode_proto, analyze_frame, build_clean_json, parse_matches_frame,
    payload_live_count, payload_get_match, payload_list_matches,
)
from proto.client import (
    BASE, HEADERS, MARKET_FILTERS,
    api_call, fetch_first_frame, fetch_first_frame_raw,
    cmd_list_matches,
)
import requests
import struct
import concurrent.futures

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
    """Decode any raw hex as a gRPC-web response."""
    raw = bytes.fromhex(hex_str.replace(" ", ""))
    print(f"=== Inspect ({len(raw)} bytes) ===")
    msgs, trailers = parse_grpc_response(raw)
    print(f"  Trailers: {trailers}")
    for i, m in enumerate(msgs):
        print(f"\n--- Message {i} ({len(m)} bytes) ---")
        analyze_frame(m)


def cmd_dump_raw(match_id: int, locale: str = "fr"):
    """Dump raw hex of first complete gRPC frame — paste into --inspect later."""
    p = payload_get_match(match_id, locale)
    r = requests.post(f"{BASE}/GetMatchWithNotification", headers=HEADERS, data=p,
                      timeout=(10, 30), stream=True, verify=False)
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


def cmd_clean(match_id: int, locale: str = "fr", output_file: str | None = None, market_code: str | None = None):
    """
    Fetch match data and output a clean JSON with markets + odds.
    Uses fast first-frame fetch — closes connection as soon as data arrives.
    Pass market_code to request only a specific market category from the API.
    If output_file is given, write to file; otherwise print to stdout.
    """
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
        # Also print summary
        n_markets = len(result.get("markets", []))
        n_sels    = sum(len(m.get("selections", [])) for m in result.get("markets", []))
        print(f"  → {n_markets} markets, {n_sels} selections total")
    else:
        print(formatted)

def cmd_stream(match_id: int, locale: str = "fr", max_frames: int = 10, clean: bool = False):
    """
    Stream mode: stay connected and print each gRPC frame as it arrives.
    Betclic pushes live odds updates continuously — this captures them all.
    With --clean, each frame is printed as compact JSON instead of the debug report.
    """
    p = payload_get_match(match_id, locale)
    print(f"=== STREAM — match {match_id} (Ctrl+C to stop) ===")
    r = requests.post(f"{BASE}/GetMatchWithNotification", headers=HEADERS, data=p,
                      timeout=(10, 120), stream=True, verify=False)
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


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

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
  python betclic3.py --list-matches
  python betclic3.py --list-matches --live
  python betclic3.py --list-matches --comp ftb_rsm
  python betclic3.py --list-matches --out matches.json
  python betclic3.py --clean 950041120653312
  python betclic3.py --clean 950041120653312 --market top
  python betclic3.py --clean 950041120653312 --market rslt
  python betclic3.py --clean 950041120653312 --market gsc
  python betclic3.py --clean 950041120653312 --market goa
  python betclic3.py --clean 950041120653312 --market cshcp
  python betclic3.py --clean 950041120653312 --market rslt --out result.json
  python betclic3.py --stream 950041120653312 --clean --frames 5
"""

def cmd_batch_odds(matched_file: str, output_file: str, locale: str = "fr",
                   workers: int = 8, market_code: str = "ca_ftb_rslt"):
    """
    Fetch 1X2 odds for all matches in a matched.json file (from arb_matcher.py).
    Uses ThreadPoolExecutor for parallel fetching.
    Output: {match_id: {"TeamName": odds, ...}}
    """
    import concurrent.futures, sys as _sys

    with open(matched_file, encoding="utf-8") as f:
        data = json.load(f)

    matched = data.get("matched", data) if isinstance(data, dict) else data
    match_ids = list({m["betclic"]["match_id"] for m in matched
                      if m.get("betclic", {}).get("match_id")})
    print(f"  Fetching 1X2 odds for {len(match_ids)} matches (workers={workers})...",
          file=_sys.stderr)

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
                      file=_sys.stderr)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(odds_db, f, ensure_ascii=False, indent=2)
    print(f"  → {len(odds_db)} matches with odds → {output_file}")


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
            market_code = MARKET_FILTERS.get(key, key)  # alias or raw code
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
        # Debug: dump raw proto structure of GetMatchesBySportWithNotifications
        comp = None
        if "--comp" in args:
            j = args.index("--comp"); comp = args[j+1]
        payload = payload_list_matches("football", comp, locale, 0)
        frame = fetch_first_frame_raw(payload)
        if not frame:
            print("no frame"); sys.exit(1)
        print("Frame size:", len(frame), "bytes\n")
        # Print top-level field numbers and types
        d = decode_proto(frame)
        def show(d, indent=0):
            prefix = "  " * indent
            if isinstance(d, dict):
                for k, v in d.items():
                    if isinstance(v, list):
                        print(f"{prefix}[{k}] x{len(v)}")
                        for i, item in enumerate(v[:3]):  # show first 3
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

