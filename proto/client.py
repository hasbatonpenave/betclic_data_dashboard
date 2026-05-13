"""
proto/client.py — Betclic sync HTTP client.

Wraps the gRPC-web HTTP endpoints with requests.
Provides payload builders and the synchronous fetchers used by
feed/manager.py (via run_in_executor) and scripts/betclic3_cli.py.
"""

import struct
import json
import sys
import os
import tempfile

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from proto.codec import (
    MARKET_FILTERS,
    grpc_frame, parse_grpc_response, decode_proto,
    build_clean_json, analyze_frame,
    payload_live_count, payload_get_match, payload_list_matches,
    parse_matches_frame,
)

BASE = "https://offering.begmedia.com/web/offering.access.api/offering.access.api.MatchService"

HEADERS = {
    "accept":                   "*/*",
    "accept-language":          "fr,en-US;q=0.9,en;q=0.8",
    "content-type":             "application/grpc-web+proto",
    "dnt":                      "1",
    "ngsw-bypass":              "1",
    "origin":                   "https://www.betclic.fr",
    "referer":                  "https://www.betclic.fr/",
    "user-agent":               "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "x-bg-ref-brand":           "BETCLIC",
    "x-bg-ref-platform":        "DESKTOP",
    "x-bg-ref-regulator-zone":  "FR",
    "x-bg-regulation":          "FR",
    "x-grpc-web":               "1",
}


def api_call(method: str, payload: bytes) -> tuple[list, dict]:
    url = f"{BASE}/{method}"
    print(f"\n→ {method}")
    print(f"  payload  : {payload.hex()}")
    r = requests.post(url, headers=HEADERS, data=payload, timeout=(10, 30), stream=True, verify=False)
    print(f"  status   : HTTP {r.status_code}")

    # Read all chunks with a cap to avoid hanging forever
    raw = b""
    try:
        for chunk in r.iter_content(chunk_size=4096):
            raw += chunk
            # Stop once we have at least one complete gRPC frame
            if len(raw) >= 5:
                flags  = raw[0]
                length = struct.unpack('>I', raw[1:5])[0]
                if not (flags & 0x80) and len(raw) >= 5 + length:
                    # Got a full data frame — check if there's a trailer right after
                    next_i = 5 + length
                    if next_i < len(raw) and (raw[next_i] & 0x80):
                        # Trailer frame follows — read it too
                        if len(raw) >= next_i + 5:
                            tlen = struct.unpack('>I', raw[next_i+1:next_i+5])[0]
                            if len(raw) >= next_i + 5 + tlen:
                                break
    except requests.exceptions.Timeout:
        print("  [stream ended / timeout — parsing what we got]")
    except Exception as e:
        print(f"  [stream error: {e} — parsing what we got]")

    print(f"  raw hex  : {raw[:160].hex()}{'...' if len(raw)>160 else ''}")
    msgs, trailers = parse_grpc_response(raw)
    print(f"  trailers : {trailers}")
    return msgs, trailers

# ──────────────────────────────────────────────────────────────────────────────
# COMMANDS
# ──────────────────────────────────────────────────────────────────────────────


def fetch_first_frame(match_id: int, locale: str = "fr", market_code: str | None = None) -> bytes:
    """
    Connect, grab the first complete gRPC data frame, then close immediately.
    Much faster than api_call() — no waiting for trailers or later frames.
    Typically returns in ~300ms instead of 30s.
    Pass market_code to request only a specific market category.
    """
    p = payload_get_match(match_id, locale, market_code)
    r = requests.post(
        f"{BASE}/GetMatchWithNotification",
        headers=HEADERS, data=p,
        timeout=(5, 10), stream=True, verify=False,
    )
    buf = b""
    try:
        for chunk in r.iter_content(chunk_size=4096):
            buf += chunk
            if len(buf) < 5:
                continue
            flags  = buf[0]
            length = struct.unpack(">I", buf[1:5])[0]
            if flags & 0x80:
                break   # trailer only, no data frame
            if len(buf) >= 5 + length:
                return buf[5:5 + length]   # full frame received — cut connection
    except Exception:
        pass
    finally:
        r.close()
    return b""


def fetch_first_frame_raw(payload: bytes, endpoint: str = "GetMatchesBySportWithNotifications") -> bytes:
    """Like fetch_first_frame but takes a pre-built payload and optional endpoint."""
    r = requests.post(
        f"{BASE}/{endpoint}",
        headers=HEADERS, data=payload,
        timeout=(5, 15), stream=True, verify=False,
    )
    buf = b""
    try:
        for chunk in r.iter_content(chunk_size=4096):
            buf += chunk
            if len(buf) < 5:
                continue
            flags  = buf[0]
            length = struct.unpack(">I", buf[1:5])[0]
            if flags & 0x80:
                break
            if len(buf) >= 5 + length:
                return buf[5:5 + length]
    except Exception:
        pass
    finally:
        r.close()
    return b""



def cmd_list_matches(competition: str | None = None, locale: str = "fr",
                     live_only: bool = False, output_file: str | None = None):
    """
    Fetch ALL football matches by paginating GetMatchesBySportWithNotifications.
    Page size = 20. Stops when a page returns 0 matches or fewer than previous.
    """
    all_matches: list[dict] = []
    seen_ids: set = set()
    offset = 0
    page = 0

    total_available = None

    while True:
        payload = payload_list_matches("football", competition, locale, offset)
        frame = fetch_first_frame_raw(payload)
        if not frame:
            print(f"  [page {page}] no frame received — stopping", file=__import__('sys').stderr)
            break

        page_matches, total_avail = parse_matches_frame(frame)
        if total_available is None and total_avail:
            total_available = total_avail
            print(f"  [total available on server: {total_available}]",
                  file=__import__('sys').stderr)

        new_matches = [m for m in page_matches if m["match_id"] not in seen_ids]

        if not new_matches:
            break  # no new matches = end of pagination

        for m in new_matches:
            seen_ids.add(m["match_id"])
            all_matches.append(m)

        print(f"  [page {page}] offset={offset} → {len(new_matches)} matches (total {len(all_matches)})",
              file=__import__('sys').stderr)

        if len(page_matches) < 20:
            break  # last page (partial)

        if total_available and len(all_matches) >= total_available:
            break  # fetched everything

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


# ──────────────────────────────────────────────────────────────────────────────
# HTTP CLIENT
# ──────────────────────────────────────────────────────────────────────────────

