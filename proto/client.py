"""
proto/client.py — synchronous HTTP client for the Betclic gRPC-web API.

Uses the `requests` library. Intended for CLI / debug use only.
The async feed uses api/client.py instead.
"""
from __future__ import annotations
import struct
import requests

from proto.codec import parse_grpc_response

BASE = "https://offering.begmedia.com/web/offering.access.api/offering.access.api.MatchService"

HEADERS = {
    "accept":                   "*/*",
    "accept-language":          "fr,en-US;q=0.9,en;q=0.8",
    "content-type":             "application/grpc-web+proto",
    "dnt":                      "1",
    "ngsw-bypass":              "1",
    "origin":                   "https://www.betclic.fr",
    "referer":                  "https://www.betclic.fr/",
    "user-agent":               (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
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
    r = requests.post(url, headers=HEADERS, data=payload, timeout=(10, 30), stream=True)
    print(f"  status   : HTTP {r.status_code}")

    raw = b""
    try:
        for chunk in r.iter_content(chunk_size=4096):
            raw += chunk
            if len(raw) >= 5:
                flags  = raw[0]
                length = struct.unpack('>I', raw[1:5])[0]
                if not (flags & 0x80) and len(raw) >= 5 + length:
                    next_i = 5 + length
                    if next_i < len(raw) and (raw[next_i] & 0x80):
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


def fetch_first_frame(match_id: int, locale: str = "fr", market_code: str | None = None) -> bytes:
    """
    Connect, grab the first complete gRPC data frame, then close immediately.
    Pass market_code to request only a specific market category.
    """
    from proto.codec import payload_get_match

    p = payload_get_match(match_id, locale, market_code)
    r = requests.post(
        f"{BASE}/GetMatchWithNotification",
        headers=HEADERS, data=p,
        timeout=(5, 10), stream=True,
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


def fetch_first_frame_raw(payload: bytes, endpoint: str = "GetMatchesBySportWithNotifications") -> bytes:
    """Like fetch_first_frame but takes a pre-built payload and optional endpoint."""
    r = requests.post(
        f"{BASE}/{endpoint}",
        headers=HEADERS, data=payload,
        timeout=(5, 15), stream=True,
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
