"""
api/client.py — async Betclic HTTP client.

Used by the feed only. Never imported by CLI tools.
The feed creates one BetclicClient per aiohttp.ClientSession (one session
shared across all concurrent streams).
"""
from __future__ import annotations
import ssl
import struct
import logging
from typing import AsyncIterator

import aiohttp

from proto.codec import (
    payload_get_match, payload_list_matches,
    parse_grpc_response, build_clean_json, parse_matches_frame,
)
from api.models import OddsFrame

log = logging.getLogger(__name__)

BASE = (
    "https://offering.begmedia.com/web/offering.access.api"
    "/offering.access.api.MatchService"
)

HEADERS: dict[str, str] = {
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


class BetclicClient:
    """
    Async wrapper around the Betclic gRPC-web API.

    Usage:
        connector = aiohttp.TCPConnector(limit_per_host=80)
        async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
            client = BetclicClient(session)
            async for frame in client.stream_match(match_id, "ca_ftb_rslt"):
                result = build_clean_json(frame)
    """

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    # ── Streaming ─────────────────────────────────────────────────────────────

    async def stream_match(
        self,
        match_id: int,
        market_code: str,
        locale: str = "fr",
    ) -> AsyncIterator[bytes]:
        """
        Yield raw proto frame bytes as they arrive from the server.
        Keeps the HTTP connection open indefinitely — the server pushes frames
        whenever odds change. Caller is responsible for reconnect logic.
        """
        payload = payload_get_match(match_id, locale, market_code)
        timeout  = aiohttp.ClientTimeout(connect=10, total=None, sock_read=300)

        async with self._session.post(
            f"{BASE}/GetMatchWithNotification",
            data=payload,
            timeout=timeout,
        ) as resp:
            resp.raise_for_status()
            buf = bytearray()

            async for chunk in resp.content.iter_any():
                buf += chunk

                while len(buf) >= 5:
                    flags  = buf[0]
                    length = struct.unpack_from(">I", buf, 1)[0]

                    if len(buf) < 5 + length:
                        break  # incomplete frame

                    frame = bytes(buf[5 : 5 + length])
                    del buf[: 5 + length]

                    if flags & 0x80:
                        return  # trailer frame — server closed the stream

                    yield frame

    # ── Match listing ─────────────────────────────────────────────────────────

    async def list_matches(
        self, locale: str = "fr"
    ) -> tuple[list[dict], int]:
        """
        Fetch all football matches by paginating GetMatchesBySportWithNotifications.
        Returns (matches, total_available).
        Replaces cmd_list_matches() + run_in_executor — fully async, no disk I/O.
        """
        all_matches: list[dict] = []
        seen_ids: set[int] = set()
        total_available: int | None = None
        offset = 0

        while True:
            payload = payload_list_matches("football", None, locale, offset)
            frame   = await self._fetch_first_frame(
                "GetMatchesBySportWithNotifications", payload, timeout_s=15.0
            )
            if not frame:
                log.warning("list_matches: no frame at offset %d — stopping", offset)
                break

            page_matches, total = parse_matches_frame(frame)
            if total_available is None and total:
                total_available = total
                log.debug("list_matches: %d total matches on server", total)

            new = [m for m in page_matches if m.get("match_id") not in seen_ids]
            if not new:
                break

            for m in new:
                seen_ids.add(m["match_id"])
                all_matches.append(m)

            log.debug(
                "list_matches: offset=%d page=%d new=%d total=%d",
                offset, len(page_matches), len(new), len(all_matches),
            )

            if len(page_matches) < 20:
                break  # partial page — last page
            if total_available and len(all_matches) >= total_available:
                break

            offset += 20

        return all_matches, total_available or len(all_matches)

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch_first_frame(
        self, endpoint: str, payload: bytes, timeout_s: float = 10.0
    ) -> bytes:
        """Connect, grab the first complete gRPC data frame, close immediately."""
        timeout = aiohttp.ClientTimeout(connect=5, total=timeout_s)
        try:
            async with self._session.post(
                f"{BASE}/{endpoint}", data=payload, timeout=timeout
            ) as resp:
                resp.raise_for_status()
                buf = bytearray()
                async for chunk in resp.content.iter_any():
                    buf += chunk
                    if len(buf) < 5:
                        continue
                    flags  = buf[0]
                    length = struct.unpack_from(">I", buf, 1)[0]
                    if flags & 0x80:
                        return b""  # trailer only
                    if len(buf) >= 5 + length:
                        return bytes(buf[5 : 5 + length])
        except Exception as exc:
            log.debug("_fetch_first_frame %s error: %s", endpoint, exc)
        return b""


def make_session(max_per_host: int = 80) -> aiohttp.ClientSession:
    """
    Factory for the shared aiohttp session.
    Call once at startup; pass the session to BetclicClient.
    """
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    connector = aiohttp.TCPConnector(
        limit_per_host=max_per_host,
        ttl_dns_cache=600,
        enable_cleanup_closed=True,
        force_close=False,
        ssl=ssl_ctx,
    )
    return aiohttp.ClientSession(
        headers=HEADERS,
        connector=connector,
        connector_owner=True,
    )
