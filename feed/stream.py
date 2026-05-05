"""
feed/stream.py — one coroutine per match × market, with circuit breaker.
"""
from __future__ import annotations
import asyncio
import logging
import time

from api.client import BetclicClient
from api.models import OddsUpdate, MatchMeta, LiveState
from proto.codec import build_clean_json, extract_live_state
from config import settings

log = logging.getLogger(__name__)

# Market names to skip when searching for the main 1X2 market
_SKIP = [
    "qualification", "remboursé", "rembourse",
    "prolongation", "tirs au but", "chaque mi-temps",
    "gagne au moins", "1ère mi-temps", "2ème mi-temps", "mi-temps / fin",
]

KNOWN_1X2_KEYS = {"1", "2", "Nul", "N", "X", "1X", "12", "X2"}


# ── Circuit breaker ───────────────────────────────────────────────────────────

class CircuitBreaker:
    """
    Per-stream failure tracker with exponential backoff and a long park period.

    States:
      CLOSED  → stream is healthy, reconnect immediately on transient errors
      OPEN    → too many failures, park for cb_reset_after_s before retrying
    """

    def __init__(self) -> None:
        self._failures   = 0
        self._open_until = 0.0

    @property
    def is_open(self) -> bool:
        if self._open_until and time.monotonic() < self._open_until:
            return True
        if self._open_until:
            log.info("circuit half-open — resetting failure count")
            self._failures   = 0
            self._open_until = 0.0
        return False

    def record_success(self) -> None:
        self._failures   = 0
        self._open_until = 0.0

    def next_delay(self) -> float:
        """Record a failure and return how long to sleep before retrying."""
        self._failures += 1
        if self._failures >= settings.cb_max_failures:
            self._open_until = time.monotonic() + settings.cb_reset_after_s
            log.warning(
                "circuit OPEN after %d failures — parking for %.0fs",
                self._failures, settings.cb_reset_after_s,
            )
            return settings.cb_reset_after_s
        delay = min(settings.reconnect_delay_s * (2 ** (self._failures - 1)), 60.0)
        return delay


# ── Odds extraction ───────────────────────────────────────────────────────────

def _extract_1x2_odds(frame_result: dict) -> dict[str, float] | None:
    all_markets = frame_result.get("markets", [])

    for priority in ["résultat du match (tps rég", "résultat du match"]:
        for mkt in all_markets:
            mname = (mkt.get("name") or "").lower()
            if priority in mname and not any(s in mname for s in _SKIP):
                odds = {
                    s["name"]: s["odd"]
                    for s in mkt.get("selections", [])
                    if s.get("odd") and s.get("name") and not s["name"].startswith("_")
                }
                if 2 <= len(odds) <= 3:
                    if any(k in KNOWN_1X2_KEYS for k in odds):
                        return odds

    for mkt in all_markets:
        mname = (mkt.get("name") or "").lower()
        if any(s in mname for s in _SKIP):
            continue
        odds = {
            s["name"]: s["odd"]
            for s in mkt.get("selections", [])
            if s.get("odd") and s.get("name") and not s["name"].startswith("_")
        }
        if 2 <= len(odds) <= 3 and any(k in KNOWN_1X2_KEYS for k in odds):
            return odds

    return None


def _extract_ou_odds(frame_result: dict) -> dict[str, float] | None:
    for mkt in frame_result.get("markets", []):
        mname = (mkt.get("name") or "").lower()
        if "but" not in mname and "goal" not in mname and "over" not in mname:
            continue
        odds = {
            s["name"]: s["odd"]
            for s in mkt.get("selections", [])
            if s.get("odd") and s.get("name")
        }
        if 2 <= len(odds) <= 4:
            return odds
    return None


EXTRACTORS = {
    "1X2": _extract_1x2_odds,
    "O/U": _extract_ou_odds,
}


# ── Stream coroutine ──────────────────────────────────────────────────────────

async def run_stream(
    client:      BetclicClient,
    match_id:    int,
    market_code: str,
    market_name: str,
    meta_cache:  dict[str, MatchMeta],
    queue:       asyncio.Queue[OddsUpdate],
    stop_ev:     asyncio.Event,
) -> None:
    """
    One long-lived coroutine per match × market.
    Reconnects automatically on error using exponential backoff.
    Parks for cb_reset_after_s seconds after cb_max_failures consecutive failures.
    """
    mid_str  = str(match_id)
    breaker  = CircuitBreaker()
    extractor = EXTRACTORS.get(market_name)

    while not stop_ev.is_set():

        # Circuit open — sleep then retry
        if breaker.is_open:
            try:
                await asyncio.wait_for(stop_ev.wait(), timeout=settings.cb_reset_after_s)
                return
            except asyncio.TimeoutError:
                pass
            continue

        try:
            async for frame in client.stream_match(match_id, market_code, settings.locale):
                if stop_ev.is_set():
                    return

                # Parse frame
                try:
                    result = build_clean_json(frame)
                except Exception:
                    continue

                # Update live state in meta cache
                try:
                    live_state = LiveState(**extract_live_state(frame))
                    existing   = meta_cache.get(mid_str, MatchMeta())
                    meta_cache[mid_str] = existing.update_live(live_state)
                except Exception:
                    pass

                # Extract odds
                if extractor is None:
                    continue
                odds = extractor(result)
                if not odds:
                    continue

                meta   = meta_cache.get(mid_str, MatchMeta())
                update = OddsUpdate(
                    match_id = mid_str,
                    market   = market_name,
                    odds     = odds,
                    meta     = meta,
                    ts       = time.time(),
                )

                # Drop oldest if full (keep freshest data)
                try:
                    queue.put_nowait(update)
                except asyncio.QueueFull:
                    try:
                        queue.get_nowait()
                        queue.put_nowait(update)
                    except (asyncio.QueueEmpty, asyncio.QueueFull):
                        pass

            # Stream ended cleanly (trailer received)
            breaker.record_success()

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            if stop_ev.is_set():
                return
            delay = breaker.next_delay()
            log.warning(
                "stream %s/%s error (%s): %s — retry in %.1fs",
                mid_str, market_code, type(exc).__name__, exc, delay,
            )
            try:
                await asyncio.wait_for(stop_ev.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass
