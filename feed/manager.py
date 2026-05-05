"""
feed/manager.py — spawns and manages one asyncio.Task per match × market.
Replaces the run() / stop() / get_stats() / get_meta() interface from betclic_feed.py.
"""
from __future__ import annotations
import asyncio
import logging
import time
from datetime import datetime, timezone

from api.client import BetclicClient, make_session
from api.models import OddsUpdate, MatchMeta
from config import settings
from feed.stream import run_stream

log = logging.getLogger(__name__)


def _is_relevant(match: dict) -> bool:
    if match.get("live"):
        return True
    date_str = match.get("date", "")
    if not date_str:
        return True
    try:
        dt   = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        now  = datetime.now(timezone.utc)
        diff = (dt - now).total_seconds()
        return -3600 <= diff <= settings.max_match_age_h * 3600
    except Exception:
        return True


class FeedManager:
    """
    Public interface consumed by server/app.py:

        manager = FeedManager(queue)
        await manager.run()          # long-running coroutine
        manager.stop()               # signal graceful shutdown
        manager.get_stats()          # -> dict
        manager.get_meta(mid)        # -> MatchMeta
        manager.get_all_meta()       # -> dict[str, MatchMeta]
    """

    def __init__(self, queue: asyncio.Queue[OddsUpdate]) -> None:
        self._queue    = queue
        self._stop_ev  = asyncio.Event()
        self._meta:    dict[str, MatchMeta]      = {}
        self._tasks:   dict[tuple, asyncio.Task] = {}
        self._stats    = {
            "streams": 0, "updates": 0, "matches": 0, "last_update": None
        }

    # ── Public API ─────────────────────────────────────────────────────────────

    def stop(self) -> None:
        self._stop_ev.set()

    def get_stats(self) -> dict:
        return {
            **self._stats,
            "active_tasks": sum(1 for t in self._tasks.values() if not t.done()),
        }

    def get_meta(self, match_id: str) -> MatchMeta:
        return self._meta.get(match_id, MatchMeta())

    def get_all_meta(self) -> dict[str, MatchMeta]:
        return dict(self._meta)

    # ── Main coroutine ─────────────────────────────────────────────────────────

    async def run(self, refresh_min: float = 5.0) -> None:
        markets = list(zip(settings.markets,
                           [settings.market_names[m] for m in settings.markets]))

        async with make_session(settings.max_streams_per_host) as session:
            client = BetclicClient(session)

            while not self._stop_ev.is_set():

                # 1. Refresh match list (fully async — no run_in_executor)
                try:
                    matches, total = await client.list_matches(settings.locale)
                except Exception as exc:
                    log.error("list_matches failed: %s — retry in 15s", exc)
                    try:
                        await asyncio.wait_for(self._stop_ev.wait(), timeout=15)
                    except asyncio.TimeoutError:
                        pass
                    continue

                relevant = [m for m in matches if _is_relevant(m)]
                self._stats["matches"] = len(relevant)
                log.info(
                    "%d total matches, %d relevant (≤%.0fh)",
                    len(matches), len(relevant), settings.max_match_age_h,
                )

                # Update meta cache from listing
                for m in matches:
                    mid = str(m.get("match_id", ""))
                    if mid:
                        self._meta[mid] = MatchMeta(
                            match       = m.get("match", ""),
                            competition = m.get("competition"),
                            date        = m.get("date"),
                            live        = m.get("live", False),
                            teams       = [t.get("name", "") for t in m.get("teams", [])],
                        )

                # 2. Spawn tasks for new matches
                spawned = 0
                for m in relevant:
                    mid = m.get("match_id")
                    if not mid:
                        continue
                    for mkt_code, mkt_name in markets:
                        key = (mid, mkt_code)
                        if key in self._tasks and not self._tasks[key].done():
                            continue
                        t = asyncio.create_task(
                            run_stream(
                                client, mid, mkt_code, mkt_name,
                                self._meta, self._queue, self._stop_ev,
                            ),
                            name=f"bc-{mid}-{mkt_code}",
                        )
                        self._tasks[key] = t
                        self._stats["streams"] += 1
                        spawned += 1

                # 3. Prune finished tasks
                dead = [k for k, t in self._tasks.items() if t.done()]
                for k in dead:
                    self._stats["streams"] -= 1
                    del self._tasks[k]

                active = len(self._tasks)
                log.info("%d active streams (%d new, %d pruned)", active, spawned, len(dead))

                # 4. Wait for next refresh or stop signal
                try:
                    await asyncio.wait_for(self._stop_ev.wait(), timeout=refresh_min * 60)
                    break
                except asyncio.TimeoutError:
                    pass

            # Graceful shutdown
            log.info("shutting down %d tasks…", len(self._tasks))
            for t in self._tasks.values():
                t.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            log.info("feed shutdown complete")
