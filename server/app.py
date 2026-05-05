"""
server/app.py — FastAPI application.

Replaces betclic_backend.py. Key changes from the original:
  - Uses FeedManager instead of importing betclic_feed directly
  - Uses SQLiteRepository through the PriceRepository interface
  - orjson for all serialization
  - Pre-serialized SSE snapshot (zero copy on connect)
  - OddsUpdate (Pydantic) from the queue instead of raw dict
  - asyncio.get_running_loop() throughout
  - db_thread.join() in shutdown
"""
from __future__ import annotations
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import orjson
import uvicorn
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

from api.models import OddsUpdate, PricePoint
from config import settings
from feed.manager import FeedManager
from storage.sqlite import SQLiteRepository

log = logging.getLogger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────

prices: dict[str, dict[str, dict[str, float]]] = {}
prices_lock = asyncio.Lock()

_snapshot_bytes: bytes = orjson.dumps({"type": "snapshot", "prices": {}, "ts": 0.0})
_subscribers: set[asyncio.Queue[bytes]] = set()

# ── Feed consumer ─────────────────────────────────────────────────────────────

async def consume_feed(
    q:    asyncio.Queue[OddsUpdate],
    repo: SQLiteRepository,
) -> None:
    global _snapshot_bytes

    while True:
        update: OddsUpdate = await q.get()

        # 1. Update in-memory prices (lock-free read, lock only on write)
        async with prices_lock:
            prices \
                .setdefault(update.match_id, {}) \
                .setdefault(update.market, {}) \
                .update(update.odds)
            _snapshot_bytes = orjson.dumps({
                "type":   "snapshot",
                "prices": prices,
                "ts":     update.ts,
            })

        # 2. Build SSE event as bytes
        event_bytes = orjson.dumps({
            "type":     "price",
            "match_id": update.match_id,
            "market":   update.market,
            "odds":     update.odds,
            "meta":     update.meta.model_dump(),
            "ts":       update.ts,
        })

        # 3. Fan-out to SSE subscribers — drop oldest on full queue
        dead: list[asyncio.Queue[bytes]] = []
        for sub in list(_subscribers):
            try:
                sub.put_nowait(event_bytes)
            except asyncio.QueueFull:
                try:
                    sub.get_nowait()
                    sub.put_nowait(event_bytes)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    dead.append(sub)
        for sub in dead:
            _subscribers.discard(sub)

        # 4. Persist (non-blocking — goes to SQLite daemon thread)
        repo.enqueue(update)


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    repo    = SQLiteRepository()
    repo.start()

    feed_q:   asyncio.Queue[OddsUpdate] = asyncio.Queue(maxsize=settings.feed_queue_maxsize)
    manager   = FeedManager(feed_q)

    feed_task     = asyncio.create_task(manager.run(),               name="betclic-feed")
    consumer_task = asyncio.create_task(consume_feed(feed_q, repo),  name="betclic-consumer")

    app.state.manager = manager
    app.state.repo    = repo

    log.info("Feed and consumer tasks started on port %d", settings.port)

    yield

    # Graceful shutdown
    manager.stop()
    feed_task.cancel()
    consumer_task.cancel()
    await asyncio.gather(feed_task, consumer_task, return_exceptions=True)
    repo.stop()
    repo.join(timeout=10)
    log.info("Server shutdown complete")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="Betclic Feed", version="2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── SSE /stream ───────────────────────────────────────────────────────────────

@app.get("/stream")
async def stream_sse():
    q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=500)
    _subscribers.add(q)
    snapshot = _snapshot_bytes   # reference copy — no dict clone, no serialize

    async def generator() -> AsyncGenerator[bytes, None]:
        try:
            yield b"data: " + snapshot + b"\n\n"
            while True:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=25.0)
                    yield b"data: " + data + b"\n\n"
                except asyncio.TimeoutError:
                    yield b": keepalive\n\n"
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            _subscribers.discard(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/prices")
async def get_prices():
    async with prices_lock:
        return JSONResponse(content=prices)


@app.get("/markets")
async def get_markets():
    manager = app.state.manager
    all_meta = manager.get_all_meta()
    async with prices_lock:
        active_ids = set(prices.keys())
    result = {
        mid: meta.model_dump()
        for mid, meta in all_meta.items()
        if mid in active_ids
    }
    return JSONResponse(content=result)


@app.get("/status")
async def get_status():
    manager = app.state.manager
    stats = manager.get_stats()
    stats["sse_clients"]      = len(_subscribers)
    stats["prices_in_memory"] = len(prices)
    return JSONResponse(content=stats)


@app.get("/history")
async def get_history(
    match_id:  str = Query(...),
    selection: str = Query(...),
    market:    str = Query("1X2"),
    limit:     int = Query(500, ge=1, le=5000),
):
    repo = app.state.repo
    loop = asyncio.get_running_loop()
    rows: list[PricePoint] = await loop.run_in_executor(
        None, repo.get_history, match_id, selection, market, limit
    )
    return JSONResponse(content=[r.model_dump() for r in rows])


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(name)-24s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    uvicorn.run(
        "server.app:app",
        host="0.0.0.0",
        port=settings.port,
        reload=False,
        log_level=settings.log_level.lower(),
    )
