"""
server/app.py — FastAPI application for the Betclic price feed.

Endpoints
---------
GET /stream        Server-Sent Events — all price updates in real-time
GET /prices        Current in-memory snapshot  {match_id: {market: {sel: odd}}}
GET /markets       Active matches + meta        {match_id: {match, competition, ...}}
GET /status        Feed stats from feed.manager
GET /history       SQLite price history for one match+selection

Architecture
------------
feed.manager.run()  →  asyncio.Queue  →  consume_feed()
                                              ├─ updates in-memory prices dict
                                              ├─ fans out to SSE subscribers (non-blocking)
                                              └─ pushes rows to storage.sqlite._db_queue

storage.sqlite._db_queue  →  _sqlite_writer() [daemon thread]
                                └─ batched INSERT every 500ms or 100 rows
"""

import asyncio
import json
import time
import sqlite3
import threading
import sys
import os
import resource
from contextlib import asynccontextmanager

# Raise the soft FD limit to the OS hard limit before any sockets are opened.
# Default macOS soft limit is 256; 600 matches × 2 markets = 1200 streams need more.
try:
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    _target = min(_hard, 65536)
    if _soft < _target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (_target, _hard))
        print(f"[betclic_backend] RLIMIT_NOFILE raised {_soft} → {_target}", file=sys.stderr)
except Exception as _exc:
    print(f"[betclic_backend] could not raise RLIMIT_NOFILE: {_exc}", file=sys.stderr)
from typing import AsyncGenerator

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from feed import manager as betclic_feed

# ── CONFIG ────────────────────────────────────────────────────────────────────
from config import settings
from storage.sqlite import _db_queue, DB_PATH, _sqlite_writer
PORT = settings.port

# ── IN-MEMORY STATE ───────────────────────────────────────────────────────────
# prices[match_id][market][selection] = latest odd
prices: dict[str, dict[str, dict[str, float]]] = {}
prices_lock = asyncio.Lock()

# SSE subscribers — one asyncio.Queue per connected client
_subscribers: set[asyncio.Queue] = set()

# ── SQLITE WRITER (background daemon thread) ──────────────────────────────────
async def consume_feed(q: asyncio.Queue):
    """
    Reads updates from betclic_feed's queue.
    Updates in-memory prices, fans out to SSE clients, enqueues SQLite row.
    No blocking I/O — SQLite write is offloaded to the daemon thread.
    """
    while True:
        update: dict = await q.get()

        match_id = update["match_id"]
        market   = update["market"]
        odds     = update["odds"]        # {selection: float}
        meta     = update.get("meta", {})
        ts       = time.time()

        # ── 1. Update in-memory prices ─────────────────────────────────────
        async with prices_lock:
            mdata = prices.setdefault(match_id, {})
            mdata.setdefault(market, {}).update(odds)

        # ── 2. Build SSE payload ────────────────────────────────────────────
        event_json = json.dumps({
            "type":     "price",
            "match_id": match_id,
            "market":   market,
            "odds":     odds,
            "meta":     meta,
            "ts":       ts,
        }, ensure_ascii=False)

        # ── 3. Fan-out to SSE subscribers (put_nowait — never blocks) ───────
        dead: list[asyncio.Queue] = []
        for sub in list(_subscribers):
            try:
                sub.put_nowait(event_json)
            except asyncio.QueueFull:
                dead.append(sub)   # slow client — drop
        for sub in dead:
            _subscribers.discard(sub)

        # ── 4. Enqueue SQLite row (non-blocking — goes to daemon thread) ────
        match_name  = meta.get("match", "")
        competition = meta.get("competition", "")
        match_date  = meta.get("date", "")
        is_live     = int(bool(meta.get("live", False)))

        for selection, odd in odds.items():
            _db_queue.put_nowait((
                ts, match_id, market, selection, odd,
                match_name, competition, match_date, is_live,
            ))


# ── LIFESPAN ──────────────────────────────────────────────────────────────────

_feed_queue: asyncio.Queue | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _feed_queue

    # Start SQLite daemon thread first
    db_thread = threading.Thread(
        target=_sqlite_writer, daemon=True, name="betclic-db-writer"
    )
    db_thread.start()
    print("[betclic_backend] SQLite writer thread started", file=sys.stderr)

    # Create feed queue and launch tasks
    _feed_queue = asyncio.Queue(maxsize=20_000)
    feed_task     = asyncio.create_task(betclic_feed.run(_feed_queue), name="betclic-feed")
    consumer_task = asyncio.create_task(consume_feed(_feed_queue),      name="betclic-consumer")
    print(f"[betclic_backend] Feed and consumer tasks running on port {PORT}", file=sys.stderr)

    yield  # ── server is alive ───────────────────────────────────────────────

    # Graceful shutdown
    betclic_feed.stop()
    feed_task.cancel()
    consumer_task.cancel()
    _db_queue.put(None)   # stop the writer thread
    print("[betclic_backend] Shutdown complete", file=sys.stderr)


# ── APP ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="Betclic Feed Backend", version="1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── SSE /stream ───────────────────────────────────────────────────────────────

@app.get("/stream")
async def stream_sse():
    """
    Server-Sent Events endpoint.
    Each event: data: <json>\n\n
    Keepalive comment every 25 s to prevent proxy timeouts.
    """
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=500)
    _subscribers.add(q)

    # Send a snapshot of current prices immediately on connect
    async with prices_lock:
        snapshot = dict(prices)

    snapshot_json = json.dumps({
        "type":   "snapshot",
        "prices": snapshot,
        "ts":     time.time(),
    }, ensure_ascii=False)

    async def generator() -> AsyncGenerator[str, None]:
        try:
            yield f"data: {snapshot_json}\n\n"   # initial snapshot
            while True:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=25.0)
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"       # comment line — clients ignore it
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            _subscribers.discard(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering
            "Connection":       "keep-alive",
        },
    )


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/prices")
async def get_prices():
    """Full in-memory odds snapshot."""
    async with prices_lock:
        return JSONResponse(dict(prices))


@app.get("/markets")
async def get_markets():
    """Active matches with metadata."""
    all_meta = betclic_feed.get_all_meta()
    async with prices_lock:
        active_ids = set(prices.keys())
    result = {mid: all_meta.get(mid, {}) for mid in active_ids}
    return JSONResponse(result)


@app.get("/status")
async def get_status():
    """Feed stats: streams count, update count, last update time."""
    stats = betclic_feed.get_stats()
    stats["sse_clients"] = len(_subscribers)
    stats["prices_in_memory"] = len(prices)
    return JSONResponse(stats)


@app.get("/history")
async def get_history(
    match_id:  str = Query(...,   description="Betclic match_id"),
    selection: str = Query(...,   description="Selection name e.g. '1', 'Nul', '2'"),
    market:    str = Query("1X2", description="Market: 1X2 | O/U"),
    limit:     int = Query(500,   ge=1, le=5000),
):
    """
    Return recent price history for one selection from SQLite.
    Non-blocking: query runs in a thread executor.
    """
    def _query() -> list[dict]:
        con = sqlite3.connect(DB_PATH, check_same_thread=True)
        try:
            rows = con.execute(
                """
                SELECT ts, odd
                FROM   betclic_prices
                WHERE  match_id  = ?
                  AND  market    = ?
                  AND  selection = ?
                ORDER  BY ts DESC
                LIMIT  ?
                """,
                (match_id, market, selection, limit),
            ).fetchall()
            return [{"ts": r[0], "odd": r[1]} for r in reversed(rows)]
        finally:
            con.close()

    rows = await asyncio.get_event_loop().run_in_executor(None, _query)
    return JSONResponse(rows)


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "server.app:app",
        host="0.0.0.0",
        port=PORT,
        reload=False,
        log_level="info",
    )
