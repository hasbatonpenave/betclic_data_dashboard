"""
storage/sqlite.py — SQLite batch writer for Betclic price history.

Runs in a daemon thread, completely isolated from the async event loop.
Consumes rows from _db_queue and batch-inserts every 500ms or 100 rows.
"""

import queue as _queue
import sqlite3
import time

from config import settings

DB_PATH = settings.db_path

_db_queue: _queue.Queue = _queue.Queue()

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS betclic_prices (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,
    match_id    TEXT    NOT NULL,
    market      TEXT    NOT NULL,
    selection   TEXT    NOT NULL,
    odd         REAL    NOT NULL,
    match_name  TEXT,
    competition TEXT,
    match_date  TEXT,
    is_live     INTEGER DEFAULT 0
);
"""
_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_match_ts ON betclic_prices(match_id, ts);"

_INSERT_SQL = """
INSERT INTO betclic_prices
    (ts, match_id, market, selection, odd, match_name, competition, match_date, is_live)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _sqlite_writer():
    """
    Daemon thread — consumes rows from _db_queue and batch-inserts into SQLite.
    Never blocks the async event loop.
    """
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute(_CREATE_TABLE)
    con.execute(_CREATE_INDEX)
    con.commit()

    batch: list[tuple] = []
    last_flush = time.time()

    while True:
        # Try to pull an item (block up to 1 s so we can flush on timeout)
        try:
            item = _db_queue.get(timeout=1.0)
            if item is None:       # shutdown sentinel
                break
            batch.append(item)
        except _queue.Empty:
            pass

        # Flush when batch is large enough OR enough time has passed
        if batch and (len(batch) >= 100 or time.time() - last_flush >= 2.0):
            try:
                con.executemany(_INSERT_SQL, batch)
                con.commit()
                batch.clear()
                last_flush = time.time()
            except Exception as exc:
                print(f"[betclic_backend] db write error: {exc}", file=sys.stderr)

    # Final flush on shutdown
    if batch:
        try:
            con.executemany(_INSERT_SQL, batch)
            con.commit()
        except Exception:
            pass
    con.close()
