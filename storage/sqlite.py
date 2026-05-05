"""
storage/sqlite.py — SQLite implementation of PriceRepository.

Identical batch-writer logic to betclic_backend.py, now behind a clean interface.
All disk I/O is isolated in a daemon thread — the async event loop never blocks.
"""
from __future__ import annotations
import logging
import queue as _queue
import sqlite3
import threading
import time

from api.models import OddsUpdate, PricePoint
from config import settings
from storage.repository import PriceRepository

log = logging.getLogger(__name__)

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

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_match_ts
ON betclic_prices(match_id, ts);
"""

_INSERT_SQL = """
INSERT INTO betclic_prices
    (ts, match_id, market, selection, odd, match_name, competition, match_date, is_live)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_SELECT_HISTORY = """
SELECT ts, odd
FROM   betclic_prices
WHERE  match_id  = ?
  AND  market    = ?
  AND  selection = ?
ORDER  BY ts DESC
LIMIT  ?
"""


class SQLiteRepository(PriceRepository):

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path or settings.db_path
        self._queue:  _queue.Queue = _queue.Queue()
        self._thread: threading.Thread | None = None

    # ── PriceRepository interface ─────────────────────────────────────────────

    def enqueue(self, update: OddsUpdate) -> None:
        """Enqueue all selections from an OddsUpdate for batch insert."""
        for selection, odd in update.odds.items():
            self._queue.put_nowait((
                update.ts,
                update.match_id,
                update.market,
                selection,
                odd,
                update.meta.match,
                update.meta.competition,
                update.meta.date,
                int(update.meta.live),
            ))

    def get_history(
        self,
        match_id:  str,
        selection: str,
        market:    str,
        limit:     int,
    ) -> list[PricePoint]:
        con = sqlite3.connect(self._db_path, check_same_thread=True)
        try:
            con.execute(_CREATE_TABLE)
            con.execute(_CREATE_INDEX)
            rows = con.execute(
                _SELECT_HISTORY, (match_id, market, selection, limit)
            ).fetchall()
            return [PricePoint(ts=r[0], odd=r[1]) for r in reversed(rows)]
        finally:
            con.close()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._writer_loop,
            daemon=True,
            name="betclic-db-writer",
        )
        self._thread.start()
        log.info("SQLite writer thread started (db=%s)", self._db_path)

    def stop(self) -> None:
        self._queue.put(None)  # sentinel

    def join(self, timeout: float = 10.0) -> None:
        if self._thread:
            self._thread.join(timeout=timeout)

    # ── Background writer ─────────────────────────────────────────────────────

    def _writer_loop(self) -> None:
        con = sqlite3.connect(self._db_path, check_same_thread=False)
        con.execute(_CREATE_TABLE)
        con.execute(_CREATE_INDEX)
        con.commit()

        batch: list[tuple] = []
        last_flush = time.time()

        while True:
            try:
                item = self._queue.get(timeout=1.0)
                if item is None:  # shutdown sentinel
                    break
                batch.append(item)
            except _queue.Empty:
                pass

            if batch and (len(batch) >= 100 or time.time() - last_flush >= 2.0):
                try:
                    con.executemany(_INSERT_SQL, batch)
                    con.commit()
                    batch.clear()
                    last_flush = time.time()
                except Exception as exc:
                    log.error("db write error: %s", exc)

        # Final flush
        if batch:
            try:
                con.executemany(_INSERT_SQL, batch)
                con.commit()
            except Exception as exc:
                log.error("db final flush error: %s", exc)

        con.close()
        log.info("SQLite writer thread exited cleanly")
