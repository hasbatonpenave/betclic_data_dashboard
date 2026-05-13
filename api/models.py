"""
api/models.py — Canonical data contracts.

No I/O. No business logic. Import freely from any module.
These types cross all layer boundaries (feed → server → storage).
"""

from __future__ import annotations
import time
from pydantic import BaseModel


class MatchMeta(BaseModel):
    match_id: str = ""
    name: str = ""
    competition: str | None = None
    date: str | None = None
    live: bool = False
    teams: list[str] = []
    score_home: int | None = None
    score_away: int | None = None
    period: str | None = None
    minute: int | None = None

    def update_live(self, state: LiveState) -> MatchMeta:
        return self.model_copy(update={
            "live":       state.is_live,
            "score_home": state.score_home,
            "score_away": state.score_away,
            "period":     state.period,
            "minute":     state.minute,
        })


class LiveState(BaseModel):
    is_live: bool = False
    score_home: int | None = None
    score_away: int | None = None
    period: str | None = None
    minute: int | None = None


class OddsUpdate(BaseModel):
    """Payload pushed to asyncio.Queue by the feed, consumed by server/app.py."""
    source: str = "betclic"
    match_id: str
    market: str                     # "1X2" | "O/U"
    odds: dict[str, float]          # {"1": 2.10, "Nul": 3.20, "2": 3.40}
    meta: dict = {}
    live: bool = False
    score: str | None = None
    period: str | None = None
    minute: int | None = None
    ts: float = 0.0

    def model_post_init(self, __context) -> None:
        if self.ts == 0.0:
            self.ts = time.time()


class PricePoint(BaseModel):
    ts: float
    odd: float


class SSEPriceEvent(BaseModel):
    type: str = "price"
    match_id: str
    market: str
    odds: dict[str, float]
    meta: dict
    ts: float


class SSESnapshot(BaseModel):
    type: str = "snapshot"
    prices: dict    # {match_id: {market: {selection: odd}}}
    ts: float
