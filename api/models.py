"""
api/models.py — Pydantic data contracts.

These are the canonical types that cross layer boundaries.
No I/O. No business logic. Import freely from any module.
"""
from __future__ import annotations
from pydantic import BaseModel, Field, field_validator


# ── Wire-level types (proto layer output) ─────────────────────────────────────

class Selection(BaseModel):
    name: str | None = None
    odd: float | None = None
    team: str | None = None


class Market(BaseModel):
    name: str
    selections: list[Selection] = []
    sub_markets: list[Market] = []


class OddsFrame(BaseModel):
    """
    Decoded output of build_clean_json().
    Raw proto data validated into a typed shape before business logic touches it.
    """
    match_id: int | None = None
    match: str | None = None
    date: str | None = None
    competition: str | None = None
    teams: list[dict] = []
    markets: list[Market] = []

    @field_validator("markets", mode="before")
    @classmethod
    def coerce_markets(cls, v):
        if v and isinstance(v[0], dict):
            return [Market(**m) for m in v]
        return v


# ── Live state ────────────────────────────────────────────────────────────────

class LiveState(BaseModel):
    is_live: bool = False
    score_home: int | None = None
    score_away: int | None = None
    period: str | None = None
    minute: int | None = None


# ── Match metadata (cached in feed, embedded in updates) ──────────────────────

class MatchMeta(BaseModel):
    match: str = ""
    competition: str | None = None
    date: str | None = None
    live: bool = False
    teams: list[str] = []
    score_home: int | None = None
    score_away: int | None = None
    period: str | None = None
    minute: int | None = None

    def update_live(self, state: LiveState) -> MatchMeta:
        """Return a new MatchMeta with live state fields replaced."""
        return self.model_copy(update={
            "live":       state.is_live,
            "score_home": state.score_home,
            "score_away": state.score_away,
            "period":     state.period,
            "minute":     state.minute,
        })


# ── Queue payload (feed → backend) ───────────────────────────────────────────

class OddsUpdate(BaseModel):
    """
    Typed payload pushed to asyncio.Queue by the feed and consumed by the backend.
    Replaces the raw dict that previously crossed this boundary.
    """
    source: str = "betclic"
    match_id: str
    market: str                    # "1X2" | "O/U"
    odds: dict[str, float]         # {"1": 2.10, "Nul": 3.20, "2": 3.40}
    meta: MatchMeta
    ts: float

    @field_validator("odds")
    @classmethod
    def odds_must_be_valid(cls, v):
        for name, odd in v.items():
            if not (1.001 <= odd <= 1000.0):
                raise ValueError(f"odd {name}={odd} out of sane range")
        return v


# ── Storage types ─────────────────────────────────────────────────────────────

class PricePoint(BaseModel):
    ts: float
    odd: float


# ── SSE wire types ────────────────────────────────────────────────────────────

class SSEPriceEvent(BaseModel):
    type: str = "price"
    match_id: str
    market: str
    odds: dict[str, float]
    meta: MatchMeta
    ts: float


class SSESnapshot(BaseModel):
    type: str = "snapshot"
    prices: dict   # {match_id: {market: {selection: odd}}}
    ts: float
