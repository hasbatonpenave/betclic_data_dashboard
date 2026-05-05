import pytest
import time
from api.models import (
    OddsUpdate, MatchMeta, LiveState,
    OddsFrame, Selection, Market, PricePoint,
)


def make_update(**kwargs) -> OddsUpdate:
    defaults = dict(
        match_id="123",
        market="1X2",
        odds={"1": 2.10, "Nul": 3.20, "2": 3.40},
        meta=MatchMeta(match="Test FC vs Example United"),
        ts=time.time(),
    )
    return OddsUpdate(**(defaults | kwargs))


def test_odds_update_valid():
    u = make_update()
    assert u.match_id == "123"
    assert u.odds["1"] == 2.10
    assert u.meta.match == "Test FC vs Example United"


def test_odds_update_rejects_sub_one_odd():
    with pytest.raises(Exception, match="out of sane range"):
        make_update(odds={"1": 0.99, "2": 1.50})


def test_odds_update_rejects_absurd_odd():
    with pytest.raises(Exception, match="out of sane range"):
        make_update(odds={"1": 1001.0, "2": 1.50})


def test_match_meta_update_live():
    meta = MatchMeta(match="PSG vs Lyon", live=False)
    state = LiveState(is_live=True, score_home=2, score_away=1, period="2H", minute=67)
    updated = meta.update_live(state)

    assert updated.live is True
    assert updated.score_home == 2
    assert updated.score_away == 1
    assert updated.period == "2H"
    assert updated.minute == 67
    assert updated.match == "PSG vs Lyon"


def test_match_meta_update_live_does_not_mutate_original():
    meta    = MatchMeta(match="Test", live=False)
    state   = LiveState(is_live=True, score_home=1, score_away=0)
    updated = meta.update_live(state)

    assert meta.live is False
    assert updated.live is True


def test_price_point():
    p = PricePoint(ts=1234567890.0, odd=2.35)
    assert p.odd == 2.35


def test_odds_frame_empty():
    frame = OddsFrame()
    assert frame.match_id is None
    assert frame.markets == []


def test_odds_update_serializes_to_json():
    u = make_update()
    j = u.model_dump_json()
    assert '"match_id"' in j
    assert '"odds"' in j
