import time, tempfile, os
import pytest
from api.models import OddsUpdate, MatchMeta, PricePoint
from storage.sqlite import SQLiteRepository


@pytest.fixture
def repo():
    tmp_path = tempfile.mktemp(suffix=".db")
    r = SQLiteRepository(tmp_path)
    r.start()
    yield r
    r.stop()
    r.join(timeout=5)
    try:
        os.unlink(tmp_path)
    except OSError:
        pass


def make_update(match_id="m1", odds=None, market="1X2") -> OddsUpdate:
    return OddsUpdate(
        match_id=match_id,
        market=market,
        odds=odds or {"1": 2.10, "Nul": 3.20, "2": 3.40},
        meta=MatchMeta(match="Test vs Test", live=True),
        ts=time.time(),
    )


def test_enqueue_and_retrieve(repo):
    repo.enqueue(make_update())
    time.sleep(3)

    history = repo.get_history("m1", "1", "1X2", 10)
    assert len(history) == 1
    assert abs(history[0].odd - 2.10) < 0.001


def test_multiple_selections_stored(repo):
    repo.enqueue(make_update())
    time.sleep(3)

    for sel, expected_odd in [("1", 2.10), ("Nul", 3.20), ("2", 3.40)]:
        history = repo.get_history("m1", sel, "1X2", 10)
        assert len(history) == 1, f"Missing history for {sel}"
        assert abs(history[0].odd - expected_odd) < 0.001


def test_history_ordered_oldest_first(repo):
    t1 = time.time()
    time.sleep(0.01)
    t2 = time.time()

    u1 = OddsUpdate(match_id="m2", market="1X2", odds={"1": 2.00},
                    meta=MatchMeta(), ts=t1)
    u2 = OddsUpdate(match_id="m2", market="1X2", odds={"1": 2.10},
                    meta=MatchMeta(), ts=t2)
    repo.enqueue(u1)
    repo.enqueue(u2)
    time.sleep(3)

    history = repo.get_history("m2", "1", "1X2", 10)
    assert len(history) == 2
    assert history[0].ts < history[1].ts
    assert history[0].odd == pytest.approx(2.00)
    assert history[1].odd == pytest.approx(2.10)


def test_empty_history_returns_empty_list(repo):
    result = repo.get_history("nonexistent", "1", "1X2", 10)
    assert result == []


def test_limit_respected(repo):
    for i in range(20):
        u = OddsUpdate(match_id="m3", market="1X2",
                       odds={"1": 2.00 + i * 0.01},
                       meta=MatchMeta(), ts=time.time() + i)
        repo.enqueue(u)
    time.sleep(3)

    history = repo.get_history("m3", "1", "1X2", 5)
    assert len(history) == 5
