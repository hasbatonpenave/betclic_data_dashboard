import time
from feed.stream import CircuitBreaker
from config import settings


def test_initially_closed():
    cb = CircuitBreaker()
    assert not cb.is_open


def test_opens_after_max_failures():
    cb = CircuitBreaker()
    for _ in range(settings.cb_max_failures - 1):
        cb.next_delay()
        assert not cb.is_open
    cb.next_delay()
    assert cb.is_open


def test_exponential_backoff():
    cb = CircuitBreaker()
    delays = [cb.next_delay() for _ in range(settings.cb_max_failures - 1)]
    for i in range(1, len(delays)):
        assert delays[i] >= delays[i - 1]


def test_success_resets_failures():
    cb = CircuitBreaker()
    cb.next_delay()
    cb.next_delay()
    cb.record_success()
    assert not cb.is_open
    assert cb._failures == 0


def test_circuit_resets_after_park_period():
    cb = CircuitBreaker()
    for _ in range(settings.cb_max_failures):
        cb.next_delay()
    assert cb.is_open

    cb._open_until = time.monotonic() - 1.0
    assert not cb.is_open
    assert cb._failures == 0
