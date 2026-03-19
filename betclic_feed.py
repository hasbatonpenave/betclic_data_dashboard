"""
betclic_feed.py — Betclic gRPC price feed  (fully async, aiohttp)
==================================================================

Architecture change from v1:
  BEFORE: ThreadPoolExecutor(max_workers=60)
          → 60 blocking threads, 1140 matches queued and never started

  NOW:    One asyncio.Task per match × market
          → 600+ concurrent streams sharing one event loop, zero thread overhead
          → All I/O is non-blocking; the event loop switches tasks on every
            await so one slow stream never delays another.

Public interface (unchanged — betclic_backend.py works as-is):
  await run(queue)
  stop()
  get_stats()  → dict
  get_meta(mid) → dict
  get_all_meta() → dict
  set_max_age(h) / get_max_age()

Update format pushed to queue (unchanged):
{
    "source":   "betclic",
    "match_id": str,
    "market":   "1X2" | "O/U",
    "odds":     {"1": 2.10, "Nul": 3.20, "2": 3.40},
    "meta":     {"match": str, "competition": str, "date": str, "live": bool}
}
"""

import asyncio, json, time, struct, os, sys, tempfile
from datetime import datetime, timezone

try:
    import aiohttp
except ImportError:
    print("pip install aiohttp", file=sys.stderr); sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from betclic3 import (
    payload_get_match, build_clean_json, cmd_list_matches,
    BASE, HEADERS, extract_live_state,
)
except ImportError as e:
    print(f"betclic_feed: cannot import betclic3: {e}", file=sys.stderr); sys.exit(1)

# ── CONFIG ────────────────────────────────────────────────────────────────────

LOCALE          = "fr"
RECONNECT_DELAY = 5          # seconds before reconnecting a dead stream
MAX_MATCH_AGE_H = 48         # stream matches starting within this window

MARKETS = [
    ("ca_ftb_rslt", "1X2"),
    ("ca_ftb_goa",  "O/U"),
]

# Market names to skip when searching for the main 1X2 market
_SKIP = [
    "qualification", "remboursé", "rembourse",
    "prolongation", "tirs au but", "chaque mi-temps",
    "gagne au moins", "1ère mi-temps", "2ème mi-temps",
    "mi-temps / fin",
]

# ── SHARED STATE ──────────────────────────────────────────────────────────────
# Plain dicts — CPython GIL makes simple reads/writes atomic, and we only
# write from the single event loop thread anyway.

_meta: dict = {}    # {match_id_str: {match, competition, date, live, teams}}

stats: dict = {
    "streams":     0,
    "updates":     0,
    "last_update": None,
    "matches":     0,
}

_stop_event: asyncio.Event | None = None   # set by stop()

# ── PUBLIC CONFIG HELPERS ─────────────────────────────────────────────────────

def set_max_age(hours: float) -> None:
    global MAX_MATCH_AGE_H
    MAX_MATCH_AGE_H = hours

def get_max_age() -> float:
    return MAX_MATCH_AGE_H

# ── MATCH RELEVANCE FILTER ────────────────────────────────────────────────────

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
        return -3600 <= diff <= MAX_MATCH_AGE_H * 3600
    except Exception:
        return True

# ── MATCH LIST ────────────────────────────────────────────────────────────────

def _fetch_match_list_sync() -> list[dict]:
    """
    Blocking fetch of all football matches.
    Intentionally synchronous — called via run_in_executor so it runs in a
    thread and does NOT block the event loop.
    """
    tmp = tempfile.mktemp(suffix=".json")
    try:
        cmd_list_matches(output_file=tmp)
        with open(tmp, encoding="utf-8") as f:
            data = json.load(f)

        matches = data.get("matches", data) if isinstance(data, dict) else data

        # Update meta cache (read by get_all_meta / get_meta)
        for m in matches:
            mid = str(m.get("match_id", ""))
            if mid:
                _meta[mid] = {
                    "match":       m.get("match", ""),
                    "competition": m.get("competition", ""),
                    "date":        m.get("date", ""),
                    "live":        m.get("live", False),
                    "teams":       [t.get("name", "") for t in m.get("teams", [])],
                }

        relevant = [m for m in matches if _is_relevant(m)]
        print(
            f"  [betclic_feed] {len(matches)} total, "
            f"{len(relevant)} relevant (≤{MAX_MATCH_AGE_H}h)",
            file=sys.stderr,
        )
        return relevant

    except Exception as e:
        print(f"  [betclic_feed] fetch_match_list error: {e}", file=sys.stderr)
        return []
    finally:
        try: os.unlink(tmp)
        except: pass


async def _fetch_match_list() -> list[dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_match_list_sync)

# ── ODDS EXTRACTOR ────────────────────────────────────────────────────────────

def _extract_1x2_odds(result: dict) -> dict | None:
    """
    Two-pass extraction of the main 1X2 market odds from a build_clean_json result.
    Identical logic to the original betclic_feed.py, extracted for reuse.
    Returns {selection_name: odd} or None.
    """
    all_markets = result.get("markets", [])

    # Pass 1: prefer the full-time regulation market by name
    for priority in ["résultat du match (tps rég", "résultat du match"]:
        for mkt in all_markets:
            mname = mkt.get("name", "").lower()
            if priority in mname and not any(s in mname for s in _SKIP):
                odds = {
                    s["name"]: s["odd"]
                    for s in mkt.get("selections", [])
                    if s.get("odd") and s.get("name") and not s["name"].startswith("_")
                }
                if 2 <= len(odds) <= 3:
                    return odds

    # Pass 2: fallback — first market with 2-3 selections not in skip list
    for mkt in all_markets:
        mname = mkt.get("name", "").lower()
        if any(s in mname for s in _SKIP):
            continue
        odds = {
            s["name"]: s["odd"]
            for s in mkt.get("selections", [])
            if s.get("odd") and s.get("name") and not s["name"].startswith("_")
        }
        if 2 <= len(odds) <= 3:
            return odds

    return None

# ── HELPERS ───────────────────────────────────────────────────────────────────

def _fmt_score(meta: dict) -> str | None:
    """Return 'H-A' score string if both values are known, else None."""
    h = meta.get("score_home")
    a = meta.get("score_away")
    if h is not None and a is not None:
        return f"{h}-{a}"
    return None


# ── SINGLE ASYNC STREAM ───────────────────────────────────────────────────────

async def _stream_match(
    session:     aiohttp.ClientSession,
    match_id:    int,
    market_code: str,
    market_name: str,
    queue:       asyncio.Queue,
    stop_ev:     asyncio.Event,
) -> None:
    """
    One long-lived coroutine per match × market.

    The Betclic server keeps the HTTP connection open and pushes a new gRPC
    frame whenever odds change — this is server-push over HTTP/1.1, not polling.

    On any connection error the coroutine sleeps RECONNECT_DELAY seconds then
    reconnects automatically, unless stop_ev has been set.
    """
    mid_str  = str(match_id)
    url      = f"{BASE}/GetMatchWithNotification"
    payload  = payload_get_match(match_id, LOCALE, market_code)

    stats["streams"] += 1
    try:
        while not stop_ev.is_set():
            buf = b""
            try:
                timeout = aiohttp.ClientTimeout(
                    connect=10,
                    total=None,       # no total timeout — stream lives indefinitely
                    sock_read=300,    # 5 min read timeout — server sends keepalives
                )
                async with session.post(url, data=payload, timeout=timeout) as resp:
                    resp.raise_for_status()

                    # iter_any() yields whatever bytes are available from the socket,
                    # which is exactly what we want — no artificial chunking delay.
                    async for chunk in resp.content.iter_any():
                        if stop_ev.is_set():
                            return

                        buf += chunk

                        # Consume all complete gRPC frames sitting in the buffer
                        while len(buf) >= 5:
                            flags  = buf[0]
                            length = struct.unpack(">I", buf[1:5])[0]

                            if len(buf) < 5 + length:
                                break  # incomplete frame — wait for more data

                            frame = buf[5 : 5 + length]
                            buf   = buf[5 + length:]

                            if flags & 0x80:
                                # Trailer frame — server signalled end-of-stream
                                buf = b""
                                break

                            # ── Parse frame ───────────────────────────────────
                            try:
                                result = build_clean_json(frame)
                            except Exception:
                                continue

                            # ── Live state (authoritative — from stream frame) ─
                            # This overwrites whatever the listing said.
                            # extract_live_state reads field 6 + scans for
                            # score / period / minute in unmapped sub-messages.
                            try:
                                live_state = extract_live_state(frame)
                                existing   = _meta.get(mid_str, {})
                                existing.update({
                                    "live":       live_state["is_live"],
                                    "score_home": live_state["score_home"],
                                    "score_away": live_state["score_away"],
                                    "period":     live_state["period"],
                                    "minute":     live_state["minute"],
                                })
                                _meta[mid_str] = existing
                            except Exception:
                                pass

                            chosen_odds = _extract_1x2_odds(result)
                            if not chosen_odds:
                                continue

                            meta   = dict(_meta.get(mid_str, {}))
                            update = {
                                "source":   "betclic",
                                "match_id": mid_str,
                                "market":   market_name,
                                "odds":     chosen_odds,
                                "meta":     meta,
                                # convenience top-level fields for fast dashboard reads
                                "live":     meta.get("live", False),
                                "score":    _fmt_score(meta),
                                "period":   meta.get("period"),
                                "minute":   meta.get("minute"),
                            }

                            # put_nowait is safe here — queue.maxsize is set
                            # large (20 000) in betclic_backend; if it's full
                            # we simply drop rather than block the event loop.
                            try:
                                queue.put_nowait(update)
                            except asyncio.QueueFull:
                                pass   # backpressure — drop and continue

                            stats["updates"]     += 1
                            stats["last_update"]  = time.time()

            except asyncio.CancelledError:
                raise   # propagate — do not reconnect on cancel

            except Exception as exc:
                if stop_ev.is_set():
                    return
                # Any connection / HTTP error: wait then reconnect
                print(
                    f"  [betclic_feed] stream {mid_str}/{market_code} error "
                    f"({type(exc).__name__}): {exc} — reconnecting in {RECONNECT_DELAY}s",
                    file=sys.stderr,
                )
                try:
                    await asyncio.wait_for(stop_ev.wait(), timeout=RECONNECT_DELAY)
                    return  # stop was set during the wait
                except asyncio.TimeoutError:
                    pass    # normal — reconnect

    finally:
        stats["streams"] -= 1

# ── MAIN COROUTINE ────────────────────────────────────────────────────────────

async def run(
    queue:       asyncio.Queue,
    refresh_min: float = 5.0,
) -> None:
    """
    Main feed coroutine.

    Usage in betclic_backend.py (unchanged):
        asyncio.create_task(betclic_feed.run(queue))

    Creates one asyncio.Task per match × market (up to ~1200 tasks for 600
    matches × 2 markets).  All tasks share a single aiohttp.ClientSession
    with unlimited concurrent connections to offering.begmedia.com.

    Every refresh_min minutes the match list is re-fetched and new matches
    get a stream task spawned for them.  Finished/dead tasks are pruned.
    """
    global _stop_event
    _stop_event = asyncio.Event()

    streaming: set[tuple]               = set()   # (match_id, market_code)
    tasks:     dict[tuple, asyncio.Task] = {}

    # ── aiohttp session ───────────────────────────────────────────────────────
    # One session, one DNS cache, one connection pool — shared by all streams.
    # limit=0 / limit_per_host=0: let the OS manage connection counts.
    connector = aiohttp.TCPConnector(
        limit=0,
        limit_per_host=0,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,         # reuse connections where possible
    )

    # aiohttp wants header values as str — HEADERS from betclic3 is already that
    async with aiohttp.ClientSession(
        headers=dict(HEADERS),
        connector=connector,
        connector_owner=True,
    ) as session:

        while not _stop_event.is_set():

            # ── 1. Refresh match list ─────────────────────────────────────────
            matches = await _fetch_match_list()
            if not matches:
                print("  [betclic_feed] no matches — retry in 15s", file=sys.stderr)
                try:
                    await asyncio.wait_for(_stop_event.wait(), timeout=15)
                    break
                except asyncio.TimeoutError:
                    continue

            stats["matches"] = len(matches)

            # ── 2. Spawn tasks for new matches ────────────────────────────────
            spawned = 0
            for m in matches:
                mid = m.get("match_id")
                if not mid:
                    continue

                for mkt_code, mkt_name in MARKETS:
                    key = (mid, mkt_code)

                    # Skip if already streaming (and task is alive)
                    if key in tasks and not tasks[key].done():
                        continue

                    streaming.add(key)
                    t = asyncio.create_task(
                        _stream_match(
                            session, mid, mkt_code, mkt_name,
                            queue, _stop_event,
                        ),
                        name=f"bc-{mid}-{mkt_code}",
                    )
                    tasks[key] = t
                    spawned += 1

            # ── 3. Prune finished tasks ───────────────────────────────────────
            dead = [k for k, t in tasks.items() if t.done()]
            for k in dead:
                streaming.discard(k)
                del tasks[k]

            active = len(tasks) - len(dead)
            print(
                f"  [betclic_feed] {active} active streams "
                f"({spawned} new this cycle)",
                file=sys.stderr,
            )

            # ── 4. Wait for next refresh cycle (or stop signal) ───────────────
            try:
                await asyncio.wait_for(_stop_event.wait(), timeout=refresh_min * 60)
                break   # stop() was called
            except asyncio.TimeoutError:
                pass    # normal refresh

        # ── Graceful shutdown ─────────────────────────────────────────────────
        print(f"  [betclic_feed] shutting down {len(tasks)} tasks…", file=sys.stderr)
        for t in tasks.values():
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks.values(), return_exceptions=True)
        print("  [betclic_feed] shutdown complete", file=sys.stderr)


def stop() -> None:
    """Signal the feed to shut down gracefully."""
    if _stop_event is not None:
        _stop_event.set()


def get_stats() -> dict:
    return dict(stats)


def get_meta(match_id: str) -> dict:
    return dict(_meta.get(match_id, {}))


def get_all_meta() -> dict:
    return dict(_meta)
