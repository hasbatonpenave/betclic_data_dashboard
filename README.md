# Betclic Data Dashboard

Real-time football odds monitoring platform for [Betclic](https://www.betclic.fr).  
Scrapes the internal Betclic gRPC-web API, streams price updates via SSE, persists them in SQLite, and renders three browser-based dashboards.

---

## Architecture

```
betclic3.py          — low-level gRPC-web client (protobuf encode/decode)
    ↓
betclic_feed.py      — async feed manager (one Task per match × market)
    ↓ asyncio.Queue
betclic_backend.py   — FastAPI server (SSE fan-out + REST + SQLite writer)
    ↓ SSE / REST
betclic_dashboard.html   — live odds table (all active matches)
chart.html               — price history chart (per selection)
ws_stream.html           — raw event log (debug view)
```

No external broker, no WebSocket — the transport layer is HTTP/1.1 SSE from the FastAPI backend to the browsers, and gRPC-web (plain HTTP POST with streaming response) from the feed to Betclic's servers.

---

## Files

| File | Role |
|---|---|
| `betclic3.py` | gRPC-web primitives: protobuf encoder/decoder, `payload_get_match()`, `build_clean_json()`, `cmd_list_matches()`, `extract_live_state()` |
| `betclic_feed.py` | Async feed: spawns one `asyncio.Task` per match × market, handles reconnect, pushes updates to a shared `asyncio.Queue` |
| `betclic_backend.py` | FastAPI app on port **5003**: consumes the queue, fans out to SSE clients, batches inserts into SQLite |
| `betclic_dashboard.html` | Main dashboard — live 1X2 odds table, auto-refreshes on SSE events |
| `chart.html` | Price history chart — pulls from `/history`, uses Chart.js with time axis |
| `ws_stream.html` | Raw stream viewer — prints every SSE event as JSON for debugging |

---

## Requirements

```
Python >= 3.11
fastapi
uvicorn[standard]
aiohttp
requests
```

Install:

```bash
pip install fastapi "uvicorn[standard]" aiohttp requests
```

---

## Quick start

```bash
python betclic_backend.py
```

The server starts on `http://localhost:5003`.  
Open one of the HTML files in a browser (they connect to `localhost:5003` directly).

---

## API endpoints

All endpoints are served by FastAPI on port `5003`.

### `GET /stream`

Server-Sent Events stream. Connect once and receive all price updates in real time.

Each event is a JSON object with `type: "price"` or `type: "snapshot"`.

**On connect:** a `snapshot` event is sent immediately with the full current in-memory odds state.

**Subsequent events:**

```json
{
  "type":     "price",
  "match_id": "1045033759662080",
  "market":   "1X2",
  "odds":     { "1": 2.10, "Nul": 3.20, "2": 3.40 },
  "meta": {
    "match":       "Atalanta vs Bayern",
    "competition": "Champions League",
    "date":        "2026-04-15T20:45:00Z",
    "live":        true,
    "score_home":  1,
    "score_away":  0,
    "period":      "2nd Half",
    "minute":      63
  },
  "ts": 1713220800.123
}
```

Keepalive comments (`: keepalive`) are sent every 25 seconds to prevent proxy timeouts.

---

### `GET /prices`

Returns the current in-memory odds snapshot for all active matches.

```json
{
  "1045033759662080": {
    "1X2": { "1": 2.10, "Nul": 3.20, "2": 3.40 },
    "O/U": { "Plus de 2.5": 1.85, "Moins de 2.5": 1.95 }
  }
}
```

---

### `GET /markets`

Returns metadata for all active matches (those present in `/prices`).

```json
{
  "1045033759662080": {
    "match":       "Atalanta vs Bayern",
    "competition": "Champions League",
    "date":        "2026-04-15T20:45:00Z",
    "live":        true,
    "teams":       ["Atalanta", "Bayern Munich"]
  }
}
```

---

### `GET /status`

Feed health and stats.

```json
{
  "streams":        124,
  "updates":        48302,
  "last_update":    1713220800.0,
  "matches":        62,
  "sse_clients":    3,
  "prices_in_memory": 61
}
```

---

### `GET /history`

Price history for a specific selection, read from SQLite.

**Query parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `match_id` | ✓ | — | Betclic match ID |
| `selection` | ✓ | — | Selection name: `1`, `Nul`, `2`, `Plus de 2.5`, … |
| `market` | | `1X2` | `1X2` or `O/U` |
| `limit` | | `500` | Max rows returned (1–5000) |

**Response:**

```json
[
  { "ts": 1713218000.0, "odd": 2.15 },
  { "ts": 1713219000.0, "odd": 2.10 },
  { "ts": 1713220800.0, "odd": 1.95 }
]
```

Results are ordered oldest → newest.

---

## Dashboards

### `betclic_dashboard.html` — Live Odds Table

Connects to `/stream` via SSE. Displays all active matches in a sortable table with:

- Live/upcoming indicator
- Score and match time for live matches  
- 1X2 odds with colour-coded movement (green = drifted, red = shortened)
- Filterable by competition or team name

### `chart.html` — Price History Chart

Pulls historical data from `/history` via REST. Select a match and selection from the sidebar to plot an odds timeline. Uses Chart.js 4 with a time-based X axis. Auto-refreshes by polling `/stream` for new ticks on the selected selection.

### `ws_stream.html` — Raw Stream Debugger

Minimal view that logs every SSE event as formatted JSON. Useful for inspecting what the feed is actually pushing without the dashboard abstractions.

---

## Data persistence

Price updates are written to a SQLite database (`betclic_prices.db` in the working directory) by a dedicated daemon thread (`_sqlite_writer`). The thread consumes rows from a thread-safe queue and batch-inserts every **2 seconds or 100 rows**, whichever comes first. This keeps all disk I/O entirely off the async event loop.

**Schema:**

```sql
CREATE TABLE betclic_prices (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,       -- Unix timestamp
    match_id    TEXT    NOT NULL,
    market      TEXT    NOT NULL,       -- "1X2" | "O/U"
    selection   TEXT    NOT NULL,       -- "1" | "Nul" | "2" | ...
    odd         REAL    NOT NULL,
    match_name  TEXT,
    competition TEXT,
    match_date  TEXT,
    is_live     INTEGER DEFAULT 0       -- 0 | 1
);

CREATE INDEX idx_match_ts ON betclic_prices(match_id, ts);
```

---

## Feed internals

### `betclic3.py` — gRPC-web client

Betclic's odds API is a gRPC-web service at `offering.begmedia.com`. The client hand-encodes protobuf without a generated stub:

- **`payload_get_match(match_id, locale, market_code)`** — builds the binary gRPC frame for `GetMatchWithNotification`
- **`build_clean_json(frame)`** — decodes a response frame into a structured dict with `markets → selections → odds`
- **`cmd_list_matches(output_file)`** — fetches the full match list synchronously and writes JSON to a temp file
- **`extract_live_state(frame)`** — extracts `is_live`, `score_home`, `score_away`, `period`, `minute` from raw frame bytes

The frame format is standard gRPC-web: `[1-byte flags][4-byte big-endian length][protobuf body]`. Trailer frames (flag `0x80`) signal end-of-stream.

### `betclic_feed.py` — async stream manager

- Fetches the match list every **5 minutes** via `run_in_executor` (blocking call offloaded to a thread)
- Filters to matches within the next **48 hours** and all live matches
- Spawns one `asyncio.Task` per `(match_id, market_code)` pair — up to ~1200 concurrent tasks for 600 matches × 2 markets
- All tasks share a single `aiohttp.ClientSession` with unlimited connection pool
- On stream error, each task reconnects independently after **5 seconds**
- Markets tracked: `ca_ftb_rslt` (1X2) and `ca_ftb_goa` (Over/Under)

1X2 odds extraction uses a two-pass strategy:
1. Look for a market named "résultat du match (tps rég" or "résultat du match" — excluding extra-time, penalties, and half-time variants
2. Fallback to the first market with 2–3 selections not in the skip list

---

## Configuration

Constants at the top of each file:

**`betclic_backend.py`**

| Constant | Default | Description |
|---|---|---|
| `PORT` | `5003` | HTTP port |
| `DB_PATH` | `betclic_prices.db` | SQLite database path |

**`betclic_feed.py`**

| Constant | Default | Description |
|---|---|---|
| `LOCALE` | `fr` | Locale sent to the Betclic API |
| `RECONNECT_DELAY` | `5` s | Delay before reconnecting a dead stream |
| `MAX_MATCH_AGE_H` | `48` h | Match horizon filter |
| `MARKETS` | `[ca_ftb_rslt, ca_ftb_goa]` | Markets to stream |

`MAX_MATCH_AGE_H` can be changed at runtime via `betclic_feed.set_max_age(hours)`.

---

## Limitations

- The Betclic gRPC-web API is undocumented and reverse-engineered. It may break without notice on any Betclic backend update.
- No authentication is implemented — the feed relies on public (unauthenticated) endpoints.
- SQLite is single-writer; under very high update rates (>100 rows/s sustained) the 2-second batch window may cause brief lag, but this is unlikely in practice.
- The HTML dashboards connect to `localhost:5003` and are not configured for remote hosting out of the box.
