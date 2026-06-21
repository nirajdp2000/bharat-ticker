# 🚀 Start Bharat Ticker

Real-time + historical NSE/BSE market data engine with a web dashboard.
**Boots with zero infrastructure** — no Redis, no database, no Docker required.

---

## One command

```bash
python run.py
```

That's it. On Windows you can also just **double-click `start.bat`**.

`run.py` does everything:
1. Installs Python dependencies the first time (~1–2 min, once).
2. Starts the API server (in-process cache fallback — no Redis/DB needed).
3. Opens the dashboard at **http://127.0.0.1:8000/ui** when ready.

**Prerequisite:** Python 3.12+ installed and on `PATH`.
Check with `python --version`.

---

## What you get

| URL | What |
|:---|:---|
| http://127.0.0.1:8000/ui | **Dashboard** — quotes, source toggle, full details, history, health |
| http://127.0.0.1:8000/all | **All stocks** — every NSE/BSE symbol in one table (search/sort/gainers) |
| http://127.0.0.1:8000/docs | Interactive API (Swagger) |
| http://127.0.0.1:8000/health | System health |

### Data sources (failover order)
1. **`nse_live`** — live NSE quotes via the Moneycontrol relay (real exchange
   data, no broker account, no proxy, not Akamai-blocked).
2. **`bse_scraper` / `bse_live`** — live BSE (direct BSE JSON API + MC backup).
3. **`nse_archive` / `bse_archive`** — official bhavcopy, last-traded-day,
   unbreakable fallback.
4. **`yahoo`** — **historical only** (15-min delayed; never used for live quotes).

If one source breaks, the next takes over. Live quotes never fall back to Yahoo.

If one source breaks, the next takes over automatically. You can also
connect/disconnect or live-test each source from the dashboard.

---

## Using the dashboard

- **Search a symbol** (e.g. `RELIANCE`, `TCS`, `APOLLOTYRE`) → live quote +
  full details (OHLC, VWAP, 52-week, price band, TTQ, turnover, Mcap, delivery,
  sector/industry, EPS/PE/PB/ROE) + history chart.
- **Source pills** (AUTO / NSE / BSE / Yahoo) — pin the feed to one source.
  `AUTO` uses live NSE/BSE only (never Yahoo).
- **⚡ Test all connections** — verifies each source works (green ✓ / red ✗).
- **connect / disconnect** per source — runtime enable/disable.
- **auto-refresh (3s)** — live ticking.

---

## How to check the JSON data

Every value is exposed as JSON. Four ways to view it:

1. **Swagger UI (easiest)** — open **http://127.0.0.1:8000/docs**, pick an
   endpoint, click *Try it out* → *Execute*. Shows the live JSON response.
2. **Browser** — just open the URL, e.g.
   http://127.0.0.1:8000/api/v1/quote/RELIANCE/details?source=bse
   (use a JSON-formatter browser extension for pretty output).
3. **curl** — `curl "http://127.0.0.1:8000/api/v1/quote/RELIANCE/analytics"`
4. **curl + jq** (pretty + filter) —
   `curl -s ".../quote/RELIANCE/details?source=bse" | jq .price`

### Full data catalogue (all JSON)

| Data | Endpoint | Live without proxy? |
|:---|:---|:---|
| Quote (LTP/OHLC/change) | `/api/v1/quote/{sym}?source=auto\|nse\|bse` | ✅ BSE live / NSE archive |
| Full details (VWAP, 52wk, band, TTQ, turnover, Mcap, sector, EPS/PE/PB/ROE, depth) | `/api/v1/quote/{sym}/details` | ✅ (BSE) |
| Returns 1W/1M/YTD/1Y/3Y/5Y + volatility | `/api/v1/quote/{sym}/analytics` | ✅ computed |
| Market depth (5-level) | `/api/v1/quote/{sym}/depth` | ✅ BSE (live hrs) |
| **ALL stocks, one call** (~0ms read) | `/api/v1/snapshot?exchange=NSE` | ✅ (EOD snapshot) |
| NSE indices (139) | `/api/v1/indices` | ✅ |
| Historical OHLC | `/api/v1/historical/{sym}` | ✅ |
| **NSE full quote** (trade/price/securities info) | `/api/v1/quote/{sym}/nse` | ⚠ needs `PROXY_LIST` |
| **NSE option chain (F&O)** | `/api/v1/quote/{sym}/options` | ⚠ needs `PROXY_LIST` |

> NSE's live quote-equity, option-chain and F&O APIs are Akamai bot-gated.
> They return real JSON once `PROXY_LIST` (residential Indian proxy) is set;
> without it they return `{"available": false, "requires_proxy": true}` — never
> fake data.

## Key API endpoints

```bash
# Live quote (auto = NSE/BSE live, never Yahoo)
curl "http://127.0.0.1:8000/api/v1/quote/RELIANCE"

# Force a source: auto | nse | bse | yahoo
curl "http://127.0.0.1:8000/api/v1/quote/RELIANCE?source=bse"

# Full details panel (fundamentals + stats)
curl "http://127.0.0.1:8000/api/v1/quote/APOLLOTYRE/details?source=bse"

# Historical candles (Yahoo / NSE-BSE archive backfill)
curl "http://127.0.0.1:8000/api/v1/historical/TCS?interval=1d&limit=90"

# Test a source connection
curl "http://127.0.0.1:8000/api/v1/sources/bse/test"

# List / toggle sources
curl  "http://127.0.0.1:8000/api/v1/sources"
curl -X POST "http://127.0.0.1:8000/api/v1/sources/yahoo/disable"
```

---

## All stocks at once / "0 latency"

`GET /api/v1/snapshot?exchange=NSE` returns **every** symbol (3000+ NSE, 4000+
BSE) in a single request, served from memory. The data lookup is ~0ms; the
~400ms you see is just serializing the multi-thousand-row JSON.

**Important — what "0-latency live for all stocks" really requires:**
Web-scraping cannot fetch thousands of symbols live simultaneously (one HTTP
call per symbol; the exchange rate-limits). Two real options:

1. **Broker WebSocket** (Fyers / Angel One / Upstox) — the exchange *pushes*
   ticks for all subscribed symbols. This is the only true tick-by-tick,
   all-stocks, zero-latency live feed. Needs a broker account + API key
   (scaffolded in `.env`: `FYERS_*`, `ANGEL_*`).
2. **Pre-warmed cache** (this app) — a background loop scrapes a universe and
   caches it; API reads are ~0ms. Bulk `/snapshot` is the last-traded-day
   snapshot; the live-polled subset is fresher. Freshness of *all* 4000 is
   bounded by how fast the scraper can cycle them.

So: instant **reads** for all stocks ✅ today; instant **live ticks** for all
stocks → enable a broker WebSocket.

## Enable live NSE intraday (optional)

NSE's realtime `quote-equity` API is gated by Akamai bot protection and is
blocked from datacenter/automated contexts. To unlock it, set a residential
Indian proxy in `.env` — the live NSE scraper then activates automatically:

```env
PROXY_LIST=http://user:pass@host:port
```

Without a proxy, NSE is still served from its official archive (last-traded-day,
full 3000+ symbol universe). BSE live works without a proxy.

---

## Production scale (optional)

For multi-process caching/persistence, run your own Redis + TimescaleDB and set
`REDIS_URL` / `DATABASE_URL` in `.env`, then `python run.py`. The app uses them
when reachable and falls back to the in-process store otherwise (single-machine
default).

---

## Troubleshooting

| Symptom | Fix |
|:---|:---|
| `python` not found | Install Python 3.12+, add to PATH |
| Port 8000 busy | `set API_PORT=8001 && python run.py` |
| Quote returns 404 | Source temporarily down — try `?source=bse` or another |
| NSE source shows DOWN | Expected without a proxy (see "Enable live NSE") — archive still serves NSE data |
| First start slow | One-time dependency install + bhavcopy load (~10s warm-up) |
