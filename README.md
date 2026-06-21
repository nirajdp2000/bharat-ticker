# 🇮🇳 Bharat Ticker — Real-Time Indian Stock Market Data Engine

> **Production-grade, self-healing, multi-source data ingestion system for NSE/BSE real-time and historical market data.**

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-green.svg)](https://fastapi.tiangolo.com/)
[![Redis](https://img.shields.io/badge/Redis-7.2+-red.svg)](https://redis.io/)
[![TimescaleDB](https://img.shields.io/badge/TimescaleDB-2.x-blue.svg)](https://www.timescale.com/)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA PROVIDERS                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐   │
│  │ NSE API  │ │ BSE API  │ │ Fyers WS │ │Yahoo Finance │   │
│  │ (Tier-1) │ │ (Tier-1) │ │ (Tier-2) │ │   (Tier-3)   │   │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────┬───────┘   │
│       │             │            │               │           │
│       └──────┬──────┘────────────┘───────────────┘           │
│              ▼                                               │
│  ┌───────────────────────────────────────────────────┐       │
│  │        FAILOVER ORCHESTRATOR + CIRCUIT BREAKERS   │       │
│  │  ┌──────────┐  ┌──────────┐  ┌────────────────┐  │       │
│  │  │ Schema   │  │ Rate     │  │ Proxy          │  │       │
│  │  │Validator │  │ Limiter  │  │ Rotator        │  │       │
│  │  └──────────┘  └──────────┘  └────────────────┘  │       │
│  └───────────────────────┬───────────────────────────┘       │
│                          ▼                                   │
│  ┌────────────────────────────────────────────────┐          │
│  │              REDIS (In-Memory)                 │          │
│  │  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │          │
│  │  │ Hash     │  │ Streams  │  │  Pub/Sub     │  │          │
│  │  │ Cache    │  │ Pipeline │  │  (WebSocket) │  │          │
│  │  └──────────┘  └─────┬────┘  └─────────────┘  │          │
│  └───────────────────────┼────────────────────────┘          │
│                          ▼                                   │
│  ┌──────────────────────────────────────┐                    │
│  │        TIMESCALEDB (Persistent)      │                    │
│  │  Ticks │ EOD │ OHLCV Aggregates      │                    │
│  └──────────────────────────────────────┘                    │
│                          ▲                                   │
│                          │                                   │
│  ┌──────────────────────────────────────┐                    │
│  │         FASTAPI (Serving Layer)      │                    │
│  │  /quote │ /historical │ /ws/ticks   │                    │
│  │  Smart Market-Hours Router (IST)     │                    │
│  └──────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────┘
```

## Features

- **Multi-Tier Provider Failover** — NSE/BSE scraping → Broker APIs → Yahoo Finance
- **TLS Fingerprint Spoofing** — `curl_cffi` with browser impersonation to bypass WAFs
- **Self-Healing** — Pydantic schema validation, circuit breakers, auto-recovery
- **Zero-Latency Reads** — Redis Hash cache with sub-millisecond access
- **Smart Market-Hours Routing** — IST-aware middleware routes to live/historical data
- **WebSocket Streaming** — Real-time tick push via Redis Pub/Sub
- **TimescaleDB Persistence** — Continuous aggregates for OHLCV candles
- **Comprehensive API** — Deeply nested JSON with market depth, indicators, metadata

## Implementation Status (honest)

This is a strong, professional ingestion design — multi-tier failover, circuit
breakers, TLS-impersonation scraping, Redis cache, TimescaleDB, market-hours
routing. It is **not** magic and there is no "zero-latency" REST feed: scraping
NSE/BSE internal JSON is inherently ~hundreds of ms per symbol. True
tick-by-tick zero-latency requires a licensed broker WebSocket (Fyers/Angel),
which is scaffolded but not enabled.

**NSE note:** NSE's realtime `/api/quote-equity` is gated by Akamai Bot Manager
and refuses automated clients (even a real headless browser) unless the request
comes from a residential context. So the **primary NSE source is NSE's own
official archive** (`nsearchives.nseindia.com` full bhavcopy — last-traded-day
OHLC, volume, VWAP and delivery for every symbol, ~3100+ names), which is NOT
bot-gated. To also pull live intraday `quote-equity`, set `PROXY_LIST` to a
residential Indian proxy — the realtime scraper then activates automatically.

| Capability | State |
|:---|:---|
| NSE data — official bhavcopy archive (3141 symbols, OHLC/VWAP/delivery) | ✅ Live, **primary** |
| NSE realtime `/api/quote-equity` (Akamai bot-walled) | ⚙️ Proxy-gated (set `PROXY_LIST`) |
| BSE realtime quote (live JSON API `api.bseindia.com`) | ✅ Live |
| BSE data — official UDiFF bhavcopy (4300+ securities) | ✅ Live |
| BSE scrip master from bhavcopy (UI-change-proof resolver) | ✅ Live |
| Source failover NSE↔BSE↔Yahoo (if one breaks, next serves) | ✅ Live |
| Per-source connect/disconnect + `?source=` forcing | ✅ Live |
| Web dashboard `/ui` (toggle sources, live quote, history) | ✅ Live |
| On-demand fetch-through for any symbol | ✅ Live (NSE → BSE → Yahoo) |
| Yahoo Tier-3 fallback | ✅ Live |
| Historical OHLCV (stored ticks → TimescaleDB) | ✅ Live |
| Historical backfill ("old data" day one) | ✅ Live (Yahoo) |
| Boot with **no** Redis/TimescaleDB/Docker | ✅ In-memory fallback |
| Broker WebSocket (Fyers/Angel) Tier-2 | ⚙️ Scaffolded, disabled (needs keys) |
| Full NSE/BSE universe (instrument master) | ⚙️ NIFTY100 built-in; full load = roadmap |
| Market depth (L2) | ✅ NSE only |

### One-command start

```bash
python run.py      # installs deps (first run), starts server, opens the dashboard
```

Windows: double-click `start.bat`. Full guide in [START.md](START.md).

### Run with zero infrastructure

No Redis or TimescaleDB? The app still boots — Redis degrades to an in-process
store and the DB layer is skipped (live data via fetch-through, history via
Yahoo):

```bash
pip install -e .
uvicorn src.main:app --port 8000
curl http://localhost:8000/api/v1/quote/RELIANCE
curl "http://localhost:8000/api/v1/historical/TCS?interval=1d"
```

Optionally point `REDIS_URL` / `DATABASE_URL` at a real Redis / TimescaleDB for
cross-process caching and persistence — the app uses them automatically when
reachable, and falls back to the in-process store when not.

## Quick Start

### Prerequisites
- Python 3.12+

### Run (zero infrastructure)
```bash
cd "d:\Crypto Mining\Market data\bharat-ticker"
python run.py          # installs deps on first run, starts the server, opens /ui
```
or manually:
```bash
pip install -e .
uvicorn src.main:app --port 8000
```

### Verify
```bash
curl http://localhost:8000/api/v1/ping
curl http://localhost:8000/api/v1/quote/RELIANCE
curl "http://localhost:8000/api/v1/sb/quote/RELIANCE"   # superbrain feed
```
Dashboards: `/ui` (classic), `/sb` (superbrain feed), `/docs` (Swagger).

## API Endpoints

| Method | Endpoint | Description |
|:---|:---|:---|
| GET | `/api/v1/quote/{symbol}` | Full quote with depth, OHLCV, metadata |
| GET | `/api/v1/quote/{symbol}/depth` | Market depth (5-level order book) |
| GET | `/api/v1/quotes?symbols=A,B,C` | Bulk quote fetch (up to 50) |
| GET | `/api/v1/market/status` | Current market state + session info |
| GET | `/api/v1/historical/{symbol}` | Historical OHLCV candles |
| GET | `/api/v1/historical/{symbol}/eod` | Latest end-of-day summary |
| GET | `/api/v1/search?q={query}` | Symbol/company name search |
| GET | `/api/v1/providers/status` | Data provider health |
| WS | `/api/v1/ws/ticks` | Real-time tick stream (all symbols) |
| WS | `/api/v1/ws/ticks/{symbol}` | Real-time tick stream (single symbol) |
| GET | `/health` | System health check |
| GET | `/docs` | Interactive Swagger UI |

## Configuration

All configuration is via environment variables (`.env` file). Key settings:

| Variable | Default | Description |
|:---|:---|:---|
| `STOCK_UNIVERSE` | `NIFTY50` | Symbol universe to track |
| `SCRAPE_INTERVAL_SECONDS` | `1.0` | Scraping frequency |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `DATABASE_URL` | `postgresql+asyncpg://...` | TimescaleDB connection |
| `NSE_ENABLED` | `true` | Enable NSE scraper |
| `YAHOO_ENABLED` | `true` | Enable Yahoo fallback |

## License

MIT
