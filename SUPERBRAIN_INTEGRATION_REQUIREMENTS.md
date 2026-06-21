# Bharat-Ticker → Superbrain Integration Requirements

> Goal: make **bharat-ticker** the single market-data backend for the **superbrain** app
> (`D:\Crypto Mining\localhost`). This doc lists (1) every field bharat-ticker already
> shows on its index page + serves in its API, and (2) every data field superbrain
> actually consumes, so you can add the missing fields/endpoints to bharat-ticker.
>
> Deep-investigation source of truth:
> - Bharat API: `src/api/endpoints/*.py`, `src/models/*.py`, `src/engine/market_data.py`, `src/providers/nse_live.py`
> - Bharat index page: `src/web/index.html`
> - Superbrain consumers: `src/services/upstox-service.mjs`, `src/services/market-service.mjs`,
>   `src/backtest/historical-fetcher.mjs`, `src/services/v9-scanner-service.mjs`,
>   `src/data/broad-equity-universe.json`, `data/historical/instrument_master.json`

---

## PART 1 — What bharat-ticker shows on the index page TODAY

Index page (`src/web/index.html`) renders these blocks, each backed by an API call:

| UI block | API call | Fields rendered |
|---|---|---|
| **Market status pill** | `GET /api/v1/market/status` | `status`, `session` |
| **Index strip** | `GET /api/v1/indices` | per index: `index`(name), `last`, `change`, `pct_change` — filtered to NIFTY 50/BANK/NEXT 50/IT/FIN SERVICE/MIDCAP 100/AUTO/INDIA VIX |
| **Source pills + table** | `GET /api/v1/sources`, `POST /sources/{name}/{enable\|disable}`, `GET /sources/{name}/test` | `name`, `exchange`, `priority`, `circuit_state`, `avg_latency_ms`, `enabled`, `connected` |
| **Quote card** | `GET /api/v1/quote/{symbol}?source=` | `info.symbol`, `info.exchange`, `info.isin`; `price.ltp` (**live price**), `price.change`, `price.pct_change`, `price.open`, `price.high`, `price.low`, `price.close`(prev close), `price.vwap`; `volume.total_traded_volume`; `meta.source`, `meta.source_latency_ms`, `meta.data_quality`, `timestamp` |
| **History chart** | `GET /api/v1/historical/{symbol}?interval=1d&limit=90` | candles: `timestamp`, `open`, `high`, `low`, `close`, `volume`, `value`, `vwap` |
| **Returns & volatility** | `GET /api/v1/quote/{symbol}/analytics` | `returns_pct{1W,1M,YTD,1Y,3Y,5Y}`, `volatility{daily_pct,annualised_pct}`, `week_52_high`, `week_52_low`, `sessions` |
| **Full details** | `GET /api/v1/quote/{symbol}/details` | nested: `price{ltp,change,pct_change,open,high,low,prev_close,vwap,upper_band,lower_band}`, `range{week_52_high/low(+dates),lifetime_high/low(+dates)}`, `moving_averages{5d,30d,50d,150d,200d}`, `bid_ask{bid,bid_qty,ask,ask_qty,total_buy_qty,total_sell_qty}`, `traded{volume,mcap_cr,lot_size,delivery_pct,avg_delivery_20d_pct,avg_volume_20d}`, `fundamentals{isin,face_value,eps_ttm,pe,industry_pe,book_value,book_value_cons,ceps,div_yield,dividend,sector,subsector,nse_id,bse_id}`, `returns_pct{1W,1M,3M,6M,YTD,1Y,2Y,3Y,5Y,7Y}`, `market_depth{buy[],sell[]}` (5-level), `last_updated`, `market_state` |
| **Option chain** | `GET /api/v1/quote/{symbol}/options` | `available`, `underlying_value`, `expiry_dates[]`, `strikes[]{strike,expiry,ce_oi,ce_ltp,pe_oi,pe_ltp}` (needs `PROXY_LIST`) |

Other API surface not on the index page but available:
- `GET /api/v1/quotes?symbols=` — bulk quote (max 50)
- `GET /api/v1/quote/{symbol}/depth` — 5-level order book
- `GET /api/v1/quote/{symbol}/nse` — NSE trade/price/securities info (proxy-gated)
- `GET /api/v1/snapshot?exchange=NSE` — whole-market EOD snapshot (every symbol)
- `GET /api/v1/historical/{symbol}/eod`
- `GET /api/v1/providers/status`
- `GET /api/v1/search?q=`

### Live price — status: ✅ already in the API
`price.ltp` on `/quote/{symbol}` is the live last-traded price (NSE/BSE via the Moneycontrol
relay in `nse_live.py`, real-time during market hours; EOD/last-close when closed). `meta.data_quality`
tells the client whether it is `REAL_TIME` / `END_OF_DAY` / `STALE`, and `meta.source` names the feed.
**No change needed for "live price exists"** — but see Part 3 for the field-name mapping superbrain expects.

---

## PART 2 — What superbrain actually consumes (the real data contract)

Superbrain currently sources data from **Upstox (live + broker historical)** and **Yahoo (delayed
fallback + indices)**. Every field below is something its engines read. To replace Upstox+Yahoo
with bharat-ticker, bharat must serve each of these.

### 2.1 — Live quote  (`getQuotes` / `fetchUpstoxQuotes`)
Per-symbol object superbrain expects (camelCase):

| superbrain field | meaning | bharat source field |
|---|---|---|
| `symbol` | NSE symbol | `info.symbol` |
| `companyName` | company name | `info.name` (often null today) |
| `sector` | sector | `fundamentals.sector` / universe |
| `price` | **last traded price** | `price.ltp` |
| `change` | abs change vs prev close | `price.change` |
| `changePct` | % change | `price.pct_change` |
| `open` | day open | `price.open` |
| `high` | day high | `price.high` |
| `low` | day low | `price.low` |
| `volume` | total traded volume | `volume.total_traded_volume` |
| `previousClose` | prev close | `price.close` |
| `source` | feed id (`UPSTOX_LIVE` / `YAHOO_DELAYED`) | `meta.source` |
| `asOf` | ISO timestamp | `timestamp` |

Notes: superbrain batches up to **120 symbols/request, 6 concurrent batches** (Upstox
`/market-quote/quotes`). Bharat's bulk endpoint caps at **50** — raise to ≥120 or document the cap.
Superbrain needs the gap-fade engine's **session `open`** populated (it synthesizes today's bar from
open/high/low) — open/high/low must be the *current session's*, not previous day.

### 2.2 — Daily candles  (`getDailyCandles` → `fetchYahooChart`)
Array, ascending, each candle:
```
{ timestamp: ISO, open, high, low, close, volume }
```
Ranges requested: `6mo` (default analysis) and `5y` (historical-fetcher backfill).
Bharat already serves this via `GET /historical/{symbol}?interval=1d` — **field names differ**:
bharat returns strings (`"open"`) + `bucket`/`timestamp`; superbrain wants numbers + `timestamp`.

### 2.3 — Intraday candles — TODAY's session  (`fetchIntradayCandles`)  ⚠ CRITICAL GAP
```
GET /v2/historical-candle/intraday/{key}/30minute   (Upstox)
→ [{ timestamp, open, high, low, close, volume, oi }]  ascending
```
Used by the **continuous spike-fade / gap-fade / v9 intraday scanner** (`v9-scanner-service.mjs:728`).
These are **today's forming 30-minute bars from a broker feed** — Yahoo's delayed chart does NOT
provide them reliably. **Bharat-ticker has no equivalent broker-sourced intraday endpoint today.**
Bharat's `/historical?interval=30m` is Yahoo-backed (delayed, may miss today). This is the single
biggest gap for the live edge engines.

### 2.4 — Historical dated candles (day + 30minute)  (`fetchHistoricalCandles`)
```
GET /v2/historical-candle/{key}/{interval}/{toDate}/{fromDate}   (Upstox)
interval ∈ { day, week, month, 30minute, 1minute }
→ Upstox array shape: [ts, open, high, low, close, volume, oi]
→ superbrain normalizes to { timestamp, open, high, low, close, volume, oi } AND keeps positional [0..5]
```
Critically superbrain **corporate-action-adjusts** these (`data/corporate-actions.mjs`) — splits/bonus
back-adjusted before caching. Requests use ranges like `2023-01-01 → today`.
Bharat serves dated history (`/historical?start=&end=`) but: (a) **raw, not CA-adjusted**, (b) Yahoo/TSDB
not broker, (c) no `oi`, (d) string values. Needs upgrades to match.

### 2.5 — Fundamentals  (`getFundamentals` → screener.in scrape)
Superbrain reads these ratio fields:
```
pe, pb, roe, roce, debtToEquity, promoterHolding,
salesGrowth3yr, salesGrowth5yr, profitGrowth3yr, epsGrowth,
dividendYield, operatingMargin, netMargin
```
Bharat's `/details.fundamentals` already has `pe, pb (via book_value), roe?, eps_ttm, div_yield,
book_value, face_value, sector, industry_pe` — but is **missing**: `roce, debtToEquity, promoterHolding,
salesGrowth3yr/5yr, profitGrowth3yr, epsGrowth, operatingMargin, netMargin, marketCap(number)`.
(Bharat has `mcap_cr`; superbrain wants a numeric `marketCap`.)

### 2.6 — Market context / macro  (`getMarketContext`)
Superbrain fetches these instruments (Yahoo symbols) — price + % change each:
| instrument | Yahoo symbol | bharat equivalent |
|---|---|---|
| Nifty 50 | `^NSEI` | `/indices` → "NIFTY 50" |
| Sensex | `^BSESN` | `/indices` (BSE — add) |
| USDINR | `INR=X` | **missing** (FX) |
| Brent crude | `BZ=F` | **missing** (commodity) |
| Gold | `GC=F` | **missing** (commodity) |
Plus **FII/DII net** (`fetchFIIDIIData`): `{ date, fiiNetBuy, diiNetBuy, mood }`.
Bharat has none of FX/commodity/FII-DII today.

### 2.7 — Index values & breadth  (`getMarketContext`, sectoral services)
- Index `last`, `change`, `pct_change` — bharat `/indices` ✅ (already ~139 NSE indices)
- INDIA VIX — bharat `/indices` ✅
- Sectoral indices — bharat `/indices` ✅ (filter by name)
- Market breadth (advances/declines) — **not explicitly served**; superbrain derives from universe sweep.

### 2.8 — Instrument master / universe  (`broad-equity-universe.json`, `instrument_master.json`)
Superbrain needs a symbol→identity map:
```
{ symbol, name, sector, exchange, instrumentKey: "NSE_EQ|<ISIN>", isin, aliases[] }
```
~6800 instruments. Bharat has `search` + `snapshot` but not this exact shape with `instrumentKey`/`aliases`.
For a pure bharat backend, superbrain wouldn't need Upstox `instrumentKey` — but it DOES need a
**stable symbol resolver** (symbol/company-name/alias → canonical symbol + ISIN + exchange).

### 2.9 — Option chain (optional, lower priority)  (`options-service.mjs`)
Superbrain has its own Upstox-based options engine. Bharat's `/options` (strike/CE/PE OI/LTP/IV) could
feed it, but this is secondary to quotes+candles.

---

## PART 3 — GAP ANALYSIS  (bharat HAS vs superbrain NEEDS)

| Requirement | Bharat status | Action needed |
|---|---|---|
| Live LTP + OHLC + change% + volume | ✅ `/quote` | **Rename/adapt** fields → superbrain camelCase (or add an adapter endpoint) |
| `previousClose`, `companyName`, `sector` on quote | ⚠ partial (`close` yes; name/sector often null) | Populate `info.name`, `info.sector` |
| Bulk quotes ≥120 symbols | ⚠ cap 50 | Raise cap to ≥120 |
| Daily candles (6mo, 5y) numeric | ✅ data, ⚠ shape | Numeric values + `timestamp` key |
| **Intraday TODAY 30-min (broker)** | ❌ missing | **Add broker/live intraday endpoint** (biggest gap) |
| Dated history day/30minute | ⚠ Yahoo/TSDB, raw | Add 30minute interval + CA-adjust + `oi` |
| **Corporate-action adjustment** | ❌ raw candles | Add split/bonus back-adjust |
| Fundamentals full set | ⚠ partial | Add `roce, debtToEquity, promoterHolding, sales/profit growth, margins, numeric marketCap` |
| Indices/VIX/sectoral | ✅ `/indices` | none |
| Sensex (`^BSESN`) | ⚠ | Add to indices/context |
| FX (USDINR), Brent, Gold | ❌ | Add macro endpoint |
| FII/DII net | ❌ | Add FII/DII endpoint |
| Instrument/universe resolver (symbol→ISIN+aliases) | ⚠ search only | Add universe dump + alias resolver |
| Option chain | ✅ (proxy-gated) | optional |

---

## PART 4 — RECOMMENDED bharat-ticker additions (superbrain-ready)

Add a **superbrain-compatible** layer (either new fields on existing endpoints or a thin
`/api/v1/sb/*` adapter that returns superbrain's exact camelCase shapes). Proposed contracts:

### 4.1 Quote adapter — `GET /api/v1/sb/quote/{symbol}` (and bulk `?symbols=`)
```json
{ "symbol":"RELIANCE","companyName":"Reliance Industries","sector":"Energy",
  "price":1402.5,"change":12.3,"changePct":0.88,"open":1390,"high":1410,"low":1388,
  "volume":7421233,"previousClose":1390.2,"source":"NSE_LIVE","asOf":"2026-06-20T09:35:00Z" }
```

### 4.2 Daily candles — `GET /api/v1/sb/candles/{symbol}?range=6mo|5y&interval=1d`
```json
[{ "timestamp":"2026-06-19T00:00:00Z","open":1390,"high":1410,"low":1385,"close":1402,"volume":7421233 }]
```
(numbers, ascending, CA-adjusted)

### 4.3 Intraday TODAY — `GET /api/v1/sb/intraday/{symbol}?interval=30minute`  ⚠ key new feature
```json
[{ "timestamp":"2026-06-20T09:45:00+05:30","open":1390,"high":1396,"low":1389,"close":1394,"volume":221344,"oi":0 }]
```
Must be **today's session bars from a live/broker-grade feed** (or best-available intraday), ascending.

### 4.4 Dated history — `GET /api/v1/sb/history/{symbol}?interval=day|30minute&from=&to=`
Same candle shape + `oi`, CA-adjusted.

### 4.5 Fundamentals — `GET /api/v1/sb/fundamentals/{symbol}`
```json
{ "pe":24.1,"pb":2.1,"roe":12.4,"roce":11.0,"debtToEquity":0.38,"promoterHolding":50.3,
  "salesGrowth3yr":14.2,"salesGrowth5yr":11.0,"profitGrowth3yr":18.5,"epsGrowth":18.5,
  "dividendYield":0.4,"operatingMargin":21.3,"netMargin":8.1,"marketCap":1899000,
  "eps":57.2,"bookValue":665,"faceValue":10 }
```

### 4.6 Macro context — `GET /api/v1/sb/context`
```json
{ "indices":{"NIFTY 50":{"last":24500,"changePct":0.4},"SENSEX":{"last":80500,"changePct":0.3}},
  "macro":{"USDINR":{"price":83.2,"changePct":-0.1},"BRENT":{"price":78.4,"changePct":0.6},"GOLD":{"price":2350,"changePct":0.2}},
  "fiiDii":{"date":"2026-06-19","fiiNetBuy":-1240,"diiNetBuy":980,"mood":"BEARISH"},
  "vix":13.2 }
```

### 4.7 Universe / resolver — `GET /api/v1/sb/universe` and `GET /api/v1/sb/resolve?q=`
```json
{ "symbol":"RELIANCE","name":"Reliance Industries","sector":"Energy",
  "exchange":"NSE","isin":"INE002A01018","aliases":["reliance","ril"] }
```

---

## PART 5 — Priority order (build sequence)

1. **Quote adapter** (4.1) — unblocks all live-price consumers immediately. ✅ data already exists.
2. **Daily candles adapter** (4.2) — unblocks technicals/SMA/RSI/MACD/support-resistance.
3. **Corporate-action adjustment** on candles — correctness for splits/bonus.
4. **Intraday TODAY 30-min** (4.3) — unblocks the live gap-fade / spike-fade / v9 intraday edges. *(needs a broker/live intraday source — biggest engineering lift.)*
5. **Fundamentals full set** (4.5) — unblocks fundamental gates/screeners.
6. **Macro context + FII/DII** (4.6) — unblocks regime classifier / risk sentinel.
7. **Universe resolver** (4.7) — unblocks symbol resolution at scale.
8. Option chain (optional).

---

---

## PART 6 — IMPLEMENTED (this build)

A `/api/v1/sb/*` adapter layer was added to bharat-ticker (superbrain untouched).
New files:
- `src/api/endpoints/superbrain.py` — the `/sb/*` router (wired in `src/main.py`)
- `src/engine/live_candles.py` — live 1s/10s OHLCV aggregator (samples NSE/BSE LTP)
- `src/engine/corp_actions.py` — conservative split/bonus back-adjuster
- `src/engine/fundamentals.py` — screener.in scraper + Moneycontrol fallback
- `src/engine/macro.py` — USDINR/Brent/Gold/Sensex (yfinance) + FII/DII (NSE)

### Live endpoints (all verified returning real data)
| Endpoint | Status | Notes |
|---|---|---|
| `GET /api/v1/sb/quote/{symbol}` | ✅ | camelCase, live `nse_live` source |
| `GET /api/v1/sb/quotes?symbols=` | ✅ | bulk up to 200 |
| `GET /api/v1/sb/candles/{symbol}?range=&interval=&adjust=` | ✅ | numeric ascending, CA-adjusted by default |
| `GET /api/v1/sb/intraday/{symbol}?interval=1second\|10second` | ✅ | **live tick aggregation** (NSE/BSE-sourced) — the new sub-minute feature |
| `GET /api/v1/sb/intraday/{symbol}?interval=1minute…30minute` | ✅ | today's session bars (latest available off-session) |
| `GET /api/v1/sb/history/{symbol}?interval=day\|30minute&from=&to=` | ✅ | dated, `+oi`, CA-adjusted |
| `GET /api/v1/sb/fundamentals/{symbol}` | ✅ | screener.in primary; **Moneycontrol fallback** fills pe/pb/eps/bookValue/faceValue/divYield/marketCap/sector when screener IP-blocked |
| `GET /api/v1/sb/context` | ✅ | regime + indices + VIX + macro(USDINR/Brent/Gold/Sensex) + FII/DII |
| `GET /api/v1/sb/universe` / `GET /api/v1/sb/resolve?q=` | ✅ | 3141 NSE / 4358 BSE symbols, alias/substring resolve |

### Sub-minute candles (1s / 10s) — how it works
NSE/BSE/relays expose 1-minute as the smallest interval, so true 1s/10s bars are
built by **sampling the live LTP once per second** via the failover stack and
bucketing into N-second OHLCV. A per-symbol background recorder starts on first
request and auto-stops after 120s idle. Per-bar volume = delta of cumulative
day-volume between samples. Use `interval=1second` or `interval=10second`. The
first call returns `warmingUp:true` while the buffer fills.

### Known environment limits (not code bugs)
- **screener.in returns 502 from this IP** (same anti-bot block as NSE direct) →
  fundamentals served via Moneycontrol fallback (partial set). Full ratio set
  (roce, D/E, promoter, sales/profit growth, margins) populates when deployed on
  a non-blocked IP or with `PROXY_LIST` set.
- Daily/dated candles backfill via Yahoo (delayed) unless TimescaleDB is loaded;
  CA-adjust is conservative (only high-confidence clean split/bonus ratios).
- `universe`/`resolve` `isin` is null from the bhavcopy snapshot; wire the
  instrument master for ISINs if needed.

---

## PART 7 — REAL-DATA / MIN-LATENCY STRATEGY (Yahoo minimised)

Goal: latest data, minimum latency, unbreakable, **real** (not 15-min-delayed Yahoo).
Probed every candidate source from this IP via curl_cffi. Result map:

| Source | Reachable here? | Use |
|---|---|---|
| Moneycontrol **pricefeed** (equity LTP/OHLC) | ✅ live, sub-second | live quotes + 1s/10s tick sampling |
| **BSE StockReachGraph** (`api.bseindia.com/.../StockReachGraph/w`) | ✅ **real intraday** (per-minute price+vol, full session) + daily closes | **intraday 1m–30m OHLC** |
| NSE bhavcopy archive | ✅ EOD | snapshot / universe / daily fallback |
| Yahoo chart | ✅ but 15-min delayed on the *forming* bar | completed daily OHLC + intraday fallback only |
| Moneycontrol **techCharts** | ❌ 403 Akamai | not used |
| NSE `quote-equity` / `chart-databyindex` | ❌ Akamai / empty | not used |
| screener.in | ⚠ intermittent 502 | fundamentals (MC fallback when blocked) |

### Latency hierarchy — pick by freshness need
1. **`/sb/intraday?interval=1second|10second`** — live LTP sampled 1×/s from the MC relay, bucketed → **lowest latency**, real-time. Use for tick-level / "latest price now".
2. **`/sb/intraday?interval=1minute…30minute`** — **REAL BSE intraday** curve aggregated to the timeframe (`source:bse_stockreach_realtime`). **This is what replaces the old 30-min Yahoo candles** — same shape, real exchange data, updates ~every minute during the session.
3. **`/sb/quote`** — live LTP+OHLC (`nse_live` MC relay), `dataQuality:REAL_TIME` in session.
4. **`/sb/candles` (daily)** — completed days from Yahoo (accurate; the delay only affects a *forming* bar, which is overlaid) + **today's forming bar overlaid from the live quote** (`liveLastBar:true`).

### Where Yahoo still appears (and why it's safe)
- **Completed daily OHLC**: Yahoo's 15-min delay only affects the *current* bar — completed days (yesterday and older) are exact. Today's bar is overlaid with the live quote, so nothing delayed reaches the consumer.
- **Intraday fallback**: only when BSE has no curve for an NSE-only symbol (response says `source:yahoo_intraday_delayed, dataQuality:DELAYED`).
- **Macro** (USDINR/Brent/Gold/Sensex): real alternatives are Akamai/redirect-blocked from this IP, so Yahoo stays for cross-asset context (lower priority than equity data).

### Unbreakable cascade (every endpoint)
quote → MC `nse_live` → BSE live → archive (never hard-fails).
intraday → BSE real → Yahoo delayed → (1s/10s live always real).
candles → Yahoo completed + live overlay → BSE daily-close fallback.
fundamentals → screener.in → **Moneycontrol fallback**.
intraday series cached ~45s + 1 retry (BSE rate-limits rapid repeats).

### Data-availability checks (null / incorrect detection)
- Every quote/candle response carries `source` + `dataQuality` (`REAL_TIME`/`DELAYED`/`LAST_CLOSE`/`END_OF_DAY`).
- Quotes auto-enrich `companyName` + `sector` (24h cache from `/details`) so they aren't null.
- **`GET /api/v1/sb/diagnostics/{symbol}`** — probes every source and returns an availability matrix: per-source ok/latency/sample, `nullFields` list, and a `verdict` (`liveQuote:REAL`, `intraday:REAL_BSE`, `daily:OK`, `fundamentals:OK`). Use this to confirm a symbol's data is real before trusting it.

### Future: replacing 30-min candles with "latest data"
When ready, point the superbrain 30-min consumer at `/sb/intraday?interval=30minute` (real BSE)
or, for tick-fresh, `/sb/intraday?interval=10second`. Same candle shape `{timestamp,open,high,low,close,volume,oi}` — no consumer change beyond the URL/source.

---

## PART 8 — EXTRA REAL SOURCES (Chartink / Tickertape / Groww / Investing)

Probed more low-latency public JSON sources via curl_cffi — all reachable from this IP:

| Source | Endpoint | Gives | Wired as |
|---|---|---|---|
| **Groww** | `groww.in/v1/api/stocks_data/v1/accord_points/.../latest_prices_ohlc/{sym}` | NSE LTP/OHLC + **circuit limits** + **52wk** + buy/sell qty + OI | **top-priority quote provider** (priority 5) |
| **Tickertape** | `/stocks/charts/inter/{sid}` | **real NSE intraday** (1-min curve) | **intraday primary** |
| Tickertape | `/search` | symbol → sid + **sector** + name | quote enrichment |
| Tickertape | `/stocks/quotes?sids=` | batch NSE quotes | (available) |
| Tickertape | `/mmi/now` | **Market Mood Index** | `/sb/mmi` + context |
| **Chartink** | `/screener/process` (csrf) | **technical screener** — any scan → matched stocks | `/sb/screen` + `/sb/scans` |
| **Investing.com** | `/api/financialdata/{id}/historical/chart/` | real Brent / Gold (macro) | macro primary (Yahoo fallback) |

### New / upgraded endpoints
- **`/sb/quote`** now served by **Groww** (richest): adds `upperCircuit`, `lowerCircuit`, `week52High`, `week52Low`, `totalBuyQty`, `totalSellQty`, `openInterest`, `vwap`, `isin`. `sector`/`companyName` auto-filled from Tickertape search. Cascade: Groww → MC `nse_live` → BSE → archive.
- **`/sb/intraday?interval=1m…30m`** → `source:tickertape_realtime_nse` (**real NSE**), then BSE real, then Yahoo. (`1second`/`10second` still live tick aggregation.)
- **`/sb/screen?scan=<name>`** or `?clause=<chartink DSL>` → matched stocks `{symbol,name,bseCode,close,changePct,volume}`. Built-ins: `gainers, losers, volume_shockers, breakout_20d_high, near_52w_high, near_52w_low, above_sma_50_200, rsi_oversold, rsi_overbought` (list via **`/sb/scans`**).
- **`/sb/mmi`** → `{value, zone(EXTREME_FEAR…EXTREME_GREED), lastDay/Week/Month/Year}`. Also embedded in `/sb/context` as `marketMood`.
- **`/sb/context.macro`** → Brent/Gold from **Investing.com** (real); USDINR/Sensex Yahoo fallback. Each carries `source`.
- **`/sb/diagnostics`** now probes Tickertape + BSE intraday separately; verdict `intraday:REAL_NSE_TICKERTAPE`.

### Latency / realness ranking (final)
1. `/sb/intraday?interval=10second` — live tick (sub-second).
2. `/sb/intraday?interval=1m…30m` — **real NSE** (Tickertape) → BSE → Yahoo.
3. `/sb/quote` — Groww live (circuit/52wk/OI), `dataQuality:REAL_TIME` in session.
4. `/sb/candles` daily — Yahoo completed + live today-bar overlay.
5. macro — Investing real (Brent/Gold), Yahoo fallback.

Yahoo is now **only** a fallback (completed daily OHLC + intraday last-resort + USDINR/Sensex macro). Every live/intraday path is real.

### New source files
`src/providers/groww.py`, `src/engine/tickertape.py`, `src/engine/chartink.py`,
macro Investing in `src/engine/macro.py`; TickData gained optional `total_buy_qty`/`total_sell_qty`/`open_interest`.

---

## PART 9 — MILITARY-LEVEL TEST + BUGS FIXED

Ran `scripts/military_test.py` (3 waves) against the live server. **3 real bugs found + fixed; final state 0 bugs.**

### Bug 1 — TATAMOTORS cross-source disagreement (11.8% spread) ✅ FIXED
Post-demerger NSE dropped the "TATAMOTORS" ticker → split into **TMCV** (commercial,
new ISIN, ₹402) + **TMPV** (passenger, original ISIN INE155A01022, ₹359.5).
Providers aliased the dead ticker differently (Moneycontrol→commercial, NSE
bhavcopy/BSE/Tickertape→passenger), so `/sb/quote` returned 402 but `/sb/intraday`
returned 359.5 — internally inconsistent + wrong entity.
**Fix:** shared `SYMBOL_ALIASES` (`config/constants.py`) → `TATAMOTORS→TMPV`
(original-ISIN successor), applied in the core `market_data_service` (so classic
*and* `/sb` agree) + removed the stale `TATAMOTORS→TML02` Moneycontrol seed. Quote
now reports `aliasedFrom`. Result: all 6 sources agree at 359.5 (0.00% spread).

### Bug 2 — intraday volume inflated ~1.6× ✅ FIXED
Tickertape `duration=1d` returns ~1.5 sessions; summing all bars double-counted
volume and mixed two days into a "today" chart.
**Fix:** `_latest_session_only()` keeps only the most-recent trading day's bars.
Result: 77 five-min bars/session, volume ratio 1.00 (was 1.6), `sessionDate` exposed.

### Bug 3 — index pseudo-symbols served as equities ✅ FIXED
`/sb/quote/NIFTY` returned a junk/stale ₹19425 (real Nifty ₹24013) via a Groww
mis-match.
**Fix:** `_reject_index()` blocks NIFTY/BANKNIFTY/SENSEX/etc on equity endpoints
(400 → use `/sb/context` or `/indices`).

### Final verification (market closed, last session 2026-06-19)
- **Accuracy:** cross-source spread ≤0.07% on all liquid names (Groww/MC/BSE/Yahoo/Tickertape agree).
- **OHLC integrity:** 0 violations across 10 symbols × intraday + daily.
- **Intraday volume:** sums to 1.00× day volume.
- **Fundamentals:** 14–16 fields (screener.in live; MC fallback when blocked).
- **Latency (warm):** quote **p50 30ms / p95 44ms**, intraday 60ms, candles 170ms, mmi 11ms, context 40ms cached.
- **Concurrency:** 20 parallel quotes → 20/20 ok in 340ms.
- **Stability:** intraday/screen/mmi identical across repeated calls (per-call sessions hold).
- **Edge cases:** invalid→404, lowercase→ok, index→400, fake-in-bulk→`failed[]`.

Test harness kept at `scripts/military_test.py` for regression runs.

---

## PART 10 — NULL/EMPTY AUDIT + UI + JSON LINKS + LIVE REFRESH

### Null/empty audit (every /sb field, 6 symbols) → 0 nulls
Scanned all responses for `null` / `""` / whitespace. Classified + fixed:
| Field | Was | Fix |
|---|---|---|
| `openInterest` | null on cash equities | legit (F&O-only) → **omitted when absent** |
| `vwap` | null (Groww lacks it) | filled from details when enriched; else **omitted** |
| `isin` | null (Groww/MC lack it) | **filled from in-memory BSE master** (no HTTP) — INE002A01018 etc |
| `eps` | null (screener omits) | **computed = price ÷ P/E** |
| `week52High/Low` | null for 1 odd symbol | **omitted when absent** |
| `sector`/`companyName` | (prev fix) | enriched from Tickertape |
**Rule:** core fields (price/OHLC/change/volume/prevClose) always present; optional
rich fields are *omitted when genuinely unavailable* — JSON carries no null/empty noise.
Verified: 0 null/empty across quote+fundamentals+intraday for all test symbols.

### UI — new Superbrain dashboard at `GET /sb`
`src/web/sb.html` (linked from the classic `/` UI). Shows everything from the new feed:
- **Rich quote** card — price, change%, OHLC, circuit band, 52wk, buy/sell qty, VWAP/ISIN
  (only the fields actually present), `dataQuality` badge (LIVE/DELAYED/LAST CLOSE) + source.
- **Real intraday chart** — timeframe selector (1m/5m/15m/30m/10s), shows `source` +
  `sessionDate` (Tickertape real NSE / BSE / live tick).
- **Market Mood** gauge (MMI value + zone bar).
- **Context** — regime, VIX, USDINR/Brent/Gold, FII/DII; index strip.
- **Fundamentals** grid.
- **Screener** — dropdown of Chartink scans → clickable results (click a row → loads that symbol).

### JSON links (check the raw API directly)
Every panel header has a **`{ } JSON`** link opening the exact endpoint in a new tab
(e.g. `/api/v1/sb/quote/RELIANCE`, `/api/v1/sb/context`, `/api/v1/sb/screen?scan=gainers`).

### Continuous refresh — market-aware
The dashboard polls **quote every 3s + intraday every 20s while the market is OPEN**, and
**auto-pauses when closed** (re-checks market state every 30s and resumes automatically at
open). Server side, `/sb/quote` fetches fresh each call (no read-cache on the live path), so
every poll is a real-time tick. Status line shows "live · refreshing every 3s" vs "market
closed — auto-refresh paused".

### Field-name cheat-sheet (bharat → superbrain)
```
ltp                 → price
pct_change          → changePct
change              → change
close               → previousClose
total_traded_volume → volume
info.name           → companyName
bucket/timestamp    → timestamp   (candles, as NUMBER values not strings)
mcap_cr (×1)        → marketCap   (numeric, ₹ lakh/cr — agree on unit)
```
