"""Military-level adversarial test for this session's changes.

Goal: prove the data layer is ARMED (today=scraper/past=Yahoo), low-latency,
large-request safe, and UNBREAKABLE. Structural assertions are strict; data
that depends on a live upstream (market may be closed) is checked for SANITY
only (skipped-not-failed when empty), so the suite is deterministic.

Run:  PYTHONIOENCODING=utf-8 python scripts/military_test_v3.py
"""
from __future__ import annotations

import asyncio
import time
import warnings

warnings.filterwarnings("ignore")

from fastapi.testclient import TestClient  # noqa: E402

from src.main import app  # noqa: E402
from src.engine.market_data import _yahoo_symbol  # noqa: E402
from src.api.endpoints.superbrain import _HIST_CACHE, _TODAY_CACHE  # noqa: E402

c = TestClient(app)
PASS, FAIL, SKIP = 0, 0, 0
BUGS: list[str] = []


def ok(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        BUGS.append(f"{name} — {detail}")
        print(f"  FAIL  {name} :: {detail}")


def skip(name: str, why: str) -> None:
    global SKIP
    SKIP += 1
    print(f"  SKIP  {name} ({why})")


def ohlc_integrity(candles: list, name: str) -> None:
    """Strict OHLC sanity + ascending time on any returned candle set."""
    if not candles:
        skip(name + ":ohlc", "no bars (upstream closed/empty)")
        return
    bad = 0
    prev = ""
    asc = True
    for k in candles:
        o, h, l, cl = k.get("open"), k.get("high"), k.get("low"), k.get("close")
        if None in (o, h, l, cl):
            bad += 1
            continue
        if not (h >= l and h >= max(o, cl) - 1e-6 and l <= min(o, cl) + 1e-6):
            bad += 1
        ts = k.get("timestamp", "")
        if ts < prev:
            asc = False
        prev = ts
    ok(bad == 0, name + ":ohlc-sane", f"{bad} bad bars")
    ok(asc, name + ":ascending", "timestamps not monotonic")


print("\n== 1. INDEX MAPPING (unit) ==")
ok(_yahoo_symbol("^NSEI", "NSE") == "^NSEI", "caret passthrough")
ok(_yahoo_symbol("NIFTY", "NSE") == "^NSEI", "NIFTY alias")
ok(_yahoo_symbol("BANKNIFTY", "NSE") == "^NSEBANK", "BANKNIFTY alias")
ok(_yahoo_symbol("SENSEX", "NSE") == "^BSESN", "SENSEX alias")
ok(_yahoo_symbol("RELIANCE", "NSE") == "RELIANCE.NS", "equity .NS")
ok(_yahoo_symbol("RELIANCE", "BSE") == "RELIANCE.BO", "equity .BO")
ok(_yahoo_symbol("", "NSE") == ".NS", "empty symbol no crash")  # edge: no false-^

print("\n== 2. INPUT VALIDATION (unbreakable) ==")
ok(c.get("/api/v1/sb/history/RELIANCE?interval=banana").status_code == 400, "bad interval → 400")
ok(c.get("/api/v1/sb/history/RELIANCE?interval=day&from=notadate").status_code == 400, "bad from → 400")
ok(c.get("/api/v1/sb/intraday/RELIANCE?interval=1second&date=xx-yy").status_code == 400, "bad date → 400")
ok(c.get("/api/v1/sb/intraday/NIFTY?interval=1minute").status_code == 400, "index on intraday → 400")
ok(c.get("/api/v1/sb/candles/NIFTY").status_code == 400, "index on candles → 400")
ok(c.get("/api/v1/sb/intraday/RELIANCE?interval=zzz").status_code == 400, "bad intraday interval → 400")
r = c.get("/api/v1/sb/intraday/RELIANCE?interval=1second&limit=999999")
ok(r.status_code in (200, 422), "huge limit clamped/handled")

print("\n== 3. PAST-DAY STORE (honest, no-DB) ==")
r = c.get("/api/v1/sb/intraday/RELIANCE?interval=10second&date=2020-01-01")
ok(r.status_code == 503, "past-day, DB down → honest 503", str(r.status_code))
ok("not connected" in r.text.lower(), "503 message explains store")
# valid future/today date must NOT hit the store (falls to live path)
r2 = c.get("/api/v1/sb/intraday/RELIANCE?interval=1second&date=2099-01-01")
ok(r2.status_code == 200, "future date → live path not store")

print("\n== 4. ARMED RULE (today=scraper / past=Yahoo) ==")
for iv in ("1minute", "5minute", "30minute"):
    r = c.get(f"/api/v1/sb/history/RELIANCE?interval={iv}")
    j = r.json()
    ok(r.status_code == 200, f"history {iv} 200")
    ok(j.get("pastSource") == "yahoo", f"{iv} pastSource=yahoo")
    cs = j.get("candles", [])
    today = time.strftime("%Y-%m-%d")
    leak = [k for k in cs if k["timestamp"][:10] == today and j.get("todaySource") != "scraper_live"]
    ok(not leak, f"{iv} no Yahoo today-bar leak", f"{len(leak)} leaked")
    ohlc_integrity(cs, f"hist:{iv}")

print("\n== 5. DAILY + INDEX ==")
rj = c.get("/api/v1/sb/history/RELIANCE?interval=day&from=2024-01-01").json()
ohlc_integrity(rj.get("candles", []), "daily")
ix = c.get("/api/v1/sb/history/%5ENSEI?interval=day&from=2024-01-01").json()
if ix.get("count"):
    ok(ix["count"] > 50, "index ^NSEI history served", f"bars={ix.get('count')}")
    ohlc_integrity(ix.get("candles", []), "index")
else:
    skip("index", "yahoo empty this run")

print("\n== 6. CACHE: bound + stats + clear ==")
_HIST_CACHE.clear(); _TODAY_CACHE.clear()
big = [{"timestamp": "2026-06-19T09:15:00+05:30", "open": 1.0, "high": 1.0,
        "low": 1.0, "close": 1.0, "volume": 1, "oi": 0} for _ in range(2925)]
for i in range(3000):
    _HIST_CACHE.put((f"S{i}", "NSE", "5m", "2024-01-01", "2026-06-21"), [dict(b) for b in big])
st = _HIST_CACHE.stats()
ok(st["approxMB"] <= st["budgetMB"] + 0.1, "cache RAM bounded", f"{st['approxMB']}MB > {st['budgetMB']}MB")
ok(st["entries"] < 3000, "cache evicted under storm", f"entries={st['entries']}")
sc = c.get("/api/v1/sb/cache").json()
ok("past" in sc and "today" in sc, "cache stats shape")
cl = c.post("/api/v1/sb/cache/clear").json()
ok(c.get("/api/v1/sb/cache").json()["past"]["entries"] == 0, "cache clear works")

print("\n== 7. RECORDER (opt-in, observable) ==")
st = c.get("/api/v1/sb/recorder").json()
ok(st["enabled"] is False and st["running"] is False, "dormant by default (no WATCHLIST)")
r = c.post("/api/v1/sb/recorder?symbols=RELIANCE,TCS,INFY")
j = r.json()
ok(j.get("running") is True and j.get("count") == 3, "POST start + watchlist set")
ok(c.post("/api/v1/sb/recorder?symbols=").status_code == 400, "empty watchlist → 400")

print("\n== 8. LARGE-REQUEST / CONCURRENCY STORM (unbreakable) ==")
# True async concurrency in ONE event loop (httpx ASGITransport) — the correct
# way to hammer an async app. (TestClient + threads is NOT thread-safe.)
import httpx  # noqa: E402

syms = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "ITC", "LT"]
urls = [f"/api/v1/sb/history/{s}?interval=30minute" for s in syms] * 6   # 48 concurrent


async def storm():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as ac:
        t0 = time.time()
        res = await asyncio.gather(*(ac.get(u) for u in urls), return_exceptions=True)
        return res, time.time() - t0


res, dt = asyncio.run(storm())
codes = [getattr(r, "status_code", None) for r in res]
errs = [r for r in res if not hasattr(r, "status_code")]
ok(not errs, "no exceptions under 48-way storm", f"{len(errs)} raised")
ok(all(x == 200 for x in codes), "48 concurrent history all 200", f"codes={set(codes)}")
st = _HIST_CACHE.stats()
ok(st["approxMB"] <= st["budgetMB"] + 0.1, "cache bound held under storm", f"{st['approxMB']}MB")
# huge no-limit + NDJSON-stream pulls under the same client
async def heavy():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t", timeout=60) as ac:
        r_all = await ac.get("/api/v1/sb/history/RELIANCE?interval=5minute")        # ALL bars, no limit
        r_str = await ac.get("/api/v1/sb/history/RELIANCE?interval=day&from=2024-01-01&stream=true")
        return r_all, r_str


ra, rs = asyncio.run(heavy())
ok(ra.status_code == 200 and ra.json().get("count", 0) > 1000, "no-limit returns ALL bars", f"n={ra.json().get('count')}")
nd = [l for l in rs.text.splitlines() if l.strip()]
ok(rs.status_code == 200 and len(nd) > 1 and "meta" in nd[0][:20], "NDJSON stream meta+rows", f"lines={len(nd)}")
print(f"  (48 concurrent in {dt:.2f}s, cache {st['approxMB']}MB/{st['budgetMB']}MB, all-bars={ra.json().get('count')}, ndjson={len(nd)})")

print("\n== 9. SPECIAL-CHAR / EDGE SYMBOLS ==")
r = c.get("/api/v1/sb/history/M%26M?interval=day&from=2024-06-01")  # M&M url-encoded
ok(r.status_code == 200, "M&M (encoded &) handled", str(r.status_code))

print("\n" + "=" * 56)
print(f"RESULT: {PASS} pass, {FAIL} fail, {SKIP} skip")
if BUGS:
    print("BUGS:")
    for b in BUGS:
        print("  -", b)
print("=" * 56)
