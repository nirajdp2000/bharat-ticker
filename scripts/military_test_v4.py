"""Military test for the low-latency bug-fix batch (B1, B2, B3, B5, B6).

Deterministic: the volume/cadence/single-flight/pin assertions are pure-unit
(no network); the quote-contract assertions hit the in-process app and are
sanity-checked (skip-not-fail) when the upstream is closed/empty.

Run:  PYTHONIOENCODING=utf-8 python scripts/military_test_v4.py
"""
from __future__ import annotations

import asyncio
import time
import warnings
from datetime import datetime
from zoneinfo import ZoneInfo

warnings.filterwarnings("ignore")

from fastapi.testclient import TestClient  # noqa: E402

from src.main import app  # noqa: E402
from src.engine.live_candles import LiveCandleEngine  # noqa: E402
from src.engine.failover import FailoverController  # noqa: E402
from src.models.tick import TickData  # noqa: E402
import src.engine.market_data as md  # noqa: E402
import src.api.endpoints.superbrain as sb  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
c = TestClient(app)
PASS = FAIL = SKIP = 0
BUGS: list[str] = []


def ok(cond, name, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        BUGS.append(f"{name} :: {detail}")
        print(f"  FAIL  {name} :: {detail}")


def skip(name, why):
    global SKIP
    SKIP += 1
    print(f"  SKIP  {name} ({why})")


def _tick(sym="X", ex="NSE", src="groww", ltp=100):
    return TickData(symbol=sym, exchange=ex, ltp=ltp, open=ltp, high=ltp, low=ltp,
                    close=ltp - 1, volume=1, timestamp=datetime.now(IST), source=src)


# ── B5: deprecations gone ────────────────────────────────────────────────────
print("\n== B5. NO regex= deprecations (pattern=) ==")
import inspect  # noqa: E402
src_text = inspect.getsource(sb)
ok('regex="^(NSE|BSE)$"' not in src_text, "no Query(regex=) left", "still present")
ok(src_text.count('pattern="^(NSE|BSE)$"') >= 7, "exchange validators use pattern=")


# ── B1: honest, consistent freshness on EVERY quote shape ────────────────────
print("\n== B1. asOf / fetchedAt / feedLagSec contract ==")
fr = sb._freshness("2026-06-22T04:00:00.000Z")
ok({"asOf", "fetchedAt", "feedLagSec"} <= set(fr), "freshness has all 3 fields", str(fr))
ok(fr["feedLagSec"] > 0, "stale feed stamp → positive lag", str(fr["feedLagSec"]))
fr2 = sb._freshness(None)
ok(fr2["feedLagSec"] == 0.0 and fr2["asOf"] == fr2["fetchedAt"], "no feed stamp → lag 0, asOf=fetch")
# live contract via the app (sanity when open)
r = c.get("/api/v1/sb/quote/RELIANCE")
if r.status_code == 200:
    j = r.json()
    ok(all(k in j for k in ("asOf", "fetchedAt", "feedLagSec")), "single /sb/quote carries freshness", str(list(j)[:0]))
else:
    skip("single quote contract", f"upstream {r.status_code}")
rb = c.get("/api/v1/sb/quotes?symbols=RELIANCE,TCS")
if rb.status_code == 200 and rb.json().get("quotes"):
    q0 = rb.json()["quotes"][0]
    ok(all(k in q0 for k in ("asOf", "fetchedAt", "feedLagSec")), "bulk /sb/quotes carries freshness", str(list(q0)))
else:
    skip("bulk quote contract", "upstream empty")


# ── B2: cross-source volume contamination is impossible ──────────────────────
print("\n== B2. single-source rule (no interleave / no double-count) ==")
e = LiveCandleEngine()
# groww owns the key; a 2nd source (bse) arriving while groww is fresh is DROPPED
# — so its cumulative series can't interleave or double the bucket volume.
for ts, px, cum, s in [(0.0, 100, 1000, "groww"), (0.1, 101, 1010, "groww"),
                       (0.2, 99, 2950, "bse"), (0.3, 99, 2960, "bse")]:
    e.record("X", "NSE", px, cum, ts=ts, source=s)
vol = sum(b["volume"] for b in e.build("X", "NSE", seconds=3600))
ok(vol == 10, "secondary source dropped while owner fresh (no double-count)", f"got {vol}, want 10")
ok(e.latest_tick("X", "NSE").get("source") == "groww", "owner source holds the tape")
# pure same-source still accumulates correctly
e2 = LiveCandleEngine()
for ts, cum in [(0.0, 1000), (0.1, 1010), (0.2, 1025)]:
    e2.record("Y", "NSE", 100, cum, ts=ts, source="groww")
v2 = sum(b["volume"] for b in e2.build("Y", "NSE", seconds=3600))
ok(v2 == 25, "same-source delta intact (no over-suppression)", f"got {v2}, want 25")
ok(e.latest_tick("X", "NSE").get("source") == "groww", "latest_tick exposes source")
# stale owner → a new source legitimately takes over
e4 = LiveCandleEngine()
e4.record("W", "NSE", 100, 1000, ts=0.0, source="groww")
e4.record("W", "NSE", 100, 9999, ts=2.0, source="tickertape")   # 2s < TTL → dropped
ok(len(list(e4._samples["NSE:W"])) == 1, "owner holds within TTL")
e4.record("W", "NSE", 100, 500, ts=10.0, source="tickertape")   # >TTL idle → takeover
ok(e4.latest_tick("W", "NSE")["source"] == "tickertape", "stale owner → new source takes over")

# pin: failover confined to one venue
print("\n== B2. failover exchange-pin ==")


class FakeP:
    def __init__(self, name, ex, pri):
        self.name = name
        self.exchange = ex
        self.tier = 1
        self.priority = pri
        self.delayed = False
        self.is_connected = True
        self.avg_latency_ms = 1.0
        self.error_rate = 0.0

    async def connect(self):
        self.is_connected = True

    async def fetch_quote(self, symbol):
        return _tick(symbol, self.exchange, self.name)


async def pin_test():
    fc = FailoverController()
    fc.register_provider(FakeP("groww", "NSE", 5))
    fc.register_provider(FakeP("bse_scraper", "BSE", 20))
    pinned = await fc.fetch_quote("RELIANCE", exchange="NSE")
    unpinned_bse = await fc.fetch_quote("RELIANCE", exchange="BSE")
    return pinned.source, unpinned_bse.source


ps, bs = asyncio.run(pin_test())
ok(ps == "groww", "pin NSE → NSE provider only", ps)
ok(bs == "bse_scraper", "pin BSE → BSE provider only", bs)


# ── B3: single-flight collapses concurrent duplicate fetches ─────────────────
print("\n== B3. single-flight micro-cache ==")
calls = {"n": 0}
_orig = md.market_data_service.get_quote_through


async def _counting(symbol, exchange="NSE", **kw):
    calls["n"] += 1
    await asyncio.sleep(0.2)  # simulate upstream latency
    return _tick(symbol, "NSE", "groww")


async def b3():
    md.market_data_service.get_quote_through = _counting
    sb._live_q_cache.clear()
    sb._live_q_inflight.clear()
    res = await asyncio.gather(*[sb._live_quote("RELIANCE", "NSE") for _ in range(50)])
    return res


try:
    res = asyncio.run(b3())
    ok(calls["n"] == 1, "50 concurrent → 1 upstream fetch", f"got {calls['n']} calls")
    ok(all(t is not None and t.source == "groww" for t in res), "all 50 callers got the tick")
    # TTL cache hit (no new upstream call within TTL)
    calls["n"] = 0
    asyncio.run(sb._live_quote("RELIANCE", "NSE"))
    ok(calls["n"] == 0, "within-TTL repeat served from cache (0 upstream)")
finally:
    md.market_data_service.get_quote_through = _orig
    sb._live_q_cache.clear()
    sb._live_q_inflight.clear()


# ── B6: measured cadence reflects real gaps (non-additive sleep) ─────────────
print("\n== B6. measured cadence honesty ==")
e3 = LiveCandleEngine()
for i, t in enumerate([0.0, 0.5, 1.0, 1.5, 2.0]):
    e3.record("Z", "NSE", 100 + i, 10 * i, ts=t, source="groww")
ok(e3.measured_cadence_s("Z", "NSE") == 0.5, "median inter-sample gap measured", str(e3.measured_cadence_s("Z", "NSE")))
st = e3.status("Z", "NSE")
ok("measuredCadenceSeconds" in st and "samplerTargetSeconds" in st, "status exposes target + measured", str(st))


print("\n" + "=" * 56)
print(f"RESULT: {PASS} pass, {FAIL} fail, {SKIP} skip")
for b in BUGS:
    print("  -", b)
print("=" * 56)
import sys  # noqa: E402
sys.exit(1 if FAIL else 0)
