"""High-pressure stress + consistency test for the /sb/* superbrain backend.

NB: symbols are URL-encoded (urllib.parse.quote) so `&`-tickers (M&M) don't
truncate the bulk query string.

Simulates superbrain switching to bharat-ticker as its SOLE market-data backend
pulling continuously without delay. Measures: latency percentiles, throughput,
error/serve rate, degradation under sustained load, cross-repeat consistency,
and full superbrain data-contract completeness.

Run:  python scripts/stress_test.py
(server must already be up on 127.0.0.1:8000)
"""
from __future__ import annotations
import asyncio, json, statistics, time, sys
import httpx

BASE = "http://127.0.0.1:8000/api/v1"

# Liquid NSE basket (superbrain scans/journal trade these).
BASKET = [
    "RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","SBIN","ITC","LT","AXISBANK","WIPRO",
    "KOTAKBANK","BHARTIARTL","HINDUNILVR","BAJFINANCE","ASIANPAINT","MARUTI","TITAN","SUNPHARMA",
    "ULTRACEMCO","NESTLEIND","TATAMOTORS","TATASTEEL","POWERGRID","NTPC","ONGC","COALINDIA",
    "ADANIENT","ADANIPORTS","JSWSTEEL","HINDALCO","GRASIM","CIPLA","DRREDDY","DIVISLAB",
    "BAJAJFINSV","HCLTECH","TECHM","BRITANNIA","EICHERMOT","HEROMOTOCO","BAJAJ-AUTO","M&M",
    "INDUSINDBK","SHRIRAMFIN","APOLLOHOSP","BPCL","IOC","TATACONSUM","UPL","LTIM",
    "DMART","PIDILITIND","SBILIFE","HDFCLIFE","ICICIPRULI","DLF","GODREJCP","HAVELLS",
    "AMBUJACEM","BANKBARODA","PNB","CANBK","IRCTC","ZOMATO","PAYTM","NYKAA",
]

def pct(xs, p):
    if not xs: return 0.0
    xs = sorted(xs); k = (len(xs)-1)*p/100
    f = int(k); c = min(f+1, len(xs)-1)
    return xs[f] + (xs[c]-xs[f])*(k-f)

def stat_line(name, lat, statuses, dur):
    ok = sum(1 for s in statuses if s == 200)
    n = len(statuses)
    rps = n/dur if dur else 0
    codes = {}
    for s in statuses: codes[s] = codes.get(s,0)+1
    bad = {k:v for k,v in codes.items() if k!=200}
    print(f"{name:34} n={n:5d} ok={ok:5d} ({100*ok/max(n,1):5.1f}%) "
          f"rps={rps:7.1f}  p50={pct(lat,50):6.0f} p95={pct(lat,95):6.0f} "
          f"p99={pct(lat,99):6.0f} max={max(lat) if lat else 0:6.0f}ms  "
          f"{'BAD='+str(bad) if bad else 'clean'}")
    return {"n":n,"ok":ok,"rps":rps,"p50":pct(lat,50),"p95":pct(lat,95),
            "p99":pct(lat,99),"max":max(lat) if lat else 0,"bad":bad}

async def fetch(client, path):
    t = time.perf_counter()
    try:
        r = await client.get(path)
        ms = (time.perf_counter()-t)*1000
        return r.status_code, ms, r
    except Exception as e:
        return 0, (time.perf_counter()-t)*1000, str(e)

async def burst(client, paths, concurrency):
    """Fire len(paths) requests with a concurrency cap; return (lats, statuses, dur)."""
    sem = asyncio.Semaphore(concurrency)
    lats=[]; statuses=[]
    async def one(p):
        async with sem:
            s,ms,_ = await fetch(client,p)
            lats.append(ms); statuses.append(s)
    t0=time.perf_counter()
    await asyncio.gather(*(one(p) for p in paths))
    return lats, statuses, time.perf_counter()-t0

async def main():
    limits = httpx.Limits(max_connections=300, max_keepalive_connections=300)
    async with httpx.AsyncClient(base_url=BASE, timeout=60, limits=limits) as c:
        results={}

        # warm a few caches so we measure steady-state not cold-start
        for p in ["/sb/quote/RELIANCE","/sb/context","/sb/candles/RELIANCE?range=6mo"]:
            await fetch(c,p)

        print("="*120)
        print("SCENARIO A — QUOTE CONCURRENCY RAMP  (superbrain live-quote path, NO server read-cache → every call hits upstream)")
        print("="*120)
        for conc in [10,25,50,100,200]:
            paths=[f"/sb/quote/{BASKET[i%len(BASKET)]}" for i in range(conc*4)]
            lats,st,dur=await burst(c,paths,conc)
            results[f"quote_c{conc}"]=stat_line(f"quote conc={conc} (x{len(paths)})",lats,st,dur)

        print("\n"+"="*120)
        print("SCENARIO B — BULK QUOTES  (superbrain batches 120 symbols, 6 concurrent batches)")
        print("="*120)
        import urllib.parse as _up
        for size,label in [(50,"50"),(120,"120"),(200,"200")]:
            syms=_up.quote(",".join((BASKET*4)[:size]))   # encode & in M&M etc.
            paths=[f"/sb/quotes?symbols={syms}"]*6
            lats,st,dur=await burst(c,paths,6)
            # verify counts
            s,ms,r=await fetch(c,f"/sb/quotes?symbols={syms}")
            got=r.json().get("count") if s==200 else 0
            failed=len(r.json().get("failed",[])) if s==200 else "?"
            results[f"bulk_{label}"]=stat_line(f"bulk {label}sym x6 (got={got}/{size} failed={failed})",lats,st,dur)

        print("\n"+"="*120)
        print("SCENARIO C — INTRADAY CONCURRENCY  (real-NSE tickertape path, the live-edge feed)")
        print("="*120)
        for conc in [10,25,50]:
            paths=[f"/sb/intraday/{BASKET[i%len(BASKET)]}?interval=30minute" for i in range(conc*2)]
            lats,st,dur=await burst(c,paths,conc)
            results[f"intraday_c{conc}"]=stat_line(f"intraday30m conc={conc} (x{len(paths)})",lats,st,dur)

        print("\n"+"="*120)
        print("SCENARIO D — MIXED REALISTIC WORKLOAD  (quote+candles+intraday+context+screen, like real superbrain consumers)")
        print("="*120)
        mix=[]
        for i in range(120): mix.append(f"/sb/quote/{BASKET[i%len(BASKET)]}")
        for i in range(40):  mix.append(f"/sb/intraday/{BASKET[i%len(BASKET)]}?interval=30minute")
        for i in range(20):  mix.append(f"/sb/candles/{BASKET[i%len(BASKET)]}?range=6mo")
        for i in range(10):  mix.append("/sb/context")
        for i in range(10):  mix.append("/sb/screen?scan=gainers&limit=20")
        import random; random.shuffle(mix)
        lats,st,dur=await burst(c,mix,80)
        results["mixed"]=stat_line(f"mixed workload (x{len(mix)}) conc=80",lats,st,dur)

        print("\n"+"="*120)
        print("SCENARIO E — SUSTAINED CONTINUOUS POLL  (superbrain pulls WITHOUT DELAY for 60s; watch for degradation / upstream 429)")
        print("="*120)
        DURATION=60; CONC=40
        sem=asyncio.Semaphore(CONC)
        buckets={}  # 10s bucket -> (lats, statuses)
        stop=time.perf_counter()+DURATION
        start=time.perf_counter()
        async def worker(wid):
            i=0
            while time.perf_counter()<stop:
                sym=BASKET[(wid*7+i)%len(BASKET)]
                async with sem:
                    s,ms,_=await fetch(c,f"/sb/quote/{sym}")
                b=int((time.perf_counter()-start)//10)
                buckets.setdefault(b,([],[]))
                buckets[b][0].append(ms); buckets[b][1].append(s)
                i+=1
        await asyncio.gather(*(worker(w) for w in range(CONC)))
        all_l=[]; all_s=[]
        print(f"  {'window':10} {'reqs':>6} {'ok%':>6} {'rps':>7} {'p50':>7} {'p95':>7} {'p99':>7} {'max':>7}")
        for b in sorted(buckets):
            l,s=buckets[b]; all_l+=l; all_s+=s
            ok=100*sum(1 for x in s if x==200)/max(len(s),1)
            print(f"  {b*10:>3}-{b*10+10:<6}s {len(s):>6} {ok:>6.1f} {len(s)/10:>7.1f} "
                  f"{pct(l,50):>7.0f} {pct(l,95):>7.0f} {pct(l,99):>7.0f} {max(l):>7.0f}")
        results["sustained"]=stat_line("SUSTAINED total",all_l,all_s,DURATION)
        # degradation check: first bucket vs last bucket p95
        bs=sorted(buckets)
        if len(bs)>=2:
            first95=pct(buckets[bs[0]][0],95); last95=pct(buckets[bs[-1]][0],95)
            drift=(last95-first95)/max(first95,1)*100
            print(f"  >> p95 drift first→last window: {first95:.0f}ms → {last95:.0f}ms ({drift:+.0f}%)  "
                  f"{'⚠ DEGRADING' if drift>50 else 'STABLE'}")
            results["drift_pct"]=drift

        print("\n"+"="*120)
        print("SCENARIO F — CONSISTENCY  (same symbol x30 rapid repeats: field stability + required-field presence)")
        print("="*120)
        REQ_QUOTE=["symbol","price","open","high","low","previousClose","volume","change","changePct","source","dataQuality"]
        for sym in ["RELIANCE","TCS","HDFCBANK"]:
            paths=[f"/sb/quote/{sym}"]*30
            lats,st,dur=await burst(c,paths,10)
            # pull bodies for field/price check
            prices=[]; missing=set()
            rs=await asyncio.gather(*(fetch(c,f"/sb/quote/{sym}") for _ in range(15)))
            for s,ms,r in rs:
                if s==200:
                    j=r.json(); prices.append(j.get("price"))
                    for f in REQ_QUOTE:
                        if j.get(f) in (None,""): missing.add(f)
            pv=[p for p in prices if p]
            spread=(max(pv)-min(pv))/min(pv)*100 if pv else 0
            ok=sum(1 for s in st if s==200)
            print(f"  {sym:10} 30x ok={ok}/30 p95={pct(lats,95):.0f}ms  priceSpread={spread:.3f}%  "
                  f"missingReqFields={sorted(missing) if missing else 'NONE'}")

        print("\n"+"="*120)
        print("SCENARIO G — SUPERBRAIN DATA-CONTRACT COMPLETENESS  (every field the engines read, across the basket)")
        print("="*120)
        sample=BASKET[:20]
        # quotes
        qok=0; qfields={}
        for sym in sample:
            s,ms,r=await fetch(c,f"/sb/quote/{sym}")
            if s==200:
                qok+=1; j=r.json()
                for f in REQ_QUOTE:
                    qfields[f]=qfields.get(f,0)+(1 if j.get(f) not in (None,"") else 0)
        print(f"  QUOTE  {qok}/{len(sample)} ok. core-field presence:")
        for f in REQ_QUOTE:
            print(f"      {f:16} {qfields.get(f,0)}/{qok}")
        # candles ascending+sane
        cbad=0; cok=0
        for sym in sample[:10]:
            s,ms,r=await fetch(c,f"/sb/candles/{sym}?range=6mo")
            if s==200:
                cok+=1; cs=r.json().get("candles",[])
                lt=None
                for cd in cs:
                    o,h,l,cl=cd["open"],cd["high"],cd["low"],cd["close"]
                    if None in (o,h,l,cl) or not(l<=o<=h and l<=cl<=h): cbad+=1; break
                    if lt and cd["timestamp"]<=lt: cbad+=1; break
                    lt=cd["timestamp"]
        print(f"  CANDLES {cok}/10 ok, OHLC/asc violations: {cbad}")
        # intraday
        iok=0; ireal=0
        for sym in sample[:10]:
            s,ms,r=await fetch(c,f"/sb/intraday/{sym}?interval=30minute")
            if s==200:
                iok+=1
                if "yahoo" not in (r.json().get("source") or ""): ireal+=1
        print(f"  INTRADAY {iok}/10 ok, real-feed (non-yahoo): {ireal}/10")
        # fundamentals
        FUND=["pe","pb","roe","marketCap","eps"]
        fok=0; ffields={}
        for sym in sample[:10]:
            s,ms,r=await fetch(c,f"/sb/fundamentals/{sym}")
            if s==200:
                fok+=1; j=r.json()
                for f in FUND: ffields[f]=ffields.get(f,0)+(1 if j.get(f) is not None else 0)
        print(f"  FUNDAMENTALS {fok}/10 ok. field presence: "+", ".join(f"{f}={ffields.get(f,0)}" for f in FUND))
        # context
        s,ms,r=await fetch(c,"/sb/context")
        if s==200:
            j=r.json()
            print(f"  CONTEXT ok: regime={j.get('regime')} vix={j.get('vix')} "
                  f"indices={list((j.get('indices') or {}).keys())} "
                  f"macro={list((j.get('macro') or {}).keys())} "
                  f"fiiDii={'yes' if j.get('fiiDii') else 'no'} mmi={'yes' if j.get('marketMood') else 'no'}")

        print("\n"+"="*120)
        print("VERDICT")
        print("="*120)
        worst_ok=min(r["ok"]/max(r["n"],1) for k,r in results.items() if isinstance(r,dict) and "n" in r)
        any_bad=any(r.get("bad") for k,r in results.items() if isinstance(r,dict))
        print(f"  lowest success rate across all scenarios: {worst_ok*100:.1f}%")
        print(f"  any non-200 anywhere: {'YES' if any_bad else 'NO'}")
        if "drift_pct" in results:
            print(f"  sustained p95 drift: {results['drift_pct']:+.0f}% ({'DEGRADING' if results['drift_pct']>50 else 'STABLE under continuous load'})")

asyncio.run(main())
