"""Military-level test of the UPDATED /sb system — all timeframes (incl
sub-second), the SSE live stream, and the tickertape-batch bulk path.

Adversarial: hunts OHLC/tz/monotonicity/math/null/accounting bugs and reports a
bug count. Run against a live server on 127.0.0.1:8000.
"""
from __future__ import annotations
import asyncio, json, math, time
from datetime import datetime
import httpx

B = "http://127.0.0.1:8000/api/v1"
BUGS: list[str] = []
def bug(m): BUGS.append(m); print("   !! BUG:", m)
def ok(m):  print("   ok:", m)

BASKET = ["RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","SBIN","ITC","LT","AXISBANK","WIPRO",
 "KOTAKBANK","BHARTIARTL","HINDUNILVR","BAJFINANCE","ASIANPAINT","MARUTI","TITAN","SUNPHARMA",
 "ULTRACEMCO","NESTLEIND","TATASTEEL","POWERGRID","NTPC","ONGC","COALINDIA","ADANIENT","ADANIPORTS",
 "JSWSTEEL","HINDALCO","GRASIM","CIPLA","DRREDDY","BAJAJFINSV","HCLTECH","TECHM","M&M","BPCL","IOC",
 "TATACONSUM","UPL","IRCTC","ZOMATO","TATAMOTORS","DLF","DMART","SBILIFE","HDFCLIFE","PNB","CANBK","BEL"]

def ohlc_bad(c):
    o,h,l,cl = c.get("open"),c.get("high"),c.get("low"),c.get("close")
    if None in (o,h,l,cl): return "null OHLC"
    if h < l: return f"high<low {h}<{l}"
    if not (l <= o <= h): return f"open out of range o={o} [{l},{h}]"
    if not (l <= cl <= h): return f"close out of range c={cl} [{l},{h}]"
    if c.get("volume",0) < 0: return "neg volume"
    return None

async def main():
    async with httpx.AsyncClient(base_url=B, timeout=60) as c:
        # baseline prices
        base = {}
        for s in ["RELIANCE","TCS","HDFCBANK"]:
            r = await c.get(f"/sb/quote/{s}"); base[s] = r.json().get("price")

        print("="*78,"\n1. ALL TIMEFRAMES — OHLC/tz/monotonic/granularity/null\n","="*78)
        # warm sub-second recorder
        for _ in range(5):
            await c.get("/sb/intraday/RELIANCE", params={"interval":"500ms"}); await asyncio.sleep(0.5)
        iv = await (await c.get("/sb/intervals")).aread()  # noqa
        allt = ["250ms","500ms","1s","5s","15s","30s","1m","2m","5m","15m","30m","1h"]
        for tf in allt:
            r = await c.get("/sb/intraday/RELIANCE", params={"interval":tf})
            if r.status_code != 200: bug(f"{tf}: status {r.status_code}"); continue
            j = r.json(); cs = j.get("candles",[])
            if not cs: bug(f"{tf}: 0 candles"); continue
            secs = j.get("granularitySeconds")        # set only on live_tick path
            is_live = secs is not None
            sub_second = bool(is_live and secs < 1)    # ms precision only for <1s bars
            last_ts=None; viol=0; tzbad=0; msbad=0; alignbad=0
            for cd in cs:
                b=ohlc_bad(cd)
                if b: viol+=1
                ts=cd["timestamp"]
                if "+05:30" not in ts: tzbad+=1
                if last_ts and ts<=last_ts: bug(f"{tf}: non-monotonic ts {ts}<={last_ts}")
                last_ts=ts
                if sub_second and "." not in ts.split("+")[0]: msbad+=1
                if is_live:
                    e=datetime.fromisoformat(ts).timestamp()
                    if abs((e/secs)-round(e/secs))>1e-6: alignbad+=1
            if viol: bug(f"{tf}: {viol} OHLC violations")
            if tzbad: bug(f"{tf}: {tzbad} non-IST timestamps")
            if msbad: bug(f"{tf}: {msbad} sub-second bars missing ms precision")
            if alignbad: bug(f"{tf}: {alignbad} bars not grid-aligned to {secs}s")
            if is_live and secs not in (0.25,0.5,1,5,15,30): bug(f"{tf}: bad granularitySeconds {secs}")
            if not (viol or tzbad or msbad or alignbad):
                ok(f"{tf:6} bars={len(cs):>3} src={j.get('source'):26} gran={secs} last={last_ts}")

        print("="*78,"\n2. SUB-SECOND MECHANICS — samples grow, bucket spacing\n","="*78)
        s1=(await c.get("/sb/intraday/RELIANCE",params={"interval":"500ms"})).json()
        n1=s1.get("samples"); await asyncio.sleep(2.2)
        s2=(await c.get("/sb/intraday/RELIANCE",params={"interval":"500ms"})).json()
        n2=s2.get("samples")
        if n2<=n1: bug(f"samples not growing ({n1}->{n2})")
        else: ok(f"samples grew {n1}->{n2} over ~2.2s (sampler ~0.5s)")
        cs=s2.get("candles",[])
        if len(cs)>=2:
            es=[datetime.fromisoformat(x["timestamp"]).timestamp() for x in cs[-6:]]
            gaps=[round(b-a,3) for a,b in zip(es,es[1:])]
            ok(f"500ms bar gaps (s): {gaps}")
            if any(g<0 for g in gaps): bug("negative 500ms gap")

        print("="*78,"\n3. SSE STREAM — frames valid, epoch monotonic, rate, price sane\n","="*78)
        frames=[]; t0=time.time()
        async with c.stream("GET","/sb/stream/RELIANCE",params={"maxSeconds":8}) as r:
            ctype=r.headers.get("content-type","")
            if "text/event-stream" not in ctype: bug(f"stream content-type {ctype}")
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    try: frames.append(json.loads(line[5:].strip()))
                    except Exception: bug(f"malformed SSE frame: {line[:60]}")
                if time.time()-t0>6: break
        ticks=[f for f in frames if f.get("price") is not None]
        if not frames or frames[0].get("price") is not None: bug("first SSE frame should be the open/meta frame")
        else: ok(f"open frame: {frames[0]}")
        if len(ticks)<3: bug(f"too few SSE ticks ({len(ticks)}) in ~6s")
        else:
            rate=len(ticks)/6
            ok(f"{len(ticks)} ticks in ~6s ({rate:.1f}/s)")
            if rate<0.5 or rate>6: bug(f"tick rate {rate:.1f}/s out of sane band")
        eps=[t["epoch"] for t in ticks]
        if eps!=sorted(eps): bug("SSE epoch not monotonic")
        else: ok("SSE epoch monotonic")
        for t in ticks:
            if t["price"]<=0 or t["price"]!=t["price"]: bug(f"SSE bad price {t['price']}")
            if "+05:30" not in t.get("timestamp",""): bug("SSE ts not IST")
        if ticks and base.get("RELIANCE"):
            d=abs(ticks[-1]["price"]-base["RELIANCE"])/base["RELIANCE"]*100
            if d>2: bug(f"SSE price {ticks[-1]['price']} disagrees quote {base['RELIANCE']} ({d:.1f}%)")
            else: ok(f"SSE price agrees quote (Δ{d:.2f}%)")
        # maxSeconds bounds
        r=await c.get("/sb/stream/RELIANCE",params={"maxSeconds":3})
        if r.status_code!=422: bug(f"maxSeconds=3 should 422, got {r.status_code}")
        else: ok("maxSeconds<5 rejected (422)")
        # index reject on stream
        r=await c.get("/sb/stream/NIFTY",params={"maxSeconds":10})
        if r.status_code!=400: bug(f"stream NIFTY should 400, got {r.status_code}")
        else: ok("stream index-symbol rejected (400)")

        print("="*78,"\n4. BULK BATCH — completeness/accounting/math/source/rich/cap\n","="*78)
        import urllib.parse as up
        syms=up.quote(",".join(BASKET))   # 50 incl M&M
        t=time.time(); j=(await c.get(f"/sb/quotes?symbols={syms}")).json(); ms=(time.time()-t)*1000
        uniq=len(set(_canon_local(s) for s in BASKET))
        if j["count"]+len(j["failed"])<uniq: bug(f"bulk accounting: count {j['count']} + failed {len(j['failed'])} < unique {uniq}")
        else: ok(f"bulk {j['count']}/{uniq} in {ms:.0f}ms failed={j['failed']} sources={j['sources']}")
        if "sources" not in j: bug("bulk missing sources breakdown")
        # M&M must be present (fallback)
        got=[q["symbol"] for q in j["quotes"]]
        if not any(x in ("M&M","TMPV","TATAMOTORS") for x in got): pass
        if "M&M" not in got and "M&M" not in j["failed"]: bug("M&M neither returned nor failed (silent drop)")
        else: ok("M&M handled (no silent drop)")
        # changePct math on tickertape rows
        mathbad=0
        for q in j["quotes"]:
            p,pc,pct=q.get("previousClose"),q.get("change"),q.get("changePct")
            if p and pct is not None and pc is not None:
                exp=round(pc/p*100,2)
                if abs(exp-pct)>0.2: mathbad+=1
        if mathbad: bug(f"bulk changePct math off on {mathbad} rows")
        else: ok("bulk changePct math correct")
        # cross-check bulk price == single quote
        diffbad=0
        for q in j["quotes"][:8]:
            sr=await c.get(f"/sb/quote/{up.quote(q['symbol'])}")
            if sr.status_code==200:
                sp=sr.json().get("price")
                if sp and q["price"] and abs(sp-q["price"])/sp*100>1.5: diffbad+=1
        if diffbad: bug(f"bulk vs single price disagree on {diffbad}/8")
        else: ok("bulk price == single quote (≤1.5%)")
        # rich mode
        jr=(await c.get(f"/sb/quotes?symbols={syms}&rich=true")).json()
        rq=jr["quotes"][0]
        if "open" not in rq: bug("rich bulk missing open")
        elif "lowerCircuit" not in rq and "week52High" not in rq: bug("rich bulk missing circuit/52wk")
        else: ok(f"rich bulk has open={rq.get('open')} circ={rq.get('lowerCircuit')}-{rq.get('upperCircuit')} sources={jr['sources']}")
        # garbage symbols accounted in failed
        jg=(await c.get("/sb/quotes",params={"symbols":"ZZZFAKE1,RELIANCE,QQQNOPE2"})).json()
        if "ZZZFAKE1" not in jg["failed"] or "QQQNOPE2" not in jg["failed"]: bug(f"garbage not in failed[]: {jg['failed']}")
        else: ok(f"garbage symbols -> failed {jg['failed']}, real ones ok ({jg['count']})")
        # 200 cap
        many=",".join((BASKET*5))  # 250 entries
        jc=(await c.get("/sb/quotes",params={"symbols":many})).json()
        ok(f"250-entry request -> count={jc['count']} (cap 200 unique<=50 here)")
        # 6x concurrent 120
        syms120=up.quote(",".join((BASKET*3)[:120]))
        async def batch():
            t=time.time(); r=await c.get(f"/sb/quotes?symbols={syms120}"); return (time.time()-t)*1000, r.json()["count"]
        t0=time.time(); res=await asyncio.gather(*(batch() for _ in range(6))); wall=time.time()-t0
        counts=set(x[1] for x in res)
        if any(x[1]<100 for x in res): bug(f"concurrent bulk dropped: counts={[x[1] for x in res]}")
        else: ok(f"6x120 concurrent wall={wall:.2f}s counts={counts}")

        print("="*78,"\n5. EDGE / ADVERSARIAL\n","="*78)
        cases=[("bad interval","/sb/intraday/RELIANCE?interval=7minute",400),
               ("intraday index","/sb/intraday/NIFTY?interval=5m",400),
               ("intraday huge limit","/sb/intraday/RELIANCE?interval=5m&limit=99999",422),
               ("quote invalid","/sb/quote/ZZZNOTREAL",404),
               ("quote lowercase","/sb/quote/reliance",200),
               ("empty symbols","/sb/quotes?symbols=",400),
               ("intervals","/sb/intervals",200)]
        for name,path,exp in cases:
            r=await c.get(path)
            if r.status_code!=exp: bug(f"{name}: expected {exp} got {r.status_code}")
            else: ok(f"{name}: {r.status_code}")

        print("="*78,"\n6. CONCURRENCY — mixed burst (quote+TF+bulk)\n","="*78)
        import urllib.parse as up2
        paths=[]
        for i in range(40): paths.append(f"/sb/quote/{up2.quote(BASKET[i%len(BASKET)])}")
        for tf in ["500ms","1s","5s","5m","30m"]*4: paths.append(f"/sb/intraday/RELIANCE?interval={tf}")
        for i in range(6): paths.append(f"/sb/quotes?symbols={up2.quote(','.join(BASKET[:30]))}")
        sem=asyncio.Semaphore(40)
        async def one(p):
            async with sem:
                try:
                    r=await c.get(p); return r.status_code
                except Exception: return 0
        t0=time.time(); st=await asyncio.gather(*(one(p) for p in paths)); dur=time.time()-t0
        okc=sum(1 for s in st if s==200)
        if okc<len(paths): bug(f"mixed burst: {okc}/{len(paths)} ok, bad={[s for s in st if s!=200][:8]}")
        else: ok(f"mixed burst {okc}/{len(paths)} ok in {dur:.1f}s")

        print("\n"+"="*78)
        print(f"TOTAL BUGS: {len(BUGS)}")
        for b in BUGS: print("  -", b)

def _canon_local(s):
    return {"TATAMOTORS":"TMPV"}.get(s.upper(), s.upper())

asyncio.run(main())
