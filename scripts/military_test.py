"""Military-level test harness for the /sb/* adapter against a running server."""
from __future__ import annotations
import json, time, urllib.request, urllib.parse, concurrent.futures as cf

BASE = "http://127.0.0.1:8000/api/v1"
BUGS: list[str] = []
def bug(msg): BUGS.append(msg); print("  !! BUG:", msg)

def get(path, **params):
    url = f"{BASE}{path}"
    if params: url += "?" + urllib.parse.urlencode(params)
    t0 = time.time()
    try:
        r = urllib.request.urlopen(url, timeout=30)
        ms = (time.time()-t0)*1000
        return r.status, json.loads(r.read().decode()), ms
    except urllib.error.HTTPError as e:
        return e.code, (json.loads(e.read().decode()) if e.headers.get("content-type","").startswith("application/json") else None), (time.time()-t0)*1000
    except Exception as e:
        return 0, {"error": str(e)}, (time.time()-t0)*1000

def ohlc_sane(c, tag):
    o,h,l,cl = c.get("open"),c.get("high"),c.get("low"),c.get("close")
    if None in (o,h,l,cl): bug(f"{tag}: null OHLC {c}"); return
    if not (l <= o <= h and l <= cl <= h and h >= l):
        bug(f"{tag}: OHLC violation o={o} h={h} l={l} c={cl}")

print("="*70, "\n1. CROSS-SOURCE ACCURACY (last price agreement)\n", "="*70)
for sym in ["RELIANCE","TCS","INFY","IRCTC","TATAMOTORS","HDFCBANK"]:
    prices = {}
    for src,path,key in [("sb_quote",f"/sb/quote/{sym}","price")]:
        s,j,ms = get(path)
        if s==200: prices["sb"]=j.get("price")
    # raw provider compare via forced source on the classic quote
    for src in ["groww","nse","bse","yahoo"]:
        s,j,ms = get(f"/quote/{sym}", source=src)
        if s==200:
            prices[src]=float(j["data"]["price"]["ltp"])
    s,jtt,ms = get(f"/sb/intraday/{sym}", interval="30minute")
    if s==200 and jtt.get("candles"): prices["tt_intra_last"]=jtt["candles"][-1]["close"]
    vals=[v for v in prices.values() if v]
    spread = (max(vals)-min(vals))/min(vals)*100 if vals else 0
    flag = "  <-- DISAGREE" if spread>3 else ""
    print(f"{sym:12} {prices}  spread={spread:.2f}%{flag}")
    if spread>5: bug(f"{sym}: cross-source price spread {spread:.1f}% — {prices}")

print("="*70, "\n2. QUOTE FIELD COMPLETENESS + MATH\n", "="*70)
for sym in ["RELIANCE","TCS","IRCTC"]:
    s,j,ms = get(f"/sb/quote/{sym}")
    print(f"{sym:10} {ms:.0f}ms src={j.get('source')} q={j.get('dataQuality')} circ={j.get('lowerCircuit')}-{j.get('upperCircuit')} 52w={j.get('week52Low')}-{j.get('week52High')} sec={j.get('sector')}")
    for fld in ["price","open","high","low","previousClose","upperCircuit","lowerCircuit","week52High","week52Low","companyName","sector"]:
        if j.get(fld) in (None,""): bug(f"{sym} quote.{fld} is null")
    # math: change == price - previousClose
    if j.get("price") and j.get("previousClose"):
        exp = round(j["price"]-j["previousClose"],2)
        if abs(exp - (j.get("change") or 0)) > 0.2: bug(f"{sym} change math: got {j.get('change')} expected {exp}")
        exp_pct = round((j["price"]-j["previousClose"])/j["previousClose"]*100,2)
        if abs(exp_pct - (j.get("changePct") or 0)) > 0.2: bug(f"{sym} changePct math: got {j.get('changePct')} expected {exp_pct}")
    # circuit sanity: lower < price < upper
    if j.get("upperCircuit") and j.get("lowerCircuit") and j.get("price"):
        if not (j["lowerCircuit"] <= j["price"] <= j["upperCircuit"]): bug(f"{sym} price outside circuit band")
    # 52wk sanity
    if j.get("week52High") and j.get("week52Low"):
        if j["week52Low"] > j["week52High"]: bug(f"{sym} 52w low>high")

print("="*70, "\n3. INTRADAY ACCURACY (TT real NSE) — continuity, vol, tz\n", "="*70)
for sym in ["RELIANCE","TCS"]:
    s,j,ms = get(f"/sb/intraday/{sym}", interval="5minute")
    cs=j.get("candles",[])
    print(f"{sym:10} {ms:.0f}ms src={j.get('source')} bars={len(cs)} q={j.get('dataQuality')}")
    last_ts=None; negvol=0
    for c in cs:
        ohlc_sane(c, f"{sym} intraday")
        if c["volume"]<0: negvol+=1
        if last_ts and c["timestamp"]<=last_ts: bug(f"{sym} intraday non-monotonic ts {c['timestamp']}<={last_ts}")
        last_ts=c["timestamp"]
        if "+05:30" not in c["timestamp"]: bug(f"{sym} intraday ts not IST: {c['timestamp']}")
    if negvol: bug(f"{sym} {negvol} negative-volume bars")

print("="*70, "\n4. DAILY CANDLES — ascending, no null, CA\n", "="*70)
for sym in ["RELIANCE","TCS"]:
    s,j,ms = get(f"/sb/candles/{sym}", range="6mo")
    cs=j.get("candles",[]); print(f"{sym:10} {ms:.0f}ms bars={len(cs)} adjusted={j.get('adjusted')}")
    lt=None
    for c in cs:
        ohlc_sane(c, f"{sym} daily")
        if lt and c["timestamp"]<=lt: bug(f"{sym} daily non-monotonic {c['timestamp']}")
        lt=c["timestamp"]

print("="*70, "\n5. EDGE CASES\n", "="*70)
cases = [("invalid", "/sb/quote/ZZZNOTREAL999"), ("lowercase","/sb/quote/reliance"),
         ("index-ish","/sb/quote/NIFTY"), ("empty-screen","/sb/screen")]
for name,path in cases:
    s,j,ms = get(path)
    print(f"{name:14} status={s} {str(j)[:80]}")
get_inv = get("/sb/quote/ZZZNOTREAL999")
if get_inv[0]==200: bug("invalid symbol returned 200 (should 404)")
s,j,ms = get("/sb/quote/reliance")
if s!=200: bug("lowercase symbol not handled")

print("="*70, "\n6. SCREENER\n", "="*70)
for sc in ["gainers","losers","volume_shockers","near_52w_high","rsi_oversold"]:
    s,j,ms = get("/sb/screen", scan=sc, limit=5)
    print(f"{sc:18} {ms:.0f}ms total={j.get('total')} sample={[x['symbol'] for x in j.get('stocks',[])[:3]]}")
    for x in j.get("stocks",[]):
        if not x.get("symbol"): bug(f"screen {sc}: row missing symbol")

print("="*70, "\n7. LATENCY PROFILE (warm)\n", "="*70)
for path,p in [("/sb/quote/RELIANCE",{}),("/sb/candles/RELIANCE",{"range":"1mo"}),
               ("/sb/intraday/RELIANCE",{"interval":"30minute"}),("/sb/context",{}),
               ("/sb/fundamentals/RELIANCE",{}),("/sb/mmi",{})]:
    s,j,ms = get(path, **p)
    print(f"{path:32} {ms:7.0f}ms  status={s}")

print("="*70, "\n8. CONCURRENCY BURST (20 parallel quotes)\n", "="*70)
syms=["RELIANCE","TCS","INFY","HDFCBANK","ICICIBANK","SBIN","ITC","LT","AXISBANK","WIPRO"]*2
t0=time.time()
with cf.ThreadPoolExecutor(max_workers=20) as ex:
    res=list(ex.map(lambda s: get(f"/sb/quote/{s}"), syms))
oks=sum(1 for r in res if r[0]==200)
print(f"{oks}/{len(syms)} ok in {(time.time()-t0)*1000:.0f}ms; statuses={set(r[0] for r in res)}")
if oks < len(syms): bug(f"concurrency: only {oks}/{len(syms)} succeeded")

print("\n"+"="*70)
print(f"TOTAL BUGS: {len(BUGS)}")
for b in BUGS: print("  -", b)
