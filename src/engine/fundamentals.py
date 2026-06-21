"""Fundamentals fetcher — screener.in scrape (matches the superbrain field set).

Returns the exact ratio set superbrain's `getFundamentals` consumes:
    pe, pb, roe, roce, debtToEquity, promoterHolding,
    salesGrowth3yr, salesGrowth5yr, profitGrowth3yr, epsGrowth,
    dividendYield, operatingMargin, netMargin
plus marketCap, eps, bookValue, faceValue when present.

Cached in-process (6h). Bharat's `/quote/{symbol}/details` already covers the
exchange-side stats (bands, 52wk, delivery, moving averages); this fills the
fundamental ratios that the Moneycontrol/NSE feeds don't expose.
"""

from __future__ import annotations

import re
import time
from typing import Any

from curl_cffi.requests import AsyncSession

from ..config.settings import settings
from ..utils.fingerprint import fingerprint_mgr
from ..utils.logger import get_logger

log = get_logger(__name__)

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_TTL_S = 6 * 60 * 60

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

_RATIO_RE = re.compile(
    r'<li[^>]*>[\s\S]*?<span[^>]*class="name"[^>]*>\s*([\s\S]*?)\s*</span>'
    r'[\s\S]*?<span[^>]*class="number"[^>]*>\s*([\d,.\-]+)\s*</span>',
    re.IGNORECASE,
)


def _num(s: str) -> float | None:
    try:
        v = float(str(s).replace(",", ""))
        return v if v == v else None  # drop NaN
    except (TypeError, ValueError):
        return None


def _last_row_value(section_html: str, label: str) -> float | None:
    """Last numeric <td> of the first P&L/BS row whose text contains ``label``."""
    for row in re.findall(r"<tr[^>]*>[\s\S]*?</tr>", section_html, re.IGNORECASE):
        if label.lower() in row.lower():
            tds = re.findall(r"<td[^>]*>\s*(-?[\d,]+\.?\d*)\s*</td>", row, re.IGNORECASE)
            if tds:
                return _num(tds[-1])
    return None


def _variants(symbol: str) -> list[str]:
    s = symbol.strip().upper()
    out = [s]
    if not s.endswith("EQ"):
        out.append(s)  # screener uses bare symbol; consolidated page = /company/SYM/consolidated/
    # consolidated view first improves bank/holding-co coverage
    return [f"{s}/consolidated", s]


_CORE_KEYS = ("roe", "roce", "debtToEquity", "promoterHolding", "salesGrowth3yr",
              "profitGrowth3yr", "operatingMargin", "netMargin")


def _missing_core(d: dict[str, Any] | None) -> bool:
    """True when the screener pull lacks most of the ratio-only fields."""
    if not d:
        return True
    present = sum(1 for k in _CORE_KEYS if d.get(k) is not None)
    return present < 3


async def _from_details(symbol: str) -> dict[str, Any] | None:
    """Moneycontrol/NSE details fallback (reachable when screener.in is IP-blocked).

    Supplies pe/pb/eps/bookValue/faceValue/dividendYield/marketCap/sector — the
    ratio-only fields (roce, D/E, promoter, growth, margins) still need screener.
    """
    try:
        from .market_data import market_data_service
        d = await market_data_service.get_details(symbol)
    except Exception as e:  # noqa: BLE001
        log.debug("fundamentals_details_fallback_failed", symbol=symbol, error=str(e))
        return None
    if not d:
        return None
    f = d.get("fundamentals", {}) or {}
    price = _num((d.get("price") or {}).get("ltp"))
    bv = _num(f.get("book_value"))
    pb = round(price / bv, 2) if (price and bv and bv > 0) else None
    return {
        "symbol": symbol,
        "pe": _num(f.get("pe")),
        "pb": pb,
        "eps": _num(f.get("eps_ttm")),
        "bookValue": bv,
        "faceValue": _num(f.get("face_value")),
        "dividendYield": _num(f.get("div_yield")),
        "marketCap": _num((d.get("traded") or {}).get("mcap_cr")),
        "sector": f.get("sector"),
        "industryPe": _num(f.get("industry_pe")),
    }


async def fetch_fundamentals(symbol: str) -> dict[str, Any] | None:
    symbol = symbol.strip().upper()
    hit = _CACHE.get(symbol)
    if hit and (time.time() - hit[0]) < _TTL_S:
        return hit[1]

    screener = await _scrape_screener(symbol)
    base: dict[str, Any] = dict(screener) if screener else {"symbol": symbol}

    # Fill gaps from the reachable Moneycontrol feed when screener is missing /
    # IP-blocked (502/403). Real partial data beats a 404.
    if _missing_core(screener):
        det = await _from_details(symbol)
        if det:
            for k, v in det.items():
                if base.get(k) is None and v is not None:
                    base[k] = v
            base["source"] = "screener.in+moneycontrol" if screener else "moneycontrol"

    has_data = any(v is not None for k, v in base.items() if k not in ("symbol", "source"))
    if not has_data:
        return None
    _CACHE[symbol] = (time.time(), base)
    return base


async def _scrape_screener(symbol: str) -> dict[str, Any] | None:
    symbol = symbol.strip().upper()
    session = AsyncSession(impersonate=fingerprint_mgr.get_random(),
                           timeout=max(settings.scrape_timeout_seconds, 12), verify=True)
    try:
        for variant in _variants(symbol):
            url = f"https://www.screener.in/company/{variant}/"
            try:
                r = await session.get(url, headers={"User-Agent": UA})
            except Exception as e:  # noqa: BLE001
                log.debug("screener_fetch_failed", symbol=symbol, error=str(e))
                continue
            html = r.text or ""
            if r.status_code != 200 or len(html) < 5000:
                continue

            ratio_map: dict[str, float] = {}
            for name, number in _RATIO_RE.findall(html):
                key = re.sub(r"\s+", " ", name).strip().lower()
                val = _num(number)
                if key and val is not None:
                    ratio_map[key] = val

            def gv(*keys: str) -> float | None:
                for k in keys:
                    for rk, rv in ratio_map.items():
                        if k.lower() in rk:
                            return rv
                return None

            promoter = re.search(r"Promoters[\s\S]{0,400}?<td[^>]*>\s*(\d{1,2}\.\d{1,2})%", html)
            sales3 = re.search(r"Compounded\s+Sales\s+Growth[\s\S]{0,600}?3\s+Years[\s\S]{0,200}?<td[^>]*>\s*(-?\d+\.?\d*)\s*%", html, re.IGNORECASE)
            sales5 = re.search(r"Compounded\s+Sales\s+Growth[\s\S]{0,600}?5\s+Years[\s\S]{0,200}?<td[^>]*>\s*(-?\d+\.?\d*)\s*%", html, re.IGNORECASE)
            profit3 = re.search(r"Compounded\s+Profit\s+Growth[\s\S]{0,600}?3\s+Years[\s\S]{0,200}?<td[^>]*>\s*(-?\d+\.?\d*)\s*%", html, re.IGNORECASE)
            opm = re.search(r"OPM\s*%[\s\S]{0,100}?<td[^>]*>\s*(-?\d+\.?\d*)\s*%?\s*</td>", html, re.IGNORECASE)

            current_price = gv("current price")
            book_value = gv("book value")
            pb = round(current_price / book_value, 2) if (current_price and book_value and book_value > 0) else None

            # P&L slice → net margin
            pl_start = html.find('id="profit-loss"')
            pl_end = html.find('id="balance-sheet"')
            pl_html = html[pl_start:(pl_end if pl_end > -1 else pl_start + 35000)] if pl_start > -1 else ""
            last_sales = (_last_row_value(pl_html, "Sales") or _last_row_value(pl_html, "Revenue")
                          or _last_row_value(pl_html, "Net Revenue") or _last_row_value(pl_html, "Total Income"))
            last_np = (_last_row_value(pl_html, "Net Profit") or _last_row_value(pl_html, "Profit after tax")
                       or _last_row_value(pl_html, "PAT"))
            net_margin = round(last_np / last_sales * 100, 1) if (last_sales and last_np and last_sales > 0) else None

            # Balance-sheet slice → debt/equity
            bs_start = html.find('id="balance-sheet"')
            bs_end = html.find('id="cash-flow"')
            bs_html = html[bs_start:(bs_end if bs_end > -1 else bs_start + 25000)] if bs_start > -1 else ""
            borrowings = _last_row_value(bs_html, "Borrowings")
            equity_cap = _last_row_value(bs_html, "Equity Capital")
            reserves = _last_row_value(bs_html, "Reserves")
            d_to_e = (round(borrowings / (equity_cap + reserves), 2)
                      if (borrowings is not None and equity_cap is not None and reserves is not None
                          and (equity_cap + reserves) != 0) else None)

            data = {
                "symbol": symbol,
                "pe": gv("stock p/e", "p/e"),
                "pb": pb,
                "roe": gv("roe"),
                "roce": gv("roce"),
                "debtToEquity": d_to_e,
                "promoterHolding": _num(promoter.group(1)) if promoter else None,
                "salesGrowth3yr": _num(sales3.group(1)) if sales3 else None,
                "salesGrowth5yr": _num(sales5.group(1)) if sales5 else None,
                "profitGrowth3yr": _num(profit3.group(1)) if profit3 else None,
                "epsGrowth": _num(profit3.group(1)) if profit3 else None,
                "dividendYield": gv("dividend yield"),
                "operatingMargin": _num(opm.group(1)) if opm else None,
                "netMargin": net_margin,
                "marketCap": gv("market cap"),
                # Screener rarely lists EPS in the ratio box → derive from P/E.
                "eps": gv("eps") or (round(current_price / pe_val, 2)
                                     if (current_price and (pe_val := gv("stock p/e", "p/e")) and pe_val > 0) else None),
                "bookValue": book_value,
                "faceValue": gv("face value"),
                "source": "screener.in",
            }
            if any(v is not None for k, v in data.items() if k not in ("symbol", "source")):
                return data
        return None
    finally:
        try:
            await session.close()
        except Exception:  # noqa: BLE001
            pass
