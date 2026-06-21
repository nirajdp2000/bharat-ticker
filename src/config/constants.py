"""Exchange constants, symbol mappings, and Nifty index constituents."""

from __future__ import annotations

# ── Exchange Identifiers ─────────────────────────────────────────────────────
EXCHANGE_NSE = "NSE"
EXCHANGE_BSE = "BSE"

# ── NSE Internal API Endpoints ───────────────────────────────────────────────
# These are the *internal* JSON endpoints used by the NSE website frontend.
# They are NOT official public APIs and may change without notice.
NSE_ENDPOINTS = {
    "home": "/",
    "market_status": "/api/marketStatus",
    "quote_equity": "/api/quote-equity",          # ?symbol=RELIANCE
    "quote_derivative": "/api/quote-derivative",   # ?symbol=RELIANCE
    "equity_meta": "/api/equity-meta-info",        # ?symbol=RELIANCE
    "trade_info": "/api/quote-equity?symbol={symbol}&section=trade_info",
    "option_chain": "/api/option-chain-equities",  # ?symbol=RELIANCE
    "all_indices": "/api/allIndices",
    "index_constituents": "/api/equity-stockIndices",  # ?index=NIFTY%2050
    "market_turnover": "/api/market-turnover",
    "advances_declines": "/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O",
    "block_deals": "/api/block-deal",
    "bulk_deals": "/api/bulk-deal",
    "corp_info": "/api/corporates-corporateActions", # ?index=equities&...
    "chart_data": "/api/chart-databyindex",          # ?index=NIFTY%2050&...
}

# ── BSE Internal API Endpoints ───────────────────────────────────────────────
BSE_ENDPOINTS = {
    "home": "/",
    "quote": "/api/Quote/GetQuote",            # ?scripCode=500325
    "stock_reach": "/api/StockReach/GetStockReach",
    "market_cap": "/api/MarketCap/GetMarketCap",
    "bhav_copy": "/download/BhavCopy/Equity",
}

# ── Market Session Times (IST / UTC+5:30) ────────────────────────────────────
# All times in HH:MM 24-hour format
MARKET_SESSIONS = {
    "pre_open_start": "09:00",
    "pre_open_end": "09:08",
    "pre_open_order_match": "09:08",
    "buffer_period_end": "09:15",
    "market_open": "09:15",
    "market_close": "15:30",
    "post_close_start": "15:30",
    "post_close_end": "15:40",
    "amc_start": "16:00",      # After Market Close session
    "amc_end": "16:00",
}

# ── Yahoo Finance Symbol Mapping ─────────────────────────────────────────────
# NSE symbols on Yahoo use .NS suffix, BSE use .BO suffix
YAHOO_NSE_SUFFIX = ".NS"
YAHOO_BSE_SUFFIX = ".BO"

# ── Nifty 50 Constituents (as of June 2026) ─────────────────────────────────
# This list should be refreshed quarterly from:
# https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050
NIFTY_50_SYMBOLS: list[str] = [
    "ADANIENTERP", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BPCL",
    "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY",
    "EICHERMOT", "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK",
    "ITC", "INDUSINDBK", "INFY", "JSWSTEEL", "KOTAKBANK",
    "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND",
    "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN",
    "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
]

# ── Nifty 100 Additional Symbols ────────────────────────────────────────────
NIFTY_100_ADDITIONAL: list[str] = [
    "ABB", "ABBOTINDIA", "ADANIGREEN", "ADANIPOWER", "AMBUJACEM",
    "ATGL", "AUROPHARMA", "BAJAJHLDNG", "BANKBARODA", "BERGEPAINT",
    "BOSCHLTD", "CANBK", "CHOLAFIN", "COLPAL", "DLF",
    "DABUR", "DIVISLAB", "GAIL", "GODREJCP", "HAVELLS",
    "HDFC", "ICICIGI", "ICICIPRULI", "IDFC", "IDFCFIRSTB",
    "INDIGO", "IOC", "IRCTC", "JIOFIN", "JINDALSTEL",
    "LICI", "LUPIN", "MAXHEALTH", "MOTHERSON", "NAUKRI",
    "PEL", "PIDILITIND", "PNB", "POLYCAB", "RECLTD",
    "SAIL", "SRF", "SHREECEM", "SHRIRAMFIN", "SIEMENS",
    "TORNTPHARM", "TVSMOTOR", "UNITDSPR", "VEDL", "ZYDUSLIFE",
]

# ── User Agents Pool ─────────────────────────────────────────────────────────
# Rotated to avoid fingerprinting. Updated to latest browser versions.
USER_AGENTS: list[str] = [
    # Chrome 131 — Windows 11
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome 131 — macOS Sequoia
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Firefox 133 — Windows 11
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Safari 18 — macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    # Edge 131 — Windows 11
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    # Chrome 131 — Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome 130 — Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Firefox 132 — macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
]

# ── TLS Fingerprint Impersonation Targets ────────────────────────────────────
# curl_cffi impersonate values — match real browser TLS/JA3/JA4 fingerprints.
# IMPORTANT: only list targets the installed curl_cffi actually supports, or
# session creation raises "Impersonating X is not supported" and the provider
# fails intermittently. Verified against curl_cffi 0.15.x.
IMPERSONATE_TARGETS: list[str] = [
    "chrome131",
    "chrome136",
    "chrome142",
    "chrome146",
    "chrome124",
    "safari18_0",
]

# ── Redis Key Prefixes ──────────────────────────────────────────────────────
REDIS_PREFIX_STOCK = "stock"
REDIS_PREFIX_STREAM = "stream:ticks"
REDIS_PREFIX_MARKET = "market"
REDIS_PREFIX_PROVIDER = "provider"
REDIS_PREFIX_RATELIMIT = "ratelimit"
REDIS_PREFIX_SESSION = "session"

# ── Deprecated / renamed symbol aliases ──────────────────────────────────────
# NSE drops a ticker after a demerger/rename; providers then alias the old name
# inconsistently. Canonicalise to the original-ISIN successor so every source
# and endpoint agrees. (Tata Motors split → TMCV commercial + TMPV passenger;
# TMPV keeps the original ISIN INE155A01022, so TATAMOTORS → TMPV.)
SYMBOL_ALIASES: dict[str, str] = {
    "TATAMOTORS": "TMPV",
}


def canonical_symbol(symbol: str) -> str:
    """Map a deprecated/renamed symbol to its canonical NSE successor."""
    s = (symbol or "").strip().upper()
    return SYMBOL_ALIASES.get(s, s)


# ── Data Quality Levels ─────────────────────────────────────────────────────
DATA_QUALITY_REALTIME = "REAL_TIME"
DATA_QUALITY_NEAR_REALTIME = "NEAR_REAL_TIME"
DATA_QUALITY_DELAYED = "DELAYED"
DATA_QUALITY_EOD = "END_OF_DAY"
DATA_QUALITY_STALE = "STALE"
