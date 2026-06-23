"""Tests for the /sb latency features: warm-cache (#7), field projection (#8),
edge Cache-Control (#5)."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.main import app
import src.api.endpoints.superbrain as sb


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _fake_quote(symbol: str) -> dict:
    return {
        "symbol": symbol, "companyName": symbol, "price": 100.0,
        "change": 1.0, "changePct": 1.0, "high": 101.0, "low": 99.0,
        "volume": 1234, "previousClose": 99.0, "source": "tickertape_realtime_nse",
        "dataQuality": "REAL_TIME", "live": True, "open": 99.5,
    }


def test_sb_quotes_field_projection(client, monkeypatch):
    """?fields= shrinks every quote to the requested keys (+ symbol always)."""
    monkeypatch.setattr(sb, "_batch_quotes",
                        AsyncMock(return_value={"RELIANCE": _fake_quote("RELIANCE")}))
    r = client.get("/api/v1/sb/quotes?symbols=RELIANCE&fast=true&fields=price,changePct")
    assert r.status_code == 200
    q = r.json()["quotes"][0]
    assert set(q.keys()) == {"symbol", "price", "changePct"}


def test_sb_quotes_no_projection_keeps_full(client, monkeypatch):
    monkeypatch.setattr(sb, "_batch_quotes",
                        AsyncMock(return_value={"RELIANCE": _fake_quote("RELIANCE")}))
    r = client.get("/api/v1/sb/quotes?symbols=RELIANCE&fast=true")
    q = r.json()["quotes"][0]
    assert "high" in q and "volume" in q          # untouched


def test_sb_quotes_edge_cache_header(client, monkeypatch):
    """#5: response carries a freshness-bounded public Cache-Control for a CDN."""
    monkeypatch.setattr(sb, "_batch_quotes",
                        AsyncMock(return_value={"RELIANCE": _fake_quote("RELIANCE")}))
    r = client.get("/api/v1/sb/quotes?symbols=RELIANCE&fast=true")
    cc = r.headers.get("cache-control", "")
    assert "public" in cc and "max-age=" in cc and "stale-while-revalidate=" in cc


def test_warm_cache_served_from_memory(client, monkeypatch):
    """#7: a warm symbol is served WITHOUT touching the upstream batch."""
    sb._warm_cache.clear()
    sb._warm_cache["RELIANCE"] = (time.time(), _fake_quote("RELIANCE"))
    batch = AsyncMock(return_value={})
    monkeypatch.setattr(sb, "_batch_quotes", batch)
    # request fast=true so the warm entry qualifies and _fill_open is skipped
    r = client.get("/api/v1/sb/quotes?symbols=RELIANCE&fast=true")
    assert r.status_code == 200
    assert r.json()["quotes"][0]["symbol"] == "RELIANCE"
    batch.assert_not_awaited()                    # never hit upstream
    sb._warm_cache.clear()


def test_warm_get_shape_gating():
    """_warm_get rejects entries that can't satisfy the request shape."""
    sb._warm_cache.clear()
    q = _fake_quote("TCS")
    q.pop("open")                                  # price-only warm entry
    sb._warm_cache["TCS"] = (time.time(), q)
    assert sb._warm_get("TCS", rich=False, fast=True) is not None    # fast: ok
    assert sb._warm_get("TCS", rich=False, fast=False) is None       # needs open
    assert sb._warm_get("TCS", rich=True, fast=True) is None         # rich: never warm
    # expired entry
    sb._warm_cache["TCS"] = (time.time() - sb._WARM_TTL_S - 1, q)
    assert sb._warm_get("TCS", rich=False, fast=True) is None
    sb._warm_cache.clear()


def test_sb_warm_status_endpoint(client):
    r = client.get("/api/v1/sb/warm")
    assert r.status_code == 200
    data = r.json()
    for k in ("enabled", "running", "symbols", "intervalSeconds", "ttlSeconds", "cachedQuotes"):
        assert k in data
