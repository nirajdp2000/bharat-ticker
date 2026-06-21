"""Residential-proxy pool — round-robin / random / least-used rotation over
``PROXY_LIST`` with per-proxy failure cooldown.

The NSE-direct providers (``nse_scraper``, ``nse_public``) route their curl_cffi
egress through the *current* proxy and ``rotate()`` on an Akamai block or a dead
endpoint, so a multi-proxy ``PROXY_LIST`` actually distributes load and survives a
bad proxy. The previous code used ``proxy_list[0]`` only (no rotation), leaving
``PROXY_ROTATION_STRATEGY`` unused — this wires it up.
"""

from __future__ import annotations

import random
import threading
import time

from ..config.settings import ProxyStrategy, settings
from ..utils.logger import get_logger

log = get_logger(__name__)

_COOLDOWN_S = 120.0  # park a failing proxy this long before reusing it


def mask_proxy(proxy: str | None) -> str | None:
    """Strip credentials for logs/responses — keep only host:port (after last @)."""
    if not proxy:
        return None
    return proxy.split("@")[-1]


class ProxyPool:
    """Picks the active outbound proxy and rotates on failure. Single-worker safe."""

    def __init__(self) -> None:
        self._proxies: list[str] = list(settings.proxy_list)
        self._strategy: ProxyStrategy = settings.proxy_rotation_strategy
        self._idx = 0
        self._uses: dict[str, int] = {p: 0 for p in self._proxies}
        self._dead_until: dict[str, float] = {}
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self._proxies)

    @property
    def count(self) -> int:
        return len(self._proxies)

    def _alive(self) -> list[str]:
        now = time.time()
        return [p for p in self._proxies if self._dead_until.get(p, 0.0) <= now]

    def current(self) -> str | None:
        """The proxy to use right now (sticky until ``rotate()`` or cooldown)."""
        if not self._proxies:
            return None
        with self._lock:
            pool = self._alive() or self._proxies  # all parked → use anyway
            if self._strategy == ProxyStrategy.RANDOM:
                p = random.choice(pool)
            elif self._strategy == ProxyStrategy.LEAST_USED:
                p = min(pool, key=lambda x: self._uses.get(x, 0))
            else:  # ROUND_ROBIN
                p = pool[self._idx % len(pool)]
            self._uses[p] = self._uses.get(p, 0) + 1
            return p

    def rotate(self, bad: str | None = None, reason: str = "") -> str | None:
        """Advance to the next proxy; park ``bad`` on cooldown if given."""
        if not self._proxies:
            return None
        with self._lock:
            if bad:
                self._dead_until[bad] = time.time() + _COOLDOWN_S
                log.warning("proxy_marked_bad", proxy=mask_proxy(bad), reason=reason[:80])
            self._idx = (self._idx + 1) % len(self._proxies)
        return self.current()

    def as_proxies(self, proxy: str | None = None) -> dict[str, str] | None:
        """curl_cffi-shaped ``{"http":..,"https":..}`` for ``proxy`` or current."""
        p = proxy or self.current()
        return {"http": p, "https": p} if p else None

    def status(self) -> dict:
        now = time.time()
        return {
            "enabled": self.enabled,
            "count": self.count,
            "strategy": self._strategy.value,
            "current": mask_proxy(self.current()) if self.enabled else None,
            "proxies": [
                {
                    "proxy": mask_proxy(p),
                    "uses": self._uses.get(p, 0),
                    "deadForS": round(max(0.0, self._dead_until.get(p, 0.0) - now), 1),
                }
                for p in self._proxies
            ],
        }

    def reload(self) -> None:
        """Re-read settings after a runtime ``PROXY_LIST`` change."""
        with self._lock:
            self._proxies = list(settings.proxy_list)
            self._strategy = settings.proxy_rotation_strategy
            self._idx = 0
            self._uses = {p: 0 for p in self._proxies}
            self._dead_until = {}


# Singleton
proxy_pool = ProxyPool()
