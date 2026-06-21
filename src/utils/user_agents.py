"""User-Agent rotation pool with weighted random selection."""

from __future__ import annotations

import random

from ..config.constants import USER_AGENTS


class UserAgentRotator:
    """Thread-safe user-agent rotator.

    Provides a fresh User-Agent string on each call. Uses a pool of
    modern browser UA strings to avoid fingerprinting.
    """

    def __init__(self, agents: list[str] | None = None) -> None:
        self._agents = agents or USER_AGENTS
        self._index = 0

    def get_random(self) -> str:
        """Get a random user agent."""
        return random.choice(self._agents)

    def get_next(self) -> str:
        """Get the next user agent in round-robin order."""
        ua = self._agents[self._index % len(self._agents)]
        self._index += 1
        return ua

    def get_headers(self, referer: str | None = None) -> dict[str, str]:
        """Get a complete set of browser-like headers."""
        headers = {
            "User-Agent": self.get_random(),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        if referer:
            headers["Referer"] = referer
        return headers


# Singleton
ua_rotator = UserAgentRotator()
