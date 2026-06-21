"""TLS fingerprint configuration for curl_cffi browser impersonation."""

from __future__ import annotations

import random

from ..config.constants import IMPERSONATE_TARGETS


class FingerprintManager:
    """Manages TLS/JA3 fingerprint impersonation targets for curl_cffi.

    curl_cffi can impersonate real browser TLS handshakes.  This manager
    rotates through available impersonation profiles to reduce the chance
    of fingerprint-based blocking.
    """

    def __init__(self, targets: list[str] | None = None) -> None:
        self._targets = targets or IMPERSONATE_TARGETS
        self._index = 0

    def get_random(self) -> str:
        """Get a random impersonation target."""
        return random.choice(self._targets)

    def get_next(self) -> str:
        """Get the next target in round-robin order."""
        target = self._targets[self._index % len(self._targets)]
        self._index += 1
        return target

    @property
    def available_targets(self) -> list[str]:
        return list(self._targets)


# Singleton
fingerprint_mgr = FingerprintManager()
