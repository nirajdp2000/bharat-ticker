"""Event-loop watchdog — self-heal a wedged/overloaded process.

When the asyncio event loop stops making progress (deadlock, exhausted thread
pool, an upstream stall that backs everything up — the kind of "server is up but
answers nothing" overload that took this service offline), a normal in-loop check
can't fire because the loop itself is stuck. So this watchdog runs in a SEPARATE
OS thread: it periodically asks the loop to run a trivial callback and, if the
loop fails to run it for `wedge_s`, force-exits the process.

On Northflank (and any container host with the default restart-on-exit policy)
a process exit triggers an automatic restart of a fresh container — so this is a
self-healing redeploy that needs no API token, no external trigger, and works
even when the app is too wedged to help itself.

Opt-out with WATCHDOG_ENABLED=false. Tune with WATCHDOG_WEDGE_S / WATCHDOG_INTERVAL_S.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time

from .logger import get_logger

log = get_logger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def start_event_loop_watchdog(loop: asyncio.AbstractEventLoop) -> threading.Thread | None:
    """Start the watchdog thread for `loop`. Returns the thread (or None if disabled)."""
    if not _env_bool("WATCHDOG_ENABLED", True):
        log.info("watchdog_disabled")
        return None

    interval_s = max(2.0, float(os.environ.get("WATCHDOG_INTERVAL_S", "10")))
    # Default 60s: well past any legitimate slow upstream batch, so only a true
    # wedge (not mere slowness) trips it. Min 20s as a safety floor.
    wedge_s = max(20.0, float(os.environ.get("WATCHDOG_WEDGE_S", "60")))

    last_beat = {"t": time.monotonic()}

    def _beat() -> None:
        last_beat["t"] = time.monotonic()

    def _run() -> None:
        # Prime one beat, then poll. Each cycle: a healthy loop will have run the
        # previously-scheduled _beat during our sleep, keeping `stale` ~= interval.
        try:
            loop.call_soon_threadsafe(_beat)
        except Exception:  # noqa: BLE001
            pass
        while True:
            time.sleep(interval_s)
            stale = time.monotonic() - last_beat["t"]
            if stale > wedge_s:
                log.error("watchdog_wedge_detected_exiting", stale_s=round(stale, 1),
                          wedge_s=wedge_s)
                # Flush logs, then hard-exit so the container restarts cleanly.
                os._exit(1)
            try:
                loop.call_soon_threadsafe(_beat)
            except Exception:  # noqa: BLE001
                # Loop closed (graceful shutdown) — stop watching.
                return

    t = threading.Thread(target=_run, name="loop-watchdog", daemon=True)
    t.start()
    log.info("watchdog_started", interval_s=interval_s, wedge_s=wedge_s)
    return t
