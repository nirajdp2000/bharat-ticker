"""Bharat Ticker — one-command launcher.

    python run.py

Does everything needed to bring the app up smoothly on a fresh machine:
  1. Ensures Python dependencies are installed (first run only).
  2. Starts the FastAPI server (zero external infra required — Redis/DB are
     optional; the app falls back to an in-process store automatically).
  3. Opens the dashboard in the browser once the server is ready.

No Redis, no TimescaleDB, no Docker needed to run. Live data comes straight
from NSE/BSE; Yahoo is used only for historical backfill.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
import urllib.request

HOST = os.environ.get("API_HOST", "127.0.0.1")
PORT = int(os.environ.get("API_PORT", "8000"))
URL = f"http://{HOST}:{PORT}/ui"


def ensure_deps() -> None:
    """Install project dependencies the first time only."""
    try:
        import fastapi, uvicorn, curl_cffi, yfinance, redis, sqlalchemy  # noqa: F401
        return
    except ImportError:
        print(">> Installing dependencies (first run, ~1-2 min)...")
        here = os.path.dirname(os.path.abspath(__file__))
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", "."], cwd=here)
        print(">> Dependencies installed.\n")


def open_when_ready() -> None:
    """Poll the server, then open the dashboard."""
    import webbrowser
    for _ in range(60):
        try:
            with urllib.request.urlopen(f"http://{HOST}:{PORT}/api/v1/ping", timeout=2) as r:
                if r.status == 200:
                    break
        except Exception:
            time.sleep(1)
    print(f"\n>> Bharat Ticker is live  ->  {URL}\n")
    try:
        webbrowser.open(URL)
    except Exception:
        pass


def main() -> None:
    ensure_deps()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    threading.Thread(target=open_when_ready, daemon=True).start()
    import uvicorn
    print(f">> Starting server on http://{HOST}:{PORT}  (Ctrl+C to stop)")
    uvicorn.run("src.main:app", host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
