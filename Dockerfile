# Bharat Ticker — lightweight container for Northflank / Render / Fly / any
# persistent-process host. Runs the FastAPI server as a long-lived process so
# the live sampler, SSE stream, in-memory cache and warm archives stay alive
# (0 degradation). NOT for serverless — this app needs a persistent process.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    API_HOST=0.0.0.0

WORKDIR /app

# ca-certificates: outbound HTTPS to Groww/Tickertape/BSE/Moneycontrol.
# curl: container HEALTHCHECK only.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Install deps first (better layer caching). README.md is referenced by pyproject.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

# Run as non-root.
RUN useradd -m app && chown -R app /app
USER app

# Northflank injects $PORT; default 8000 for local `docker run`.
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${PORT:-8000}/api/v1/ping" || exit 1

CMD ["sh", "-c", "uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
