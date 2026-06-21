"""Structured logging with structlog + rich console output."""

from __future__ import annotations

import logging
import sys

import structlog

from ..config.settings import settings


def setup_logging() -> None:
    """Configure structlog for the entire application.

    - Development: Rich console output with colours and pretty printing.
    - Production: JSON lines for machine consumption.
    """
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=False),
    ]

    if settings.is_production:
        # JSON output for production log aggregation
        shared_processors.append(structlog.processors.JSONRenderer())
    else:
        # Pretty console output for development
        shared_processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                pad_event=40,
            )
        )

    structlog.configure(
        processors=shared_processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # Also configure standard logging to route through structlog
    logging.basicConfig(
        format="%(message)s",
        level=logging.getLevelName(settings.log_level),
        handlers=[logging.StreamHandler(sys.stderr)],
        force=True,
    )


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """Get a bound logger instance.

    Usage::

        from src.utils.logger import get_logger
        log = get_logger(__name__)
        log.info("scrape_complete", symbol="RELIANCE", latency_ms=145.3)
    """
    logger = structlog.get_logger(name)
    return logger
