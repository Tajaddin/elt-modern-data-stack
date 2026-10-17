"""Structured JSON logging. One line per event, machine-readable, with timestamps in UTC."""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    # force=True rebinds the stdlib handler to the current stdout on every call (test runners swap it).
    logging.basicConfig(level=level, stream=sys.stdout, format="%(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # its per-request INFO line is not JSON
    renderer: structlog.types.Processor
    if json_output:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        # No file argument and no caching: stdout is resolved at log time, not at import time.
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]
