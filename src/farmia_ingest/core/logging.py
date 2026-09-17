"""Logging estructurado en JSON.

Un log por línea en formato JSON facilita parsearlo y explotarlo después. No depende de Spark.
"""
from __future__ import annotations

import json
import logging
import sys
from typing import Any


class JsonFormatter(logging.Formatter):
    """Formatea cada registro de log como una línea JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Campos extra inyectados vía logger.info(..., extra={"context": {...}})
        context = getattr(record, "context", None)
        if isinstance(context, dict):
            payload.update(context)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def get_logger(name: str = "farmia_ingest", level: int = logging.INFO) -> logging.Logger:
    """Devuelve un logger con salida JSON a stdout (idempotente: no duplica handlers)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, msg: str, **context: Any) -> None:
    """Atajo para emitir un evento con contexto estructurado."""
    logger.info(msg, extra={"context": context})
