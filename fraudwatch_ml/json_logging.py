"""Structured logging helpers shared by FraudWatch Python services."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os


_RESERVED = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def __init__(self, *, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        document = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "severity": record.levelname,
            "service": self.service,
            "build_revision": os.getenv("FRAUDWATCH_BUILD_REVISION", "development"),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                document[key] = value
        if record.exc_info:
            document["exception"] = self.formatException(record.exc_info)
        return json.dumps(document, default=str, separators=(",", ":"))


def configure_json_logging(service: str) -> None:
    """Replace root handlers with one unbuffered JSON stream handler."""
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(service=service))
    root.addHandler(handler)
    root.setLevel(os.getenv("FRAUDWATCH_LOG_LEVEL", "INFO").upper())
