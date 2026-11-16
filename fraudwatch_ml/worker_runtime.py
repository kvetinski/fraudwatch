"""Shared failure and shutdown policy for Kafka workers."""

from __future__ import annotations

import signal
from contextlib import contextmanager
from enum import StrEnum
from types import FrameType
from typing import Iterator

import psycopg
from confluent_kafka import KafkaException

from fraudwatch_ml.quarantine import QuarantinableEvent


class FailureClass(StrEnum):
    DETERMINISTIC_DATA = "deterministic_data"
    TRANSIENT_INFRASTRUCTURE = "transient_infrastructure"
    FATAL_PROCESSING = "fatal_processing"


def classify_failure(error: BaseException) -> FailureClass:
    """Classify policy without changing the offset-safety behavior."""
    if isinstance(error, QuarantinableEvent):
        return FailureClass.DETERMINISTIC_DATA
    if isinstance(
        error,
        (psycopg.Error, KafkaException, ConnectionError, TimeoutError),
    ):
        return FailureClass.TRANSIENT_INFRASTRUCTURE
    return FailureClass.FATAL_PROCESSING


class ShutdownController:
    """Signal-safe cooperative stop flag.

    The handler only mutates Python attributes. Worker loops inspect the flag
    after poll and after finishing the current database/offset commit boundary.
    """

    def __init__(self) -> None:
        self.requested = False
        self.signal_number: int | None = None

    def request(
        self,
        signal_number: int,
        _frame: FrameType | None = None,
    ) -> None:
        self.requested = True
        self.signal_number = signal_number


@contextmanager
def install_shutdown_handlers(
    controller: ShutdownController,
) -> Iterator[None]:
    """Install SIGINT/SIGTERM handlers and restore the previous handlers."""
    previous = {
        signal_number: signal.getsignal(signal_number)
        for signal_number in (signal.SIGINT, signal.SIGTERM)
    }
    for signal_number in previous:
        signal.signal(signal_number, controller.request)
    try:
        yield
    finally:
        for signal_number, handler in previous.items():
            signal.signal(signal_number, handler)
