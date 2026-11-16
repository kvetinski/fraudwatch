import signal

import psycopg

from fraudwatch_ml.quarantine import InvalidLabel
from fraudwatch_ml.worker_runtime import (
    FailureClass,
    ShutdownController,
    classify_failure,
    install_shutdown_handlers,
)


def test_failure_policy_is_explicit():
    assert classify_failure(
        InvalidLabel("bad label")
    ) is FailureClass.DETERMINISTIC_DATA
    assert classify_failure(
        psycopg.OperationalError("database unavailable")
    ) is FailureClass.TRANSIENT_INFRASTRUCTURE
    assert classify_failure(
        TimeoutError("broker timeout")
    ) is FailureClass.TRANSIENT_INFRASTRUCTURE
    assert classify_failure(
        ValueError("model returned an invalid score")
    ) is FailureClass.FATAL_PROCESSING


def test_shutdown_handler_sets_flag_and_restores_previous_handler():
    controller = ShutdownController()
    previous = signal.getsignal(signal.SIGTERM)

    with install_shutdown_handlers(controller):
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        handler(signal.SIGTERM, None)
        assert controller.requested is True
        assert controller.signal_number == signal.SIGTERM

    assert signal.getsignal(signal.SIGTERM) == previous
