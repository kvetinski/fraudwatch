import json
import logging

from fraudwatch_ml.json_logging import JsonFormatter


def test_json_formatter_adds_service_build_and_context(monkeypatch):
    monkeypatch.setenv("FRAUDWATCH_BUILD_REVISION", "abc123")
    record = logging.LogRecord(
        name="fraudwatch.test",
        level=logging.WARNING,
        pathname=__file__,
        lineno=10,
        msg="record %s",
        args=("quarantined",),
        exc_info=None,
    )
    record.source_partition = 2
    record.outcome = "quarantined"

    document = json.loads(JsonFormatter(service="scoring").format(record))

    assert document["service"] == "scoring"
    assert document["build_revision"] == "abc123"
    assert document["severity"] == "WARNING"
    assert document["message"] == "record quarantined"
    assert document["source_partition"] == 2
    assert document["outcome"] == "quarantined"
