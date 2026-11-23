import json
import urllib.parse

from scripts import smoke_k8s


def test_loki_transaction_query_uses_bounded_application_log_selector(monkeypatch):
    requested_url = ""

    def fake_get(url: str, *, timeout: float = 3) -> str:
        nonlocal requested_url
        requested_url = url
        return json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [{"stream": {}, "values": [["1", "line"]]}],
                },
            }
        )

    monkeypatch.setattr(smoke_k8s, "get", fake_get)
    monkeypatch.setattr(smoke_k8s.time, "time_ns", lambda: 600_000_000_000)

    assert smoke_k8s.loki_has_transaction_log("http://loki", "k8s-smoke-123")

    parsed = urllib.parse.urlparse(requested_url)
    params = urllib.parse.parse_qs(parsed.query)
    assert parsed.path == "/loki/api/v1/query_range"
    assert params["query"] == [
        '{namespace="fraudwatch", service_name="ingestion"} |= "k8s-smoke-123"'
    ]
    assert params["start"] == ["300000000000"]
    assert params["end"] == ["600000000000"]
    assert params["limit"] == ["20"]


def test_loki_transaction_query_reports_no_matching_streams(monkeypatch):
    monkeypatch.setattr(
        smoke_k8s,
        "get",
        lambda *_args, **_kwargs: json.dumps(
            {
                "status": "success",
                "data": {"resultType": "streams", "result": []},
            }
        ),
    )

    assert not smoke_k8s.loki_has_transaction_log("http://loki", "missing")
