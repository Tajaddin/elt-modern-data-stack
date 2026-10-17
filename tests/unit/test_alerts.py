import io
import json
import logging
import urllib.request

import pytest

from ghcn_elt import alerts


class _TI:
    dag_id = "ghcn_daily"
    task_id = "extract_and_load"
    try_number = 2
    log_url = "http://airflow/log"


def test_failure_message_names_task_error_and_log() -> None:
    msg = alerts.build_failure_message(
        {"task_instance": _TI(), "run_id": "manual__1", "exception": ValueError("boom")}
    )
    assert "ghcn_daily.extract_and_load" in msg
    assert "run manual__1, try 2" in msg
    assert "ValueError: boom" in msg
    assert msg.endswith("http://airflow/log")


def test_failure_message_survives_missing_context() -> None:
    assert "?.?" in alerts.build_failure_message({})


def test_post_slack_rejects_non_https() -> None:
    with pytest.raises(ValueError, match="https"):
        alerts.post_slack("http://hooks.example.com/x", "hi")


def test_post_slack_sends_json_body(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    class _Resp(io.BytesIO):
        status = 200

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *a):  # type: ignore[no-untyped-def]
            return False

    def fake_urlopen(req, timeout):  # type: ignore[no-untyped-def]
        seen["url"] = req.full_url
        seen["body"] = json.loads(req.data.decode("utf-8"))
        seen["timeout"] = timeout
        return _Resp(b"ok")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert alerts.post_slack("https://hooks.slack.com/services/x", "text here") == 200
    assert seen == {
        "url": "https://hooks.slack.com/services/x",
        "body": {"text": "text here"},
        "timeout": 10.0,
    }


def test_notify_without_webhook_only_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with caplog.at_level(logging.WARNING):
        alerts.notify_failure({"task_instance": _TI(), "run_id": "r", "exception": RuntimeError("x")})
    assert any("alert logged only" in r.getMessage() for r in caplog.records)
    assert any("ghcn_daily.extract_and_load" in r.getMessage() for r in caplog.records)


def test_notify_swallows_webhook_errors(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/x")

    def boom(*a, **k):  # type: ignore[no-untyped-def]
        raise OSError("network down")

    monkeypatch.setattr(alerts, "post_slack", boom)
    with caplog.at_level(logging.ERROR):
        alerts.notify_failure({"task_instance": _TI(), "run_id": "r"})
    assert any("Slack notification failed" in r.getMessage() for r in caplog.records)
