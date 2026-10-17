"""Failure notifications for the Airflow DAG.

Slack incoming webhook when SLACK_WEBHOOK_URL is set; always logged.

Lives in the package rather than next to the DAG so the dag-processor, the task runner and the
test suite all import the same code without sys.path tricks.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

log = logging.getLogger(__name__)


def build_failure_message(context: dict[str, Any]) -> str:
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_id = getattr(ti, "dag_id", None) or getattr(dag, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id", "?")
    try_number = getattr(ti, "try_number", "?")
    exc = context.get("exception")
    url = getattr(ti, "log_url", "") or ""
    what = f"{type(exc).__name__}: {str(exc)[:300]}" if exc else "failure"
    head = f":rotating_light: Airflow task failed: *{dag_id}.{task_id}* (run {run_id}, try {try_number})"
    return f"{head}\n{what}\n{url}"


def post_slack(webhook_url: str, text: str, timeout: float = 10.0) -> int:
    if not webhook_url.startswith("https://"):
        raise ValueError("SLACK_WEBHOOK_URL must be an https URL")
    body = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(  # noqa: S310  (scheme checked above)
        webhook_url, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        return int(resp.status)


def notify_failure(context: dict[str, Any]) -> None:
    text = build_failure_message(context)
    log.error("ghcn_daily failure: %s", text)
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        log.warning("SLACK_WEBHOOK_URL not set; alert logged only")
        return
    try:
        post_slack(webhook, text)
    except Exception:
        log.exception("Slack notification failed")
