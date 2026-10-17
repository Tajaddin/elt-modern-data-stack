"""End-to-end check through the real scheduler: trigger ghcn_daily over the REST API and wait for it.

    uv run python scripts/e2e_airflow_run.py [--base http://localhost:8080] [--timeout 1800]

Credentials come from AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD (the .env file is read if present).
Exits 0 when the DAG run succeeds, 1 otherwise; prints every task instance with state and duration.
Unlike `airflow dags test`, it exercises the executor, the pool and the Cosmos tasks as production does.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def token(client: httpx.Client, user: str, password: str) -> str:
    r = client.post("/auth/token", json={"username": user, "password": password})
    r.raise_for_status()
    return str(r.json()["access_token"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080"))
    ap.add_argument("--dag-id", default="ghcn_daily")
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--poll", type=int, default=15)
    args = ap.parse_args()
    _load_dotenv(Path(".env"))
    user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
    password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "")
    if not password:
        print("AIRFLOW_ADMIN_PASSWORD is not set", file=sys.stderr)
        return 2

    with httpx.Client(base_url=args.base, timeout=30.0) as c:
        c.headers["Authorization"] = f"Bearer {token(c, user, password)}"
        c.patch(f"/api/v2/dags/{args.dag_id}", json={"is_paused": False}).raise_for_status()
        run_id = f"e2e__{datetime.now(UTC):%Y%m%dT%H%M%S}"
        r = c.post(f"/api/v2/dags/{args.dag_id}/dagRuns", json={"dag_run_id": run_id, "logical_date": None})
        r.raise_for_status()
        print(f"triggered {args.dag_id} run {run_id}")
        t0 = time.time()
        state = "queued"
        while time.time() - t0 < args.timeout:
            time.sleep(args.poll)
            try:
                if time.time() - t0 > 600:  # tokens are short-lived; refresh for long runs
                    c.headers["Authorization"] = f"Bearer {token(c, user, password)}"
                state = str(c.get(f"/api/v2/dags/{args.dag_id}/dagRuns/{run_id}").json()["state"])
                tis = c.get(
                    f"/api/v2/dags/{args.dag_id}/dagRuns/{run_id}/taskInstances", params={"limit": 200}
                ).json()
            except (
                httpx.HTTPError,
                KeyError,
                ValueError,
            ) as exc:  # transient: api-server restart, token expiry
                print(f"  {int(time.time() - t0):4d}s api unavailable ({type(exc).__name__}); retrying")
                continue
            done = sum(
                1
                for t in tis["task_instances"]
                if t["state"] in ("success", "failed", "upstream_failed", "skipped")
            )
            print(f"  {int(time.time() - t0):4d}s state={state} tasks done {done}/{tis['total_entries']}")
            if state in ("success", "failed"):
                break
        tis = c.get(
            f"/api/v2/dags/{args.dag_id}/dagRuns/{run_id}/taskInstances", params={"limit": 200}
        ).json()
        rows = sorted(tis["task_instances"], key=lambda t: (t["start_date"] or "", t["task_id"]))
        print(f"\n{'task':52s} {'state':10s} {'seconds':>8s}")
        for t in rows:
            dur = f"{t['duration']:.1f}" if t.get("duration") is not None else ""
            print(f"{t['task_id']:52s} {t['state']!s:10s} {dur:>8s}")
        print(f"\nrun {run_id}: {state} after {int(time.time() - t0)} s")
        return 0 if state == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
