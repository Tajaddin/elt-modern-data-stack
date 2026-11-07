"""The deploy workflow's .env step, run with bash the way the runner runs it."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "deploy.yml"

BASH = shutil.which("bash")
if BASH and Path(BASH).parent.name.lower() == "system32":
    BASH = None  # WSL's launcher: it does not pass the Windows environment through

# An htpasswd line as documented ($ escaped as $$) plus everything else a shell would act on.
TRICKY = "admin:$$2y$$05$$abc/def $HOME ${PATH} `id` $(id) %s \\n 'single' \"double\" #"


def _step(name: str) -> dict[str, Any]:
    steps = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]["deploy"]["steps"]
    return next(s for s in steps if s.get("name") == name)


def test_secrets_reach_the_script_only_through_env() -> None:
    step = _step("write .env")
    assert "${{" not in step["run"]


@pytest.mark.skipif(BASH is None, reason="needs bash")
def test_env_file_keeps_every_value_byte_for_byte(tmp_path: Path) -> None:
    step = _step("write .env")
    values = {name: f"{name}:{TRICKY}" for name in step["env"]}
    script = tmp_path / "step.sh"
    script.write_text(step["run"], encoding="utf-8", newline="\n")
    # The runner's default shell for a run step: bash -e <file>.
    assert BASH is not None
    subprocess.run([BASH, "-e", script.name], cwd=tmp_path, env={**os.environ, **values}, check=True)

    written = (tmp_path / "env.prod").read_text(encoding="utf-8").splitlines()
    for name, value in values.items():
        assert f"{name}={value}" in written, name
    assert "AIRFLOW_UID=1000" in written
