import os

import pytest

from ghcn_elt import tracing


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip network tests unless `-m network` was given explicitly."""
    markexpr = config.getoption("-m", default="") or ""
    if "network" in markexpr:
        return
    skip = pytest.mark.skip(reason="network test; run with -m network")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def _hermetic_tracing(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Tests never export spans anywhere: no OTLP endpoint, and a fresh (no-op) provider per test."""
    for key in list(os.environ):
        if key.startswith("OTEL_"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(tracing, "_provider", None)
    yield
