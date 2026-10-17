import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip network tests unless `-m network` was given explicitly."""
    markexpr = config.getoption("-m", default="") or ""
    if "network" in markexpr:
        return
    skip = pytest.mark.skip(reason="network test; run with -m network")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip)
