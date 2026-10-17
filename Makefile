# Linux/macOS/CI entry points. On Windows without make, run the uv commands shown in the README.
UV ?= uv

.PHONY: install lint fmt typecheck test test-net fixture run measure clean

install:
	$(UV) sync --all-extras

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

typecheck:
	$(UV) run mypy

test:
	$(UV) run pytest --cov --cov-report=term-missing --cov-report=xml

test-net:
	$(UV) run pytest -m network

fixture:
	$(UV) run python scripts/make_fixture.py

run:
	$(UV) run ghcn-elt run $(if $(RUN_DATE),--run-date $(RUN_DATE),)

measure:
	$(UV) run ghcn-elt measure $(if $(RUN_DATE),--run-date $(RUN_DATE),)

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov coverage.xml
