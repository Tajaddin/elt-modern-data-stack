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

# ---- Airflow (docker compose) ----
COMPOSE = docker compose -f airflow/docker-compose.yml --env-file .env

airflow-build:
	$(COMPOSE) build

airflow-up:
	$(COMPOSE) up -d

airflow-up-all:
	$(COMPOSE) --profile observability up -d

airflow-down:
	$(COMPOSE) --profile observability down

airflow-logs:
	$(COMPOSE) logs -f --tail=100 airflow-scheduler airflow-apiserver

airflow-e2e:
	$(UV) run python scripts/e2e_airflow_run.py

dag-tests:
	$(COMPOSE) run --rm airflow-cli python -m pytest /opt/airflow/dag_tests -q -p no:cacheprovider

# ---- VM (Terraform, Oracle Cloud Always Free) ----
TF = terraform -chdir=infra/oci

deploy:
	$(TF) init -input=false
	$(TF) apply -input=false
	@echo "Now point DNS at the IP above and run the GitHub 'deploy' workflow (see docs/DEPLOY.md)."

destroy:
	$(TF) destroy -input=false

tf-validate:
	docker run --rm -v "$(CURDIR)/infra/oci:/w" -w /w hashicorp/terraform:1.9 fmt -check -diff
	docker run --rm -v "$(CURDIR)/infra/oci:/w" -w /w hashicorp/terraform:1.9 init -backend=false -input=false
	docker run --rm -v "$(CURDIR)/infra/oci:/w" -w /w hashicorp/terraform:1.9 validate

alerts-test:
	docker run --rm -v "$(CURDIR)/airflow/observability:/t:ro" -w /t --entrypoint promtool prom/prometheus:v3.5.0 check rules /t/alert_rules.yml
	docker run --rm -v "$(CURDIR)/airflow/observability:/t:ro" -w /t --entrypoint promtool prom/prometheus:v3.5.0 test rules /t/alerts_test.yml
