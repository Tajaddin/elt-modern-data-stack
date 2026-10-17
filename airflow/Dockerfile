# Airflow 3.3 image with the extractor installed in Airflow's environment and dbt in its own
# virtualenv (Cosmos runs it by path), so the two dependency trees never fight.
FROM apache/airflow:3.3.2-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /opt/dbt-venv && chown airflow:root /opt/dbt-venv
USER airflow

# dbt in an isolated venv (owned by the airflow user; /opt itself is root-owned)
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core>=1.12,<1.13" "dbt-duckdb>=1.11,<1.12" \
      "dbt-databricks>=1.12,<1.13" "dbt-snowflake>=1.12,<1.13"

# the extractor and Cosmos in Airflow's environment
COPY --chown=airflow:root pyproject.toml README.md /opt/ghcn/
COPY --chown=airflow:root src /opt/ghcn/src
RUN pip install --no-cache-dir /opt/ghcn "astronomer-cosmos>=1.15,<2" "pytest>=8.4,<9"

ENV DBT_PROJECT_DIR=/opt/airflow/dbt \
    DBT_PROFILES_DIR=/opt/airflow/dbt \
    DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt \
    GHCN_DATA_DIR=/opt/airflow/data \
    GHCN_DUCKDB_PATH=/opt/airflow/data/warehouse.duckdb
