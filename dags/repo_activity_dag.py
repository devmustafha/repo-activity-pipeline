"""
repo_activity_dag.py

Hourly incremental ingestion of GitHub repository activity, then dbt.

    apply_github_schema     idempotent DDL on RDS (github.*)
      -> ingest_github      GitHub REST API -> github.* (paginated, incremental,
                            rate-limit-aware, idempotent upserts)
      -> dbt_deps           install dbt packages (dbt_utils)
      -> dbt_build          run + test the staging views and the
                            mart_repo_activity_daily mart

Hourly rather than daily: the GitHub source is a live API with a rate-limit
budget and a moving watermark, so it benefits from small, frequent pulls.

Config comes entirely from environment variables (docker-compose.yml + .env):

    PYTHONPATH            /opt/airflow/repo   (so `github_activity` imports)
    GITHUB_TOKEN          GitHub PAT / fine-grained token
    GITHUB_REPOS          comma-separated owner/repo list
    GITHUB_SCHEMA_SQL     path to sql/github_schema.sql
    DBT_PROJECT_DIR / DBT_PROFILES_DIR
    DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

REPO_DIR = os.environ.get("PYTHONPATH", "/opt/airflow/repo")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", f"{REPO_DIR}/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
GITHUB_SCHEMA_SQL = os.environ.get(
    "GITHUB_SCHEMA_SQL", f"{REPO_DIR}/sql/github_schema.sql"
)

_DBT_DIRS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"


def dbt_command(subcommand: str) -> str:
    return f"dbt --no-use-colors {subcommand} {_DBT_DIRS}"


def apply_github_schema() -> None:
    """Run the idempotent github DDL against RDS."""
    import psycopg2

    with open(GITHUB_SCHEMA_SQL) as f:
        ddl = f.read()

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=15,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(ddl)
    finally:
        conn.close()


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="repo_activity",
    description="Incrementally ingest GitHub repo activity into RDS, transform with dbt.",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["github", "elt", "dbt", "portfolio"],
) as dag:

    apply_schema = PythonOperator(
        task_id="apply_github_schema",
        python_callable=apply_github_schema,
    )

    ingest = BashOperator(
        task_id="ingest_github",
        bash_command=f"cd {REPO_DIR} && python -m github_activity.ingest",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=dbt_command("build"),
    )

    apply_schema >> ingest >> dbt_deps >> dbt_build
