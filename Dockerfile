# Extends the official Airflow image with the packages the pipeline needs:
# requests (GitHub API), psycopg2 (ingest + schema DDL), dbt-postgres (transform).
#
# Build is triggered by `build: .` in docker-compose.yml. Rebuild after
# changing requirements-airflow.txt:  docker compose build
FROM apache/airflow:3.3.1

COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir -r /requirements-airflow.txt
