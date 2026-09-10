"""
test.py - quick Postgres connectivity check.

    python test.py

Reads DB_* from .env. Prints the server version and the github.* tables it
can see, or the connection error.
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    sslmode=os.environ.get("DB_SSLMODE", "require"),
    connect_timeout=10,
)

with conn.cursor() as cur:
    cur.execute("SELECT version();")
    print(cur.fetchone()[0])
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'github' ORDER BY table_name;"
    )
    print("github tables:", [r[0] for r in cur.fetchall()])

conn.close()
print("Connection successful")
