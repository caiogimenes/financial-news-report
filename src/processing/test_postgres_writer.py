import os
import pytest
import psycopg2
from postgres_writer import PostgresWriter
from insight import Insight

TEST_INSIGHT = Insight(
    datetime="2025-01-01",
    company="APPL",
    fulltext="a" * 500,  # 500 characters length text
    url="www.test.com",
    sentiment="Positive",
    score=0.95,
    ner=""
)
TABLE = "tests"


@pytest.fixture
def db_connection(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "postgres")
    monkeypatch.setenv("POSTGRES_USER", "caiogimenes")
    monkeypatch.setenv("POSTGRES_PASSWORD", "financialreport")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS {table} (
        id SERIAL PRIMARY KEY,
        datetime VARCHAR(100) NOT NULL,
        url VARCHAR(100) NOT NULL,
        sentiment VARCHAR(100) NOT NULL,
        score FLOAT NOT NULL,
        ner VARCHAR(100) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """.format(table=TABLE)

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            database=os.environ.get("POSTGRES_DB"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD"),
            host=os.environ.get("POSTGRES_HOST"),
            port=os.environ.get("POSTGRES_PORT"),
        )
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()

        yield conn

    finally:
        if conn and cursor:
            cursor.execute("""
            DROP TABLE {table};
            """.format(table=TABLE)
            )
            conn.commit()
            cursor.close()
            conn.close()


def test_write(db_connection):
    writer = PostgresWriter(db_connection)
    id = writer.write(TEST_INSIGHT, TABLE)

    assert id == 1

