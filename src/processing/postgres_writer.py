import os
import psycopg2
from insight import Insight
import logging

INSERT_INSIGHT_QUERY = """
INSERT INTO {table} (datetime, company, url, sentiment, score, ner)
VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
"""

class PostgresWriter:
    def __init__(self, conn):
        self.conn = conn

    def write(self, insight: Insight, table) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(
                INSERT_INSIGHT_QUERY.format(table=table),
                (
                    insight.datetime,
                    insight.company,
                    insight.url,
                    insight.sentiment,
                    insight.score,
                    insight.ner
                )
            )
            new_id = cursor.fetchone()
            self.conn.commit()

        return new_id[0]
