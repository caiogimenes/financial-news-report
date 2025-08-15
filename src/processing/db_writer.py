import os
import psycopg2

class PostgresWriter:
    def __init__(self, conn):
        self.conn = psycopg2.connect(
            database=os.environ.get("db_name"),
            user=os.environ.get("db_name"),
            password=os.environ.get("db_name"),
            host=os.environ.get("db_name"),
            port=os.environ.get("db_name"),
        )

    def write(self):
        pass