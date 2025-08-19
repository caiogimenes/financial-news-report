import os
import psycopg2
from time import sleep
from confluent_kafka import Consumer
from postgres_writer import PostgresWriter
from finnhub_processor import ProcessFinnhubCompanyNews


def get_db_connection():
    sleep(15)  # Waits for docker db service to setup
    try:
        conn = psycopg2.connect(
            database=os.environ.get("POSTGRES_DB"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD"),
            host=os.environ.get("POSTGRES_HOST"),
            port=os.environ.get("POSTGRES_PORT"),
        )
        return conn

    except Exception as e:
        raise Exception(f"Could not connect to database due to {e}")


def consume_and_write():
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("BOOTSTRAP_SERVERS"),
        "security.protocol": os.environ.get("SECURITY_PROTOCOL"),
        "sasl.mechanisms": os.environ.get("SASL_MECHANISM"),
        "sasl.username": os.environ.get("SASL_USERNAME"),
        "sasl.password": os.environ.get("SASL_PASSWORD"),
        "client.id": os.environ.get("CLIENT_ID"),
        "group.id": os.environ.get("GROUP_ID")
    })
    consumer.subscribe([os.environ.get("SUBSCRIBE_TOPIC")])
    writer = PostgresWriter(conn=get_db_connection())

    try:
        while True:
            msg = consumer.poll(1)
            if msg and not msg.error():
                value = msg.value().decode()
                processor = ProcessFinnhubCompanyNews()
                new_insight = processor.transform(value)
                writer.write(new_insight, table="insights")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


if __name__ == "__main__":
    consume_and_write()
