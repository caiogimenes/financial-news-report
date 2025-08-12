import logging
import config
from time import sleep
from base import Connector
from kafka_producer import KafkaProducer
from finnhub_connector import FinnhubConnector


class Ingestor:
    @staticmethod
    def run(conn: Connector, symbols: list, producer: KafkaProducer):
        for symbol in symbols:
            for data in conn.fetch_company_news(
                    symbol=symbol,
            ):
                producer.send_data(data) # Register at Kafka Topic


if __name__ == "__main__":
    logging.basicConfig()

    conn = FinnhubConnector()
    producer = KafkaProducer(topic=config.TOPIC)
    while True:
        Ingestor.run(conn, config.symbols, producer)
        sleep(15)
