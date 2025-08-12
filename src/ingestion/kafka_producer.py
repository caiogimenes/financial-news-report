from confluent_kafka import Producer
from config import config


class KafkaProducer:
    def __init__(self, topic):
        self.producer = Producer({
            'bootstrap.servers': config['kafka']['KAFKA_BOOTSTRAP_SERVERS'],
            'security.protocol': config['kafka']['security.protocol'],
            'sasl.mechanisms': config['kafka']['sasl.mechanisms'],
            'sasl.username': config['kafka']['sasl.username'],
            'sasl.password': config['kafka']['sasl.password'],
            'client.id': config['kafka']['client.id'],
        })
        self.topic = 'raw_data' if not topic else topic

    def send_data(self, data):
        if not isinstance(data, str):
            try:
                data = str(data)
            except ValueError as e:
                raise ValueError(f"{e}. Check if data is string type")
        self.producer.produce(self.topic, value=data.encode('utf-8'))
        self.producer.flush()
