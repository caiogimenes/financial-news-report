from config import config
from confluent_kafka import Consumer

class KafkaConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': config['kafka']['KAFKA_BOOTSTRAP_SERVERS'],
            'security.protocol': config['kafka']['security.protocol'],
            'sasl.mechanisms': config['kafka']['sasl.mechanisms'],
            'sasl.username': config['kafka']['sasl.username'],
            'sasl.password': config['kafka']['sasl.password'],
            'client.id': config['kafka']['client.id'],
            'group.id': 'python-group-1',
            'auto.offset.reset': 'earliest'
        })
        self.topic = None

    def consume(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    yield msg.value().decode('utf-8')
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def set_topic(self, topic):
        self.topic = topic
        self.consumer.subscribe([self.topic])
        return