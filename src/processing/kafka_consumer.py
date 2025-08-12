import config
from confluent_kafka import Consumer

class KafkaConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': config.config['kafka']['bootstrap.servers'],
            'security.protocol': config.config['kafka']['security.protocol'],
            'sasl.mechanisms': config.config['kafka']['sasl.mechanisms'],
            'sasl.username': config.config['kafka']['sasl.username'],
            'sasl.password': config.config['kafka']['sasl.password'],
            'client.id': config.config['kafka']['client.id'],
            'group.id': config.config["kafka"]['python-group-1'],
            'auto.offset.reset': config.config["kafka"]['earliest'],
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