import config
from confluent_kafka import Consumer

class KafkaConsumer:
    def __init__(self):
        self.consumer = Consumer(config.config["kafka"])
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