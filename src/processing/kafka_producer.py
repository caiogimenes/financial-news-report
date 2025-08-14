from confluent_kafka import Producer
import config


class KafkaProducer:
    def __init__(self, topic):
        self.producer = Producer(config.config["kafka"])
        self.topic = config.WRITE_TOPIC if not topic else topic

    def send_data(self, data):
        if not isinstance(data, str):
            try:
                data = str(data)
            except ValueError as e:
                raise ValueError(f"{e}. Check if data is string type")
        self.producer.produce(self.topic, value=data.encode('utf-8'))
        self.producer.flush()
