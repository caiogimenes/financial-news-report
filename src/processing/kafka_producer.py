import os
from confluent_kafka import Producer


class KafkaProducer:
    def __init__(self, topic):
        self.producer = Producer({
            "bootstrap.servers": os.environ.get("BOOTSTRAP_SERVERS"),
            "security.protocol": os.environ.get("SECURITY_PROTOCOL"),
            "sasl.mechanisms": os.environ.get("SASL_MECHANISM"),
            "sasl.username": os.environ.get("SASL_USERNAME"),
            "sasl.password": os.environ.get("SASL_PASSWORD"),
            "client.id": os.environ.get("CLIENT_ID"),
            "group.id": os.environ.get("GROUP_ID")
        })
        self.topic = os.environ.get("WRITE_TOPIC") if not topic else topic

    def send_data(self, data):
        if not isinstance(data, str):
            try:
                data = str(data)
            except ValueError as e:
                raise ValueError(f"{e}. Check if data is string type")
        self.producer.produce(self.topic, value=data.encode('utf-8'))
        self.producer.flush()
