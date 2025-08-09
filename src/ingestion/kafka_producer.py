from confluent_kafka import Producer

class KafkaProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'kafka_producer'
        })
        self.topic = topic

    def send_message(self, message):
        self.producer.produce(self.topic, value=message.encode('utf-8'))
        self.producer.flush()

    def close(self):
        self.producer.close()