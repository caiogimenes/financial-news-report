import os
from confluent_kafka import Consumer
from finnhub_processor import ProcessFinnhubCompanyNews

def consume():
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
    try:
        while True:
            msg = consumer.poll(1)
            if msg and not msg.error():
                value = msg.value().decode()
                processor = ProcessFinnhubCompanyNews()
                new_value = processor.transform(value)
                print(new_value)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume()
