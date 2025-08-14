SUBSCRIBE_TOPIC = "raw_data"
WRITE_TOPIC = "processed_insights"
HF_API_KEY = ""
config = {
    'kafka': {
        'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'F37GT4XKKXJBHEB2',
        'sasl.password': 'cflt41gl/PVZhGODfCAgGY37yuGEKiQmF9aAddAdXpIK9SVryNFWSbE4TNBVC6lg',
        "client.id": "ccloud-python-client-1c103f18-0d03-4cbd-aae9-c8e2cb956593",
        "group.id": "python-group-1",
        "auto.offset.reset": "earliest",
    }
}