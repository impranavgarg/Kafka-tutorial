import json
from kafka import KafkaProducer
import os

# Ensure the folder path is correctly expanded
folderName = os.path.expanduser("./")

producer = KafkaProducer(
    bootstrap_servers="kafka-pranav-demo-impranavgarg-firstproject.k.aivencloud.com:26632",
    security_protocol="SSL",
    ssl_cafile=os.path.join(folderName, "ca.pem"),
    ssl_certfile=os.path.join(folderName, "service.cert"),
    ssl_keyfile=os.path.join(folderName, "service.key"),
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii')
)

# Send a message to the Kafka topic
producer.send(
    "test-topic",
    key={"key": 1},
    value={"message": "hello world"}
)

# Ensure all messages are sent
producer.flush()
