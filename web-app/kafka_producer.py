import json
import os

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
)


KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "processed-messages",
)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

def send_message(message: dict):
    future = producer.send(
        KAFKA_TOPIC,
        value=message,
    )

    record_metadata = future.get(timeout=10)

    return {
        "topic": record_metadata.topic,
        "partition": record_metadata.partition,
        "offset": record_metadata.offset,
    }