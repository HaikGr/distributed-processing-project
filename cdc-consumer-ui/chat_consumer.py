import json
import os
import threading
from typing import Callable

from confluent_kafka import Consumer, KafkaException


KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_GROUP_ID = os.environ["KAFKA_GROUP_ID"]


class ChatConsumer:
    def __init__(self, on_message: Callable[[dict], None]):
        self.on_message = on_message

        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

        self.running = False
        self.thread = None

    def start(self):
        self.running = True

        self.consumer.subscribe([KAFKA_TOPIC])

        self.thread = threading.Thread(
            target=self._consume,
            daemon=True,
        )

        self.thread.start()

    def _consume(self):
        while self.running:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                self.on_message(value)
            except Exception as exc:
                print(f"Failed to process Kafka message: {exc}")

    def stop(self):
        self.running = False

        if self.thread:
            self.thread.join(timeout=2)

        self.consumer.close()