import json
import os
import threading
from collections import deque
from typing import Any

from confluent_kafka import Consumer, KafkaException
from fastapi import FastAPI
from fastapi.responses import HTMLResponse


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "postgres.public.requests",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "requests-web-ui-v1",
)

MAX_MESSAGES = int(
    os.getenv("MAX_MESSAGES", "100")
)


app = FastAPI()

messages = deque(maxlen=MAX_MESSAGES)


consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,

    # For the first run of this consumer group,
    # read existing Kafka messages.
    "auto.offset.reset": "earliest",

    # Automatically commit offsets.
    "enable.auto.commit": True,
})


def consume_messages() -> None:
    consumer.subscribe([KAFKA_TOPIC])

    print(
        f"Kafka consumer started. "
        f"topic={KAFKA_TOPIC}, "
        f"bootstrap={KAFKA_BOOTSTRAP_SERVERS}, "
        f"group={KAFKA_GROUP_ID}"
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                value = json.loads(
                    msg.value().decode("utf-8")
                )

                event = {
                    "operation": value.get("op"),
                    "before": value.get("before"),
                    "after": value.get("after"),
                    "source": value.get("source"),
                    "received_at": value.get("ts_ms"),
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                }

                messages.appendleft(event)

                print(
                    f"Received CDC event: "
                    f"op={event['operation']} "
                    f"offset={event['offset']}"
                )

            except Exception as exc:
                print(
                    f"Failed to process Kafka message: {exc}"
                )

    except Exception as exc:
        print(f"Consumer stopped: {exc}")

    finally:
        consumer.close()


consumer_thread = threading.Thread(
    target=consume_messages,
    daemon=True,
)

consumer_thread.start()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/messages")
def get_messages() -> list[dict[str, Any]]:
    return list(messages)


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """
<!DOCTYPE html>
<html>
<head>
    <title>PostgreSQL CDC Monitor</title>

    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            background: #f4f6f8;
        }

        header {
            background: #222;
            color: white;
            padding: 20px 30px;
        }

        h1 {
            margin: 0;
        }

        #events {
            padding: 20px;
        }

        .event {
            background: white;
            margin-bottom: 15px;
            padding: 18px;
            border-radius: 8px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.08);
        }

        .operation {
            font-weight: bold;
            font-size: 18px;
            margin-bottom: 10px;
        }

        .snapshot {
            color: #777;
        }

        pre {
            background: #f0f0f0;
            padding: 12px;
            overflow-x: auto;
            border-radius: 5px;
        }
    </style>
</head>

<body>

<header>
    <h1>PostgreSQL → Debezium → Kafka</h1>
    <div>Topic: postgres.public.requests</div>
</header>

<div id="events">
    Loading...
</div>

<script>

function operationName(op) {
    switch (op) {
        case "c":
            return "INSERT";

        case "u":
            return "UPDATE";

        case "d":
            return "DELETE";

        case "r":
            return "SNAPSHOT";

        default:
            return op || "UNKNOWN";
    }
}


function render(messages) {

    const container = document.getElementById("events");

    if (messages.length === 0) {
        container.innerHTML = "<p>No messages received yet.</p>";
        return;
    }

    container.innerHTML = messages.map(event => {

        const operation = operationName(
            event.operation
        );

        return `
            <div class="event">

                <div class="operation">
                    ${operation}
                </div>

                <div>
                    <strong>Offset:</strong>
                    ${event.offset}
                </div>

                <div>
                    <strong>Partition:</strong>
                    ${event.partition}
                </div>

                <h3>Before</h3>

                <pre>${JSON.stringify(
                    event.before,
                    null,
                    2
                )}</pre>

                <h3>After</h3>

                <pre>${JSON.stringify(
                    event.after,
                    null,
                    2
                )}</pre>

            </div>
        `;

    }).join("");
}


async function loadMessages() {

    try {

        const response = await fetch(
            "/api/messages"
        );

        const data = await response.json();

        render(data);

    } catch (error) {

        console.error(error);

    }
}


loadMessages();

setInterval(
    loadMessages,
    1000
);

</script>

</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
    )