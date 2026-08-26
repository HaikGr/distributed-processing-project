import json
import os
import threading
from collections import deque
from typing import Any

from confluent_kafka import Consumer, Producer
from fastapi import FastAPI,  HTTPException
from fastapi.responses import HTMLResponse
from chat_postgres import create_message
from pydantic import BaseModel

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "chat-messages",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "app2-chat",
)

MAX_MESSAGES = int(
    os.getenv("MAX_MESSAGES", "100")
)

APP_ID = os.getenv(
    "APP_ID",
    "app2",
)

app = FastAPI()

messages = deque(maxlen=MAX_MESSAGES)


consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})

typing_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
})

class MessageRequest(BaseModel):
    conversation_id: str
    content: str


@app.post("/messages")
def send_message(message: MessageRequest):
    try:
        return create_message(
            conversation_id=message.conversation_id,
            sender="app2",
            receiver="app1",
            content=message.content,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create message: {exc}",
        )


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

                after = value.get("after")

                if not after:
                    continue

                event = {
                    "id": after.get("id"),
                    "conversation_id": after.get("conversation_id"),
                    "sender": after.get("sender"),
                    "receiver": after.get("receiver"),
                    "content": after.get("content"),
                    "created_at": after.get("created_at"),
                    "operation": value.get("op"),
                }

                messages.appendleft(event)

                print(
                    f"Chat message received: "
                    f"sender={event['sender']} "
                    f"receiver={event['receiver']} "
                    f"content={event['content']!r}"
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
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>App 2 Chat</title>

    <style>
        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            background: #f3f4f6;
            font-family: Arial, sans-serif;
        }

        .chat {
            width: min(800px, 95vw);
            height: 80vh;
            background: white;
            border-radius: 16px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.15);
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        .header {
            padding: 18px 24px;
            background: #111827;
            color: white;
        }

        .header h1 {
            margin: 0;
            font-size: 20px;
        }

        .header p {
            margin: 5px 0 0;
            font-size: 13px;
            opacity: 0.7;
        }

        #messages {
            flex: 1;
            overflow-y: auto;
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }

        .message {
            max-width: 70%;
            padding: 10px 14px;
            border-radius: 14px;
            line-height: 1.4;
        }

        .mine {
            align-self: flex-end;
            background: #dbeafe;
        }

        .theirs {
            align-self: flex-start;
            background: #f3f4f6;
        }

        .sender {
            font-size: 11px;
            opacity: 0.6;
            margin-bottom: 3px;
        }

        .content {
            font-size: 15px;
            word-break: break-word;
        }

        .composer {
            display: flex;
            gap: 10px;
            padding: 15px;
            border-top: 1px solid #e5e7eb;
        }

        #messageInput {
            flex: 1;
            padding: 12px 14px;
            border: 1px solid #d1d5db;
            border-radius: 10px;
            outline: none;
            font-size: 15px;
        }

        button {
            border: 0;
            padding: 12px 20px;
            border-radius: 10px;
            cursor: pointer;
            font-size: 15px;
        }
    </style>
</head>

<body>

<div class="chat">

    <div class="header">
        <h1>App 2 Chat</h1>
        <p>PostgreSQL → Debezium → Kafka</p>
    </div>

    <div id="messages"></div>

    <div class="composer">
        <input
            id="messageInput"
            type="text"
            placeholder="Type a message..."
        />
        <button onclick="sendMessage()">Send</button>
    </div>

</div>

<script>
    const messagesElement =
        document.getElementById("messages");

    const input =
        document.getElementById("messageInput");

    let displayedIds = new Set();

    function renderMessage(message) {
        if (displayedIds.has(message.id)) {
            return;
        }

        displayedIds.add(message.id);

        const wrapper =
            document.createElement("div");

        wrapper.className =
            "message " +
            (message.sender === "app2"
                ? "mine"
                : "theirs");

        wrapper.innerHTML = `
            <div class="sender">
                ${message.sender}
            </div>

            <div class="content"></div>
        `;

        wrapper.querySelector(".content").textContent =
            message.content;

        messagesElement.appendChild(wrapper);

        messagesElement.scrollTop =
            messagesElement.scrollHeight;
    }

    async function loadMessages() {
        try {
            const response =
                await fetch("/api/messages");

            if (!response.ok) {
                return;
            }

            const data =
                await response.json();

            // Kafka consumer stores newest first.
            data.reverse().forEach(renderMessage);

        } catch (error) {
            console.error(error);
        }
    }

    async function sendMessage() {
        const content =
            input.value.trim();

        if (!content) {
            return;
        }

        try {
            const response =
                await fetch("/messages", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({
                        conversation_id: "chat-1",
                        content: content
                    })
                });

            if (!response.ok) {
                alert("Failed to send message");
                return;
            }

            input.value = "";
            input.focus();

        } catch (error) {
            console.error(error);
            alert("Failed to send message");
        }
    }

    input.addEventListener("keydown", function(event) {
        if (event.key === "Enter") {
            sendMessage();
        }
    });

    loadMessages();

    setInterval(loadMessages, 1000);
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