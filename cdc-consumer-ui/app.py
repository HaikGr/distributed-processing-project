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

KAFKA_TYPING_TOPIC = os.getenv(
    "KAFKA_TYPING_TOPIC",
    "chat-typing",
)

APP_ID = os.getenv(
    "APP_ID",
    "app2",
)

app = FastAPI()

messages = deque(maxlen=MAX_MESSAGES)
typing_users = {}
typing_lock = threading.Lock()


consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})

typing_consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": f"{KAFKA_GROUP_ID}-typing",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
})

typing_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
})

class MessageRequest(BaseModel):
    conversation_id: str
    content: str

class TypingRequest(BaseModel):
    conversation_id: str
    is_typing: bool


@app.post("/typing")
def update_typing(typing: TypingRequest):
    event = {
        "event_type": "typing",
        "conversation_id": typing.conversation_id,
        "user_id": APP_ID,
        "is_typing": typing.is_typing,
    }

    try:
        typing_producer.produce(
            KAFKA_TYPING_TOPIC,
            key=f"{typing.conversation_id}:{APP_ID}",
            value=json.dumps(event).encode("utf-8"),
        )

        typing_producer.flush()

        return {
            "status": "sent",
            "event": event,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish typing event: {exc}",
        )


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

def consume_typing_events() -> None:
    typing_consumer.subscribe([KAFKA_TYPING_TOPIC])

    print(
        f"Typing consumer started. "
        f"topic={KAFKA_TYPING_TOPIC}, "
        f"group={KAFKA_GROUP_ID}-typing"
    )

    try:
        while True:
            msg = typing_consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Typing Kafka error: {msg.error()}")
                continue

            try:
                event = json.loads(
                    msg.value().decode("utf-8")
                )

                # Ignore events produced by this application.
                if event.get("user_id") == APP_ID:
                    continue

                user_id = event.get("user_id")

                if not user_id:
                    continue

                with typing_lock:
                    typing_users[user_id] = event.get(
                        "is_typing",
                        False,
                    )

                print(
                    f"Typing event received: "
                    f"user={user_id}, "
                    f"is_typing={event.get('is_typing')}"
                )

            except Exception as exc:
                print(
                    f"Failed to process typing event: {exc}"
                )

    except Exception as exc:
        print(f"Typing consumer stopped: {exc}")

    finally:
        typing_consumer.close()


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
typing_consumer_thread = threading.Thread(
    target=consume_typing_events,
    daemon=True,
)

consumer_thread.start()
typing_consumer_thread.start()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

@app.get("/api/typing")
def get_typing() -> dict[str, Any]:
    with typing_lock:
        users = [
            user_id
            for user_id, is_typing in typing_users.items()
            if is_typing
        ]

    return {
        "typing_users": users,
    }


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

        #typingIndicator {
            min-height: 24px;
            padding: 4px 15px;
            font-size: 13px;
            font-style: italic;
            color: #666;
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

    <div id="typingIndicator"></div>

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

    const typingIndicator =
    document.getElementById("typingIndicator");

    let isTyping = false;
    let typingTimeout = null;

    let displayedIds = new Set();

    async function publishTyping(isCurrentlyTyping) {
        try {
            await fetch("/typing", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    conversation_id: "chat-1",
                    is_typing: isCurrentlyTyping
                })
            });
        } catch (error) {
            console.error(
                "Failed to publish typing event:",
                error
            );
        }
    }

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

    async function loadTyping() {
        try {
            const response =
                await fetch("/api/typing");

            if (!response.ok) {
                return;
            }

            const data =
                await response.json();

            if (data.typing_users.length > 0) {
                typingIndicator.textContent =
                    data.typing_users
                        .map(user => `${user} is typing...`)
                        .join(", ");
            } else {
                typingIndicator.textContent = "";
            }

        } catch (error) {
            console.error(
                "Failed to load typing state:",
                error
            );
        }
    }

    async function sendMessage() {
        const content =
            input.value.trim();

        if (!content) {
            return;
        }

        clearTimeout(typingTimeout);

        if (isTyping) {
            isTyping = false;
            await publishTyping(false);
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

    input.addEventListener("input", function() {

    if (!isTyping && input.value.trim()) {
        isTyping = true;
        publishTyping(true);
    }

    clearTimeout(typingTimeout);

        typingTimeout = setTimeout(function() {
            if (isTyping) {
                isTyping = false;
                publishTyping(false);
            }
        }, 1500);
    });

    loadMessages();

    setInterval(loadMessages, 1000);
    setInterval(loadTyping, 500);
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