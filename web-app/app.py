import uuid
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from my_processor import process_text
import json
import os
import threading
from collections import deque
from typing import Any

from confluent_kafka import Consumer

from postgres import (
    init_db,
    save_request,
    get_all_requests,
    get_request,
    create_message
)


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
    "app1-chat",
)

MAX_MESSAGES = int(
    os.getenv("MAX_MESSAGES", "100")
)


@asynccontextmanager
async def lifespan(app: FastAPI):

    # Create PostgreSQL table when application starts
    await asyncio.to_thread(init_db)

    yield


app = FastAPI(
    lifespan=lifespan
)

chat_messages = deque(maxlen=MAX_MESSAGES)


consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})


def consume_chat_messages() -> None:
    consumer.subscribe([KAFKA_TOPIC])

    print(
        f"Chat consumer started: "
        f"topic={KAFKA_TOPIC}, "
        f"group={KAFKA_GROUP_ID}, "
        f"bootstrap={KAFKA_BOOTSTRAP_SERVERS}"
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
                }

                chat_messages.appendleft(event)

                print(
                    f"Received chat message: "
                    f"sender={event['sender']} "
                    f"receiver={event['receiver']} "
                    f"content={event['content']!r}"
                )

            except Exception as exc:
                print(
                    f"Failed to process Kafka message: {exc}"
                )

    except Exception as exc:
        print(f"Chat consumer stopped: {exc}")

    finally:
        consumer.close()

consumer_thread = threading.Thread(
    target=consume_chat_messages,
    daemon=True,
)

consumer_thread.start()


class TextRequest(BaseModel):
    text: str

class MessageRequest(BaseModel):
    conversation_id: str
    content: str


@app.get("/health")
def health():

    return {
        "status": "healthy",
    }


@app.post("/process")
async def process_request(
    request: TextRequest,
):

    request_id = str(
        uuid.uuid4()
    )

    # Process the text immediately
    processed_text = await asyncio.to_thread(
        process_text,
        request.text,
    )

    # Save result directly to PostgreSQL
    await asyncio.to_thread(
        save_request,
        request_id,
        request.text,
        processed_text,
    )

    print(
        f"Request {request_id} completed "
        f"and saved to PostgreSQL"
    )

    # Return the result immediately
    return {
        "request_id": request_id,
        "text": request.text,
        "processed_text": processed_text,
        "status": "COMPLETED",
    }


@app.get("/request/{request_id}")
def request_status(
    request_id: str,
):

    request = get_request(
        request_id
    )

    if request is None:

        return {
            "detail": "Request not found",
        }

    return request


@app.get("/requests")
def requests():

    return {
        "requests": get_all_requests(),
    }


@app.get("/", response_class=HTMLResponse)
def home():

    return """
    <!DOCTYPE html>
    <html lang="en">

    <head>

        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="width=device-width, initial-scale=1.0"
        >

        <title>Text Processor</title>

        <style>

            * {
                box-sizing: border-box;
            }

            body {
                margin: 0;
                font-family: Arial, sans-serif;
                background: #f4f6f8;

                display: flex;
                justify-content: center;
                align-items: center;

                min-height: 100vh;
            }

            .container {
                width: 90%;
                max-width: 700px;

                background: white;

                padding: 40px;

                border-radius: 16px;

                box-shadow:
                    0 10px 30px
                    rgba(0, 0, 0, 0.1);
            }

            h1 {
                text-align: center;
            }

            .subtitle {
                text-align: center;
                color: #666;

                margin-bottom: 30px;
            }

            textarea {
                width: 100%;

                min-height: 180px;

                padding: 15px;

                border: 1px solid #ccc;
                border-radius: 10px;

                font-size: 16px;

                resize: vertical;
            }

            button {
                width: 100%;

                margin-top: 15px;

                padding: 14px;

                border: none;
                border-radius: 10px;

                background: #2563eb;

                color: white;

                font-size: 16px;

                cursor: pointer;
            }

            button:disabled {
                background: #999;
            }

            #result {
                margin-top: 20px;

                padding: 20px;

                border-radius: 10px;

                display: none;

                line-height: 1.6;

                word-break: break-word;
            }

            .success {
                background: #dcfce7;
                color: #166534;
            }

            .error {
                background: #fee2e2;
                color: #991b1b;
            }

            .processed {
                margin-top: 10px;

                padding: 12px;

                background: white;

                border-radius: 8px;

                font-size: 18px;

                font-weight: bold;
            }

        </style>

    </head>

    <body>

        <div class="container">

            <h1>
                Text Processor
            </h1>

            <p class="subtitle">
                Enter text, process it,
                and save the result to PostgreSQL
            </p>

            <textarea
                id="text"
                placeholder="Enter your text here..."
            ></textarea>

            <button
                id="processButton"
                onclick="processText()"
            >
                Process Text
            </button>

            <div id="result"></div>

        </div>


        <script>

            async function processText() {

                const text =
                    document.getElementById(
                        "text"
                    ).value;

                const button =
                    document.getElementById(
                        "processButton"
                    );

                const result =
                    document.getElementById(
                        "result"
                    );


                if (!text.trim()) {

                    result.style.display =
                        "block";

                    result.className =
                        "error";

                    result.innerText =
                        "Please enter some text.";

                    return;
                }


                button.disabled = true;

                button.innerText =
                    "Processing...";


                try {

                    const response =
                        await fetch(
                            "/process",
                            {
                                method: "POST",

                                headers: {
                                    "Content-Type":
                                        "application/json"
                                },

                                body: JSON.stringify({
                                    text: text
                                })
                            }
                        );


                    const data =
                        await response.json();


                    if (!response.ok) {

                        throw new Error(
                            data.detail ||
                            "Request failed"
                        );
                    }


                    result.style.display =
                        "block";

                    result.className =
                        "success";


                    result.innerHTML = `

                        <strong>
                            ✓ Processing completed
                        </strong>

                        <br><br>

                        <strong>
                            Original text:
                        </strong>

                        <div>
                            ${escapeHtml(data.text)}
                        </div>

                        <br>

                        <strong>
                            Processed text:
                        </strong>

                        <div class="processed">
                            ${escapeHtml(
                                data.processed_text
                            )}
                        </div>

                        <br>

                        <strong>
                            Request ID:
                        </strong>

                        ${data.request_id}

                        <br>

                        <strong>
                            Status:
                        </strong>

                        ${data.status}

                        <br><br>

                        ✓ Saved to PostgreSQL
                    `;


                    document.getElementById(
                        "text"
                    ).value = "";


                } catch (error) {

                    result.style.display =
                        "block";

                    result.className =
                        "error";

                    result.innerText =
                        error.message;


                } finally {

                    button.disabled = false;

                    button.innerText =
                        "Process Text";
                }
            }


            function escapeHtml(text) {

                const div =
                    document.createElement("div");

                div.textContent = text;

                return div.innerHTML;
            }

        </script>

    </body>

    </html>
    """

@app.get("/api/messages")
def get_chat_messages() -> list[dict[str, Any]]:
    return list(chat_messages)

@app.post("/messages")
def send_message(message: MessageRequest):
    try:
        return create_message(
            conversation_id=message.conversation_id,
            sender="app1",
            receiver="app2",
            content=message.content,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create message: {exc}",
        )

@app.get("/chat", response_class=HTMLResponse)
def chat_page():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>App 1 Chat</title>

        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 800px;
                margin: 40px auto;
                padding: 0 20px;
                background: #f5f5f5;
            }

            h1 {
                text-align: center;
            }

            #messages {
                height: 500px;
                overflow-y: auto;
                background: white;
                border: 1px solid #ddd;
                border-radius: 10px;
                padding: 15px;
                margin-bottom: 15px;
            }

            .message {
                margin: 8px 0;
                padding: 10px 14px;
                border-radius: 12px;
                max-width: 70%;
                word-wrap: break-word;
            }

            .mine {
                margin-left: auto;
                background: #d9fdd3;
                text-align: right;
            }

            .theirs {
                margin-right: auto;
                background: #eeeeee;
            }

            #composer {
                display: flex;
                gap: 10px;
            }

            #messageInput {
                flex: 1;
                padding: 12px;
                border: 1px solid #ccc;
                border-radius: 8px;
                font-size: 16px;
            }

            button {
                padding: 12px 20px;
                border: none;
                border-radius: 8px;
                cursor: pointer;
            }
        </style>
    </head>

    <body>
        <h1>App 1 Chat</h1>

        <div id="messages"></div>

        <div id="composer">
            <input
                id="messageInput"
                type="text"
                placeholder="Type a message..."
            />
            <button onclick="sendMessage()">Send</button>
        </div>

        <script>
            const messagesContainer =
                document.getElementById("messages");

            const input =
                document.getElementById("messageInput");

            const displayedMessages = new Set();

            function addMessage(message) {
                if (displayedMessages.has(message.id)) {
                    return;
                }

                displayedMessages.add(message.id);

                const div = document.createElement("div");

                div.className =
                    "message " +
                    (
                        message.sender === "app1"
                            ? "mine"
                            : "theirs"
                    );

                div.textContent =
                    (
                        message.sender === "app1"
                            ? "You: "
                            : "App 2: "
                    ) + message.content;

                messagesContainer.appendChild(div);

                messagesContainer.scrollTop =
                    messagesContainer.scrollHeight;
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

                    // Backend keeps newest first.
                    // We display oldest → newest.
                    data.reverse().forEach(addMessage);

                } catch (error) {
                    console.error(
                        "Failed to load chat messages:",
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

                try {
                    const response =
                        await fetch("/messages", {
                            method: "POST",

                            headers: {
                                "Content-Type":
                                    "application/json"
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

                    const message =
                        await response.json();

                    // Display our own message immediately.
                    addMessage(message);

                    input.value = "";
                    input.focus();

                } catch (error) {
                    console.error(error);
                    alert("Failed to send message");
                }
            }


            input.addEventListener(
                "keydown",
                function(event) {
                    if (event.key === "Enter") {
                        sendMessage();
                    }
                }
            );


            loadMessages();

            setInterval(
                loadMessages,
                1000
            );
        </script>
    </body>
    </html>
    """