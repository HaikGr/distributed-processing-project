import json
import os
import threading
from collections import deque
from typing import Any

from confluent_kafka import Consumer
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
    "auto.offset.reset": "earliest",
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

                processed_text = (
                    event["after"].get("processed_text")
                    if event["after"]
                    else None
                )

                print(
                    f"Received CDC event: "
                    f"op={event['operation']} "
                    f"offset={event['offset']} "
                    f"processed_text={processed_text!r}"
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

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>Processed Text TV</title>

    <style>

        * {
            box-sizing: border-box;
        }


        body {

            margin: 0;

            min-height: 100vh;

            display: flex;

            align-items: center;
            justify-content: center;

            font-family: Arial, Helvetica, sans-serif;

            background:
                radial-gradient(
                    circle at center,
                    #202020 0%,
                    #090909 55%,
                    #000000 100%
                );

            overflow: hidden;
        }


        /*
         * TV
         */

        .tv {

            width: min(1000px, 90vw);

            aspect-ratio: 16 / 10;

            position: relative;

            background:
                linear-gradient(
                    145deg,
                    #3d3d3d,
                    #181818
                );

            border-radius: 45px;

            padding: 38px;

            box-shadow:
                0 35px 80px rgba(0, 0, 0, 0.75),
                inset 0 2px 4px rgba(255,255,255,0.08),
                inset 0 -5px 12px rgba(0,0,0,0.7);
        }


        /*
         * TV screen
         */

        .screen {

            width: 100%;
            height: 100%;

            position: relative;

            display: flex;

            align-items: center;
            justify-content: center;

            overflow: hidden;

            background: #030303;

            border-radius: 28px;

            border: 8px solid #101010;

            box-shadow:
                inset 0 0 40px rgba(0,0,0,0.9),
                0 0 25px rgba(255,255,255,0.04);
        }


        /*
         * Screen glow
         */

        .screen::before {

            content: "";

            position: absolute;

            inset: 0;

            pointer-events: none;

            background:
                linear-gradient(
                    rgba(255,255,255,0.025) 50%,
                    rgba(0,0,0,0.025) 50%
                );

            background-size: 100% 4px;

            z-index: 5;
        }


        /*
         * Screen reflection
         */

        .screen::after {

            content: "";

            position: absolute;

            top: 0;
            left: 0;

            width: 100%;
            height: 100%;

            pointer-events: none;

            background:
                linear-gradient(
                    120deg,
                    rgba(255,255,255,0.08),
                    transparent 25%
                );

            z-index: 6;
        }


        /*
         * Processed text
         */

        #processed-text {

            max-width: 85%;

            padding: 40px;

            text-align: center;

            font-size: clamp(
                35px,
                5vw,
                80px
            );

            line-height: 1.2;

            font-weight: bold;

            color: #f4f4f4;

            text-shadow:
                0 0 8px rgba(255,255,255,0.35),
                0 0 20px rgba(255,255,255,0.08);

            word-break: break-word;

            z-index: 4;
        }


        /*
         * Waiting screen
         */

        #waiting {

            color: #777;

            font-size: clamp(
                20px,
                3vw,
                32px
            );

            text-align: center;

            z-index: 4;
        }


        /*
         * Channel indicator
         */

        .channel {

            position: absolute;

            top: 20px;
            right: 28px;

            z-index: 10;

            color: rgba(255,255,255,0.65);

            font-size: 14px;

            letter-spacing: 2px;
        }


        /*
         * TV power light
         */

        .power-light {

            position: absolute;

            bottom: 15px;
            right: 55px;

            width: 9px;
            height: 9px;

            border-radius: 50%;

            background: #46ff66;

            box-shadow:
                0 0 8px #46ff66;
        }


        /*
         * TV controls
         */

        .controls {

            position: absolute;

            right: 10px;

            top: 50%;

            transform: translateY(-50%);

            display: flex;

            flex-direction: column;

            gap: 12px;
        }


        .button {

            width: 14px;
            height: 14px;

            border-radius: 50%;

            background: #111;

            box-shadow:
                inset 0 2px 2px rgba(255,255,255,0.1),
                0 2px 3px rgba(0,0,0,0.6);
        }


        /*
         * TV legs
         */

        .leg {

            position: absolute;

            bottom: -50px;

            width: 35px;
            height: 55px;

            background: #222;

            border-radius: 0 0 10px 10px;
        }


        .leg.left {

            left: 18%;

            transform: rotate(10deg);
        }


        .leg.right {

            right: 18%;

            transform: rotate(-10deg);
        }


        /*
         * Status
         */

        #status {

            position: absolute;

            bottom: 20px;
            left: 25px;

            z-index: 10;

            font-size: 13px;

            color: rgba(255,255,255,0.4);

            letter-spacing: 1px;
        }


        /*
         * Animation for new message
         */

        .flash {

            animation: screenFlash 0.45s ease;
        }


        @keyframes screenFlash {

            0% {
                filter: brightness(2);
            }

            100% {
                filter: brightness(1);
            }
        }


    </style>

</head>


<body>


<div class="tv">

    <div class="screen">

        <div class="channel">
            CH 01
        </div>


        <div id="waiting">
            Waiting for processed text...
        </div>


        <div
            id="processed-text"
            style="display: none;"
        ></div>


        <div id="status">
            POSTGRESQL → DEBEZIUM → KAFKA
        </div>

    </div>


    <div class="power-light"></div>


    <div class="controls">

        <div class="button"></div>
        <div class="button"></div>
        <div class="button"></div>

    </div>


    <div class="leg left"></div>
    <div class="leg right"></div>

</div>


<script>

let currentProcessedText = null;


function render(processedText) {

    const textElement =
        document.getElementById(
            "processed-text"
        );

    const waiting =
        document.getElementById(
            "waiting"
        );


    if (!processedText) {

        textElement.style.display =
            "none";

        waiting.style.display =
            "block";

        return;
    }


    /*
     * Don't redraw the TV
     * if the message hasn't changed.
     */

    if (
        processedText ===
        currentProcessedText
    ) {
        return;
    }


    currentProcessedText =
        processedText;


    textElement.textContent =
        processedText;


    waiting.style.display =
        "none";

    textElement.style.display =
        "block";


    /*
     * TV flash effect
     */

    const screen =
        document.querySelector(
            ".screen"
        );

    screen.classList.remove(
        "flash"
    );

    void screen.offsetWidth;

    screen.classList.add(
        "flash"
    );
}


async function loadMessages() {

    try {

        const response =
            await fetch(
                "/api/messages"
            );


        const data =
            await response.json();


        /*
         * Your deque uses appendleft(),
         * so data[0] is the newest event.
         */

        const processedText =
            data[0]
                ?.after
                ?.processed_text;


        render(processedText);


    } catch (error) {

        console.error(
            "Failed to load messages:",
            error
        );

    }
}


/*
 * Initial load
 */

loadMessages();


/*
 * Check for a new Kafka event
 * every second.
 */

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