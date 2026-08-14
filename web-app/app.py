import uuid

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from worker import register_worker
from dynamodb import save_request
from my_processor import process_text


app = FastAPI()

# Register this Kubernetes pod as a worker
worker_id = register_worker()


class TextRequest(BaseModel):
    text: str


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "worker_id": worker_id,
    }


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <!DOCTYPE html>
    <html lang="en">

    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">

        <title>Distributed Processor</title>

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
                    0 10px 30px rgba(0, 0, 0, 0.1);
            }

            h1 {
                margin-top: 0;
                margin-bottom: 10px;

                text-align: center;
            }

            .subtitle {
                text-align: center;
                color: #666;

                margin-bottom: 30px;
            }

            .worker {
                text-align: center;

                font-size: 13px;
                color: #888;

                margin-bottom: 20px;
            }

            textarea {
                width: 100%;

                min-height: 180px;

                padding: 15px;

                border: 1px solid #ccc;
                border-radius: 10px;

                font-size: 16px;

                resize: vertical;

                outline: none;
            }

            textarea:focus {
                border-color: #2563eb;
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

            button:hover {
                background: #1d4ed8;
            }

            button:disabled {
                background: #999;

                cursor: not-allowed;
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

            .info {
                margin-top: 10px;

                font-size: 13px;

                color: #555;
            }
        </style>
    </head>

    <body>

        <div class="container">

            <h1>Distributed Processor</h1>

            <p class="subtitle">
                Send text to the distributed processing system
            </p>

            <div class="worker">
                Worker: <strong id="worker"></strong>
            </div>

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

            // Display the Kubernetes pod/worker
            fetch("/health")
                .then(response => response.json())
                .then(data => {
                    document.getElementById("worker").innerText =
                        data.worker_id;
                })
                .catch(() => {
                    document.getElementById("worker").innerText =
                        "Unknown";
                });


            async function processText() {

                const text =
                    document.getElementById("text").value;

                const button =
                    document.getElementById("processButton");

                const result =
                    document.getElementById("result");


                // Validate input

                if (!text.trim()) {

                    result.style.display = "block";

                    result.className = "error";

                    result.innerText =
                        "Please enter some text.";

                    return;
                }


                // Disable button

                button.disabled = true;

                button.innerText = "Processing...";


                try {

                    const response = await fetch("/process", {

                        method: "POST",

                        headers: {
                            "Content-Type": "application/json"
                        },

                        body: JSON.stringify({
                            text: text
                        })

                    });


                    const data =
                        await response.json();


                    if (!response.ok) {

                        throw new Error(
                            data.detail ||
                            "Request failed"
                        );

                    }


                    // Display result

                    result.style.display = "block";

                    result.className = "success";


                    result.innerHTML = `

                        <strong>
                            ✓ Processing completed
                        </strong>

                        <br><br>

                        <strong>
                            Original text:
                        </strong>

                        <div>
                            ${escapeHtml(data.original_text)}
                        </div>

                        <br>

                        <strong>
                            Processed text:
                        </strong>

                        <div class="processed">
                            ${escapeHtml(data.processed_text)}
                        </div>

                        <div class="info">

                            <strong>Request ID:</strong>
                            ${data.request_id}

                            <br>

                            <strong>Status:</strong>
                            ${data.status}

                            <br>

                            <strong>Worker:</strong>
                            ${data.worker_id}

                        </div>
                    `;


                    // Clear textarea

                    document.getElementById("text").value = "";


                } catch (error) {

                    result.style.display = "block";

                    result.className = "error";

                    result.innerText =
                        error.message;


                } finally {

                    button.disabled = false;

                    button.innerText =
                        "Process Text";

                }
            }


            // Prevent HTML injection when displaying user input

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


@app.post("/process")
def process_text(request: TextRequest):

    # Generate unique request ID
    request_id = str(uuid.uuid4())


    # Process text using your my_processor module
    processed_text = process_text(request.text)


    # Save request and result to DynamoDB
    save_request(
        request_id=request_id,
        text=request.text,
        processed_text=processed_text,
    )


    return {
        "request_id": request_id,

        "status": "COMPLETED",

        "original_text": request.text,

        "processed_text": processed_text,

        "worker_id": worker_id,
    }