import uuid

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from worker import register_worker
from dynamodb import save_request


app = FastAPI()

worker_id = register_worker()


class TextRequest(BaseModel):
    text: str


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
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
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            }

            h1 {
                margin-top: 0;
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

            button:hover {
                background: #1d4ed8;
            }

            button:disabled {
                background: #999;
                cursor: not-allowed;
            }

            #result {
                margin-top: 20px;
                padding: 15px;
                border-radius: 10px;
                display: none;
            }

            .success {
                background: #dcfce7;
                color: #166534;
            }

            .error {
                background: #fee2e2;
                color: #991b1b;
            }
        </style>
    </head>

    <body>

        <div class="container">

            <h1>Distributed Processor</h1>

            <p class="subtitle">
                Send text to the distributed processing system
            </p>

            <textarea
                id="text"
                placeholder="Enter your text here..."
            ></textarea>

            <button id="processButton" onclick="processText()">
                Process Text
            </button>

            <div id="result"></div>

        </div>

        <script>

            async function processText() {

                const text = document.getElementById("text").value;
                const button = document.getElementById("processButton");
                const result = document.getElementById("result");

                if (!text.trim()) {
                    result.style.display = "block";
                    result.className = "error";
                    result.innerText = "Please enter some text.";
                    return;
                }

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

                    const data = await response.json();

                    if (!response.ok) {
                        throw new Error(data.detail || "Request failed");
                    }

                    result.style.display = "block";
                    result.className = "success";

                    result.innerHTML = `
                        <strong>✓ Request saved</strong><br>
                        Request ID: ${data.request_id}<br>
                        Status: ${data.status}
                    `;

                    document.getElementById("text").value = "";

                } catch (error) {

                    result.style.display = "block";
                    result.className = "error";
                    result.innerText = error.message;

                } finally {

                    button.disabled = false;
                    button.innerText = "Process Text";

                }
            }

        </script>

    </body>
    </html>
    """


@app.post("/process")
def process_text(request: TextRequest):

    request_id = str(uuid.uuid4())

    save_request(
        request_id=request_id,
        text=request.text
    )

    return {
        "request_id": request_id,
        "status": "PROCESSING"
    }