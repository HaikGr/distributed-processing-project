import uuid

from fastapi import FastAPI
from pydantic import BaseModel
from worker import register_worker

from my_processor import process_text
from dynamodb import save_request

app = FastAPI()
worker_id = register_worker()


class TextRequest(BaseModel):
    text: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/process")
def process(request: TextRequest):
    request_id = str(uuid.uuid4())

    result = process_text(request.text)

    save_request(request_id=request_id, text=request.text)

    return {
        "request_id": request_id,
        "input": request.text,
        "output": result,
    }
