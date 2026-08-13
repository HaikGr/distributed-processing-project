FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

ARG NEXUS_URL

RUN pip install --no-cache-dir \
    --index-url "${NEXUS_URL}" \
    -r requirements.txt

COPY app.py .
COPY dynamodb.py .
COPY worker.py .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]