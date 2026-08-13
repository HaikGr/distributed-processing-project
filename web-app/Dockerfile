FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

COPY my_processor-0.1.0-py3-none-any.whl .

RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY dynamodb.py .
COPY worker.py .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]