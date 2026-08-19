FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

ARG NEXUS_URL

RUN python -m pip install --upgrade pip

RUN pip install --no-cache-dir \
    --trusted-host host.docker.internal \
    --index-url "${NEXUS_URL}" \
    -r requirements.txt

COPY app.py .
COPY postgres.py .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]