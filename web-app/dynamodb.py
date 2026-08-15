import boto3
import os
from datetime import datetime, timezone


TABLE_NAME = os.getenv(
    "DYNAMODB_TABLE",
    "distributed-processing",
)


dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ["AWS_DEFAULT_REGION"],
)

table = dynamodb.Table(TABLE_NAME)


def save_request(
    request_id: str,
    text: str,
    processed_text: str,
    worker_id: str,
):
    now = datetime.now(timezone.utc).isoformat()

    table.put_item(
        Item={
            "request_id": request_id,
            "text": text,
            "processed_text": processed_text,
            "status": "COMPLETED",
            "worker_id": worker_id,
            "completed_at": now,
        }
    )


def get_request(request_id: str):
    response = table.get_item(
        Key={
            "request_id": request_id,
        }
    )

    return response.get("Item")


def get_all_requests():
    response = table.scan()

    return response.get("Items", [])