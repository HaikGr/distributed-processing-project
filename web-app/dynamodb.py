import boto3
import os


TABLE_NAME = os.getenv(
    "DYNAMODB_TABLE",
    "distributed-processing"
)

dynamodb = boto3.resource("dynamodb")

table = dynamodb.Table(TABLE_NAME)


def save_request(request_id: str, text: str):
    table.put_item(
        Item={
            "request_id": request_id,
            "text": text,
            "status": "PROCESSING",
        }
    )


def get_request(request_id: str):
    response = table.get_item(
        Key={
            "request_id": request_id
        }
    )

    return response.get("Item")
