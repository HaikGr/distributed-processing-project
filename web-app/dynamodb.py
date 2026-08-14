import os
import boto3

TABLE_NAME = os.getenv(
    "DYNAMODB_TABLE",
    "distributed-processing"
)

DYNAMODB_ENDPOINT = os.getenv("DYNAMODB_ENDPOINT")

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=DYNAMODB_ENDPOINT
)

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
