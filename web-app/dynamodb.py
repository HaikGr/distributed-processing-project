import boto3
import os


TABLE_NAME = os.getenv(
    "DYNAMODB_TABLE",
    "distributed-processing"
)


dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ["AWS_DEFAULT_REGION"]
)

table = dynamodb.Table(TABLE_NAME)


def save_request(
    request_id: str,
    text: str,
    processed_text: str,
):
    table.put_item(
        Item={
            "request_id": request_id,
            "text": text,
            "processed_text": processed_text,
            "status": "COMPLETED",
        }
    )


def get_request(request_id: str):
    response = table.get_item(
        Key={
            "request_id": request_id
        }
    )

    return response.get("Item")