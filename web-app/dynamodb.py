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


def create_request(
    request_id: str,
    text: str,
    expected_workers: list[str],
):

    now = datetime.now(timezone.utc).isoformat()

    table.put_item(
        Item={
            "request_id": request_id,
            "text": text,
            "status": "WAITING",
            "expected_workers": expected_workers,
            "consumed_by": [],
            "created_at": now,
        }
    )


def get_request(request_id: str):

    response = table.get_item(
        Key={
            "request_id": request_id,
        }
    )

    return response.get("Item")


def get_waiting_requests():

    response = table.scan(
        FilterExpression="#status = :status",
        ExpressionAttributeNames={
            "#status": "status",
        },
        ExpressionAttributeValues={
            ":status": "WAITING",
        },
    )

    return response.get("Items", [])


def mark_consumed(
    request_id: str,
    worker_id: str,
):

    try:

        table.update_item(
            Key={
                "request_id": request_id,
            },

            UpdateExpression="""
                SET consumed_by = list_append(
                    if_not_exists(consumed_by, :empty),
                    :worker
                )
            """,

            ConditionExpression="""
                #status = :waiting
                AND NOT contains(consumed_by, :worker_id)
            """,

            ExpressionAttributeNames={
                "#status": "status",
            },

            ExpressionAttributeValues={
                ":empty": [],
                ":worker": [worker_id],
                ":worker_id": worker_id,
                ":waiting": "WAITING",
            },
        )

        return True

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:

        return False


def all_workers_consumed(request: dict):

    expected_workers = set(
        request.get("expected_workers", [])
    )

    consumed_by = set(
        request.get("consumed_by", [])
    )

    return expected_workers.issubset(consumed_by)


def claim_processing(request_id: str):

    try:

        table.update_item(
            Key={
                "request_id": request_id,
            },

            UpdateExpression="""
                SET #status = :processing
            """,

            ConditionExpression="""
                #status = :waiting
            """,

            ExpressionAttributeNames={
                "#status": "status",
            },

            ExpressionAttributeValues={
                ":waiting": "WAITING",
                ":processing": "PROCESSING",
            },
        )

        return True

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:

        return False


def delete_request(request_id: str):

    table.delete_item(
        Key={
            "request_id": request_id,
        }
    )