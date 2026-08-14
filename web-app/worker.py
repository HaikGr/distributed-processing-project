import os
import socket
from datetime import datetime, timezone

import boto3


TABLE_NAME = os.getenv(
    "WORKERS_TABLE",
    "distributed-workers",
)

dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_DEFAULT_REGION"])
table = dynamodb.Table(TABLE_NAME)


def get_worker_id() -> str:
    return os.getenv("WORKER_ID", socket.gethostname())


def register_worker():
    worker_id = get_worker_id()

    table.put_item(
        Item={
            "worker_id": worker_id,
            "status": "ACTIVE",
            "last_heartbeat": datetime.now(timezone.utc).isoformat(),
        }
    )

    return worker_id


def heartbeat():
    worker_id = get_worker_id()

    table.update_item(
        Key={
            "worker_id": worker_id,
        },
        UpdateExpression="SET #status = :status, last_heartbeat = :heartbeat",
        ExpressionAttributeNames={
            "#status": "status",
        },
        ExpressionAttributeValues={
            ":status": "ACTIVE",
            ":heartbeat": datetime.now(timezone.utc).isoformat(),
        },
    )


def get_active_workers():
    response = table.scan(
        FilterExpression="#status = :status",
        ExpressionAttributeNames={
            "#status": "status",
        },
        ExpressionAttributeValues={
            ":status": "ACTIVE",
        },
    )

    return response.get("Items", [])
