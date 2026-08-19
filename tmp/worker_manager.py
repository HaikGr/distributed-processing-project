import os
import asyncio
import boto3

from datetime import datetime, timezone


TABLE_NAME = os.getenv(
    "WORKERS_TABLE",
    "distributed-workers",
)

HEARTBEAT_TIMEOUT = int(
    os.getenv(
        "HEARTBEAT_TIMEOUT",
        "30",
    )
)

CHECK_INTERVAL = int(
    os.getenv(
        "WORKER_MANAGER_INTERVAL",
        "10",
    )
)


dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ["AWS_DEFAULT_REGION"],
)

table = dynamodb.Table(TABLE_NAME)


def get_current_time():
    return datetime.now(timezone.utc)


def cleanup_inactive_workers():

    response = table.scan()

    workers = response.get(
        "Items",
        [],
    )

    now = get_current_time()

    for worker in workers:

        worker_id = worker["worker_id"]

        status = worker.get("status")

        last_heartbeat = worker.get(
            "last_heartbeat"
        )

        # Ignore already inactive workers
        if status != "ACTIVE":
            continue

        # Worker record is invalid
        if not last_heartbeat:

            print(
                f"Worker {worker_id} has no heartbeat. "
                f"Skipping because the heartbeat value is missing."
            )

            continue

        try:

            heartbeat_time = datetime.fromisoformat(
                last_heartbeat
            )

            age = (
                now - heartbeat_time
            ).total_seconds()

            if age > HEARTBEAT_TIMEOUT:

                print(
                    f"Worker {worker_id} heartbeat "
                    f"expired after {age:.1f} seconds."
                )

                mark_worker_inactive(
                    worker_id,
                    last_heartbeat,
                )

        except Exception as e:

            print(
                f"Failed to check worker "
                f"{worker_id}: {e}"
            )


async def manager_loop():

    print(
        "Worker Manager started. "
        f"Timeout={HEARTBEAT_TIMEOUT}s, "
        f"Interval={CHECK_INTERVAL}s"
    )

    while True:

        try:

            await asyncio.to_thread(
                cleanup_inactive_workers
            )

        except Exception as e:

            print(
                f"Worker Manager error: {e}"
            )

        await asyncio.sleep(
            CHECK_INTERVAL
        )

def mark_worker_inactive(
    worker_id: str,
    last_heartbeat: str,
):

    try:

        table.update_item(
            Key={
                "worker_id": worker_id,
            },

            UpdateExpression="""
                SET #status = :inactive
            """,

            ConditionExpression="""
                #status = :active
                AND last_heartbeat = :heartbeat
            """,

            ExpressionAttributeNames={
                "#status": "status",
            },

            ExpressionAttributeValues={
                ":active": "ACTIVE",
                ":inactive": "INACTIVE",
                ":heartbeat": last_heartbeat,
            },
        )

        print(
            f"Worker {worker_id} marked INACTIVE"
        )

        return True

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:

        print(
            f"Worker {worker_id} changed while "
            f"being checked. Skipping."
        )

        return False


if __name__ == "__main__":

    asyncio.run(manager_loop())