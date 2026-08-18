import os
import socket
from datetime import datetime, timezone
from my_processor import process_text
from postgres import save_request
import asyncio
import random
import boto3
from dynamodb import (
    get_waiting_requests,
    get_request,
    mark_consumed,
    all_workers_consumed,
    claim_processing,
    delete_request,
)


TABLE_NAME = os.getenv(
    "WORKERS_TABLE",
    "distributed-workers",
)

dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ["AWS_DEFAULT_REGION"],
)

table = dynamodb.Table(TABLE_NAME)


def get_worker_id() -> str:
    return os.getenv(
        "WORKER_ID",
        socket.gethostname(),
    )


def get_current_time() -> str:
    return datetime.now(timezone.utc).isoformat()


def register_worker():

    worker_id = get_worker_id()
    now = get_current_time()

    table.put_item(
        Item={
            "worker_id": worker_id,
            "status": "ACTIVE",
            "last_heartbeat": now,
            "registered_at": now,
        }
    )

    return worker_id


def heartbeat():
    worker_id = get_worker_id()

    table.update_item(
        Key={
            "worker_id": worker_id,
        },
        UpdateExpression="""
            SET #status = :status,
                last_heartbeat = :heartbeat
        """,
        ExpressionAttributeNames={
            "#status": "status",
        },
        ExpressionAttributeValues={
            ":status": "ACTIVE",
            ":heartbeat": get_current_time(),
        },
    )


def unregister_worker():
    worker_id = get_worker_id()

    table.update_item(
        Key={
            "worker_id": worker_id,
        },
        UpdateExpression="""
            SET #status = :status,
                last_heartbeat = :heartbeat
        """,
        ExpressionAttributeNames={
            "#status": "status",
        },
        ExpressionAttributeValues={
            ":status": "INACTIVE",
            ":heartbeat": get_current_time(),
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

async def consume_requests(worker_id: str):

    while True:

        try:

            requests = await asyncio.to_thread(
                get_waiting_requests
            )

            for request in requests:

                request_id = request["request_id"]

                expected_workers = request.get(
                    "expected_workers",
                    [],
                )

                # This worker is not part of the request snapshot
                if worker_id not in expected_workers:
                    continue


                # Try to consume only once
                consumed = await asyncio.to_thread(
                    mark_consumed,
                    request_id,
                    worker_id,
                )

                if not consumed:
                    continue


                print(
                    f"Worker {worker_id} consumed "
                    f"request {request_id}"
                )


                # Random sleep
                sleep_time = random.randint(1, 10)

                print(
                    f"Worker {worker_id} sleeping "
                    f"{sleep_time} seconds"
                )

                await asyncio.sleep(sleep_time)


                # Keep checking until all expected workers consume
                while True:

                    request = await asyncio.to_thread(
                        get_request,
                        request_id,
                    )

                    # Another worker may already have deleted it
                    if request is None:
                        break


                    if all_workers_consumed(request):

                        # Only one pod can claim processing
                        claimed = await asyncio.to_thread(
                            claim_processing,
                            request_id,
                        )

                        if claimed:

                            print(
                                f"Worker {worker_id} "
                                f"claimed request {request_id}"
                            )

                            # Actual processing will happen here
                            # We add it in the next step

                            text = request["text"]

                            processed_text = await asyncio.to_thread(
                                process_text,
                                text,
                            )

                            await asyncio.to_thread(
                                save_request,
                                request_id,
                                text,
                                processed_text,
                                worker_id,
                            )

                            print(
                                f"Worker {worker_id} processed "
                                f"request {request_id}"
                            )


                            await asyncio.to_thread(
                                delete_request,
                                request_id,
                            )

                            print(
                                f"Worker {worker_id} deleted "
                                f"cache for {request_id}"
                            )

                        break


                    print(
                        f"Worker {worker_id}: not all "
                        f"workers consumed {request_id}, "
                        f"waiting 5 seconds"
                    )

                    await asyncio.sleep(5)


        except Exception as e:

            print(
                f"Consumer error for worker "
                f"{worker_id}: {e}"
            )

        await asyncio.sleep(2)

HEARTBEAT_TIMEOUT = 30


def cleanup_inactive_workers():

    now = datetime.now(timezone.utc)

    response = table.scan()

    workers = response.get("Items", [])

    for worker in workers:

        worker_id = worker["worker_id"]

        last_heartbeat = worker.get("last_heartbeat")

        if not last_heartbeat:
            continue

        last_heartbeat_time = datetime.fromisoformat(
            last_heartbeat
        )

        age = (
            now - last_heartbeat_time
        ).total_seconds()

        if age > HEARTBEAT_TIMEOUT:

            print(
                f"Worker {worker_id} "
                f"has not sent heartbeat for "
                f"{age:.1f}s. Marking INACTIVE."
            )

            table.update_item(
                Key={
                    "worker_id": worker_id,
                },

                UpdateExpression="""
                    SET #status = :inactive
                """,

                ExpressionAttributeNames={
                    "#status": "status",
                },

                ExpressionAttributeValues={
                    ":inactive": "INACTIVE",
                },
            )

async def worker_manager():

    while True:

        try:

            await asyncio.to_thread(
                cleanup_inactive_workers
            )

        except Exception as e:

            print(
                f"Worker manager error: {e}"
            )

        await asyncio.sleep(10)