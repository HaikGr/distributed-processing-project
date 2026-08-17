import os
import psycopg
from datetime import datetime, timezone


DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = os.environ["POSTGRES_PORT"]
DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]


def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS requests (
                    request_id VARCHAR(255) PRIMARY KEY,

                    text TEXT NOT NULL,

                    processed_text TEXT,

                    status VARCHAR(50) NOT NULL,

                    worker_id VARCHAR(255),

                    created_at TIMESTAMPTZ NOT NULL,

                    completed_at TIMESTAMPTZ
                );
                """
            )

        conn.commit()


def save_request(
    request_id: str,
    text: str,
    processed_text: str,
    worker_id: str,
):

    now = datetime.now(timezone.utc)

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO requests (
                    request_id,
                    text,
                    processed_text,
                    status,
                    worker_id,
                    created_at,
                    completed_at
                )

                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                );
                """,
                (
                    request_id,
                    text,
                    processed_text,
                    "COMPLETED",
                    worker_id,
                    now,
                    now,
                ),
            )

        conn.commit()


def get_all_requests():

    with get_connection() as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    request_id,
                    text,
                    processed_text,
                    status,
                    worker_id,
                    created_at,
                    completed_at
                FROM requests
                ORDER BY created_at DESC;
                """
            )

            rows = cursor.fetchall()

    return [
        {
            "request_id": row[0],
            "text": row[1],
            "processed_text": row[2],
            "status": row[3],
            "worker_id": row[4],
            "created_at": row[5].isoformat(),
            "completed_at": (
                row[6].isoformat()
                if row[6]
                else None
            ),
        }
        for row in rows
    ]