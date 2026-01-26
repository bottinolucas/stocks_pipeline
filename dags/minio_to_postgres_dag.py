import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "adminadmin"
BUCKET = "raw-transactions"
LOCAL_DIR = "/tmp/minio_downloads"

POSTGRES_USER="admin"
POSTGRES_PASSWORD="admin"
POSTGRES_DB="admin"

def list_files():
    s3 = S3Hook(aws_conn_id="minio_conn")
    keys = s3.list_keys(bucket_name=BUCKET, prefix="")
    return keys or []

def process_file(key: str):
    s3 = S3Hook(aws_conn_id="minio_conn")
    pg = PostgresHook(postgres_conn_id="postgres_conn")

    content = s3.read_key(key, bucket_name=BUCKET)
    record = json.loads(content)

    symbol = record.get("symbol")
    ts = record.get("fetched_at")
    price = record.get("c")
    high = record.get("h")
    low = record.get("l")
    open_price = record.get("o")
    prev_close = record.get("pc")

    pg.run(
        """
        INSERT INTO stock_quotes (
            symbol, fetched_at, price, high, low, open_price, prev_close
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        parameters=(symbol, ts, price, high, low, open_price, prev_close)
    )

def process_all(ti):
    keys = ti.xcom_pull(task_ids="list_files") or []
    for key in keys:
        if key.endswith("/"):
            continue
        process_file(key)


with DAG(
    dag_id="minio_to_postgres",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:

    t1 = PythonOperator(
        task_id="list_files",
        python_callable=list_files
    )

    t2 = PythonOperator(
        task_id="process_all_files",
        python_callable=process_all
    )

    t1 >> t2