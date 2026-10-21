"""DAG ЛР № 2: Загрузка исходного файла в raw-слой SeaweedFS."""

import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from ingestion.load_raw import load_to_raw

# Считываем параметры из окружения (как в каркасе)
DATASET_SLUG = os.getenv("DATASET_SLUG", "hour_data")
SOURCE_FILENAME = os.getenv("SOURCE_FILENAME", "hour.csv")
S3_CONN_ID = os.getenv("S3_CONN_ID", "s3_conn")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")

# Путь к файлу внутри контейнера Airflow
LOCAL_FILE_PATH = f"/opt/airflow/project/data/source/{SOURCE_FILENAME}"

with DAG(
    dag_id="ingest_raw",
    description="ЛР № 2: Загрузка локального снимка в raw-слой",
    start_date=datetime(2026, 9, 16),
    end_date=datetime(2026, 12, 31), 
    schedule="@daily",
    catchup=False,
    tags=["raw"],
) as dag:

    # 1. Сенсор: ждем появления файла 
    wait_for_primary_source = FileSensor(
        task_id="wait_for_primary_source",
        filepath=LOCAL_FILE_PATH,
        poke_interval=60,
        timeout=600,
    )

    # 2. Загрузка в S3
    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_filename": SOURCE_FILENAME,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": S3_CONN_ID,
        },
    )

    wait_for_primary_source >> load_to_raw_task