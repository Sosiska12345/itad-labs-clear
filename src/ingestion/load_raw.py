"""Загрузить локальный файл и сохранить его без изменений в raw."""

from __future__ import annotations

import logging
import os
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_BUCKET = "raw"


def load_to_raw(
    *,
    source_filename: str,
    dataset_slug: str,
    s3_conn_id: str,
    **context: Any,
) -> None:
    """Загрузить исходный файл из локальной папки или оставить уже созданный raw-объект."""
    
    # Берем логическую дату из контекста Airflow 
    ds = context['ds']
    key = f"{dataset_slug}/ingested_on={ds}/{source_filename}"
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    # 1. Проверяем бакет (и НЕ создаем его)
    if not s3_hook.check_for_bucket(bucket_name=RAW_BUCKET):
        raise ValueError(
            f"Бакет {RAW_BUCKET} не найден. Убедитесь, что запустили docker compose up -d"
        )

    # 2. Проверяем, нет ли уже файла (Идемпотентность)
    if s3_hook.check_for_key(key=key, bucket_name=RAW_BUCKET):
        logging.info(
            "Raw object already exists and will not be modified: s3://%s/%s",
            RAW_BUCKET,
            key,
        )
        return

    # 3. Читаем локальный файл
    local_path = f"/opt/airflow/project/data/source/{source_filename}"
    logging.info("Reading local file: %s", local_path)
    
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")

    with open(local_path, "rb") as f:
        file_bytes = f.read()

    # 4. Загружаем байты в S3
    s3_hook.load_bytes(
        bytes_data=file_bytes,
        key=key,
        bucket_name=RAW_BUCKET,
        replace=False,
    )
    logging.info("Saved local file %s to s3://%s/%s", source_filename, RAW_BUCKET, key)