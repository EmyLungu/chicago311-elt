from datetime import datetime, timezone
import os
from pathlib import Path

import pandas as pd
import duckdb
from airflow.hooks.base import BaseHook
from sodapy import Socrata

from config import (
    DUCKDB_PATH,
    CONNECTION_ID,
    DATASET_IDENTIFIER,
    BATCH_SIZE,
    BRONZE_DIR,
    DEFAULT_START_WATERMARK,
)


def get_last_watermark():
    last_watermark = DEFAULT_START_WATERMARK

    if not DUCKDB_PATH.is_file():
        return last_watermark

    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        row = conn.execute(
            "SELECT last_run_date FROM watermark_control "
            "WHERE dataset_name = ?",
            [DATASET_IDENTIFIER],
        ).fetchone()

        if row and row[0]:
            last_watermark = row[0]

    return last_watermark


def extract_to_parquet(**context) -> str | None:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn_meta = BaseHook.get_connection(CONNECTION_ID)
    app_token = conn_meta.password
    domain = conn_meta.host

    last_watermark = get_last_watermark()
    print(
        f"Starting incremental extract for {DATASET_IDENTIFIER} "
        f"from: {last_watermark}"
    )

    client = Socrata(domain, app_token=app_token, timeout=60)
    offset = 0
    all_records = []

    while True:
        batch = client.get(
            DATASET_IDENTIFIER,
            where=f"created_date > '{last_watermark}'",
            order="created_date ASC",
            limit=BATCH_SIZE,
            offset=offset,
        )

        if not batch:
            break

        all_records.extend(batch)
        if len(batch) < BATCH_SIZE:
            break

        offset += BATCH_SIZE

    client.close()

    if not all_records:
        print("No new records found!")
        return None

    df = pd.DataFrame.from_records(all_records)
    if "street_type" in df.columns:
        df["street_type"] = df["street_type"].astype(str)

    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    parquet_path = BRONZE_DIR / f"raw_311_{batch_ts}.parquet"
    df.to_parquet(parquet_path, index=False)

    print(f"Extracted {len(df)} records to {parquet_path}")

    latest_watermark = df["created_date"].max()
    context["ti"].xcom_push(
        key="extraction_metadata",
        value={
            "parquet_path": str(parquet_path),
            "latest_watermark": latest_watermark,
        },
    )

    return str(parquet_path)


def extract_from_file_to_parquet(**context):
    file_path_param = context["params"]["file_path"]
    file_path = str(Path(file_path_param).resolve())

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found at: {file_path}")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    target_parquet_path = str(
        BRONZE_DIR / f"dataset_batch_{context['ds_nodash']}.parquet"
    )

    with duckdb.connect() as conn:
        conn.execute("SET max_memory='4GB';")

        if file_path.endswith(".csv"):
            conn.execute(
                f"""
                COPY (SELECT * FROM read_csv_auto(?, ignore_errors=true))
                TO '{target_parquet_path}'
                (FORMAT PARQUET, COMPRESSION 'SNAPPY', PER_THREAD_OUTPUT FALSE);
                """,
                [file_path],
            )
        else:
            target_parquet_path = file_path

        row_count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{target_parquet_path}')"
        ).fetchone()[0]

    context["ti"].xcom_push(
        key="extraction_metadata",
        value={"parquet_path": target_parquet_path, "row_count": row_count},
    )
