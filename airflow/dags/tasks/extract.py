from datetime import datetime, timezone

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
