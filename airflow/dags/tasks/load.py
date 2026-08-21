from config import DUCKDB_PATH, DATASET_IDENTIFIER

import duckdb


def load_parquet_to_duckdb(**context) -> None:
    ti = context["ti"]
    meta = ti.xcom_pull(key="extraction_metadata", task_ids="extract_bronze")

    if not meta:
        print("No parquet file to load")
        return

    parquet_path = meta["parquet_path"]
    latest_watermark = meta.get("latest_watermark")

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(DUCKDB_PATH)) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.raw_chicago311 AS
            SELECT * FROM read_parquet(?, union_by_name=True) WHERE 1=0;
            """,
            [parquet_path],
        )

        conn.execute(
            """
            INSERT INTO bronze.raw_chicago311 BY NAME
            SELECT * FROM read_parquet(?, union_by_name=True);
            """,
            [parquet_path],
        )

        if latest_watermark:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS watermark_control (
                dataset_name VARCHAR PRIMARY KEY,
                last_run_date VARCHAR NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            conn.execute(
                """
                INSERT INTO watermark_control
                    (dataset_name, last_run_date, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (dataset_name) DO UPDATE SET
                    last_run_date = EXCLUDED.last_run_date,
                    updated_at = EXCLUDED.updated_at;
                """,
                [DATASET_IDENTIFIER, latest_watermark],
            )

            print(f"New watermark: {latest_watermark}")

        print(f"Loaded {parquet_path} to DuckDB.")


def validate_ingestion() -> None:
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        table_exists = conn.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'bronze' AND table_name = 'raw_chicago311'
            """).fetchone()[0]

        if not table_exists:
            raise RuntimeError("Missing table bronze.raw_chicago311")

        count = conn.execute(
            "SELECT count(*) FROM bronze.raw_chicago311;"
        ).fetchone()[0]
        if count == 0:
            raise RuntimeError("bronze.raw_chicago311 is empty")

        null_srs = conn.execute(
            "SELECT count(*) FROM bronze.raw_chicago311 "
            "WHERE sr_number IS NULL;"
        ).fetchone()[0]

        if null_srs > 0:
            raise ValueError(
                f"Data Quality Failure: {null_srs} records "
                "have NULL sr_number."
            )

        print(
            f"Ingestion validated: {count} records in bronze.raw_chicago311."
        )
