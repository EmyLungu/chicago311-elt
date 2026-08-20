import os
from pathlib import Path

from cosmos import (
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import TestBehavior

DBT_PROJECT_PATH = Path("/opt/airflow/chicago311dbt")
DBT_PROFILES_YML = DBT_PROJECT_PATH / "profiles.yml"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

profile_config = ProfileConfig(
    profile_name="chicago311dbt",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_YML,
)

execution_config = ExecutionConfig(
    dbt_executable_path="dbt",
)

render_config = RenderConfig(
    test_behavior=TestBehavior.AFTER_ALL,
    select=["path:seeds", "path:models", "path:snapshots"],
    exclude=["example"],
)

default_args = {
    "owner": "chicago-data",
    "depends_on_past": False,
    "retries": 1,
}

DUCKDB_PATH = Path(
    os.environ.get("DUCKDB_PATH", "/opt/airflow/data/chicago311.duckdb")
)

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
BRONZE_DIR = DATA_DIR / "bronze"
DUCKDB_PATH = Path(
    os.environ.get("DUCKDB_PATH", "/opt/airflow/data/chicago311.duckdb")
)

DATASET_IDENTIFIER = "v6vf-nfxy"
DEFAULT_START_WATERMARK = "2026-08-12T00:00:00.000"

CONNECTION_ID = "socrata_chicago311"
BATCH_SIZE = 50000
