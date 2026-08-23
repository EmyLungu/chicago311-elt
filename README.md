# Project Documentation: Chicago 311 Service Requests ELT Pipeline

##### Project for the Data Engineering Summer Practice 2026 @ Levi9
##### Author: Lungu Emanuel-Leonard


## Executive Summary
This project delivers an automated, end-to-end ELT data pipeline for Chicago 311 service requests using Apache Airflow, DuckDB, and dbt. The architecture handles API/file ingestion, enforces data quality tests, builds an incremental star schema, and models slowly changing dimensions (SCD Type 2) to track historical ward boundary updates and request status lifecycle changes.

---

## Dataset Description & Scope
* **Source:** [City of Chicago Open Data Portal](https://data.cityofchicago.org/Service-Requests/311-Service-Requests/v6vf-nfxy/about_data) (Socrata API) / Historical CSV Batch Files.
* **Volume & Refresh:** Daily incremental updates tracking millions of historical 311 service requests across Chicago.
* **Scoping Decisions:** Source records are standardized and ingested into Parquet files before loading into DuckDB. High-frequency metrics focus on resolution times, duplicate/reopened counts, and ward redistricting shifts.

---

## Architecture Overview
The pipeline uses **Apache Airflow** to orchestrate extraction, ingestion, and validation. Raw data lands in Parquet files in `data/bronze` and is ingested into **DuckDB**. Transformations, SCD Type 2 snapshots, and data quality tests are managed via **dbt (cosmos)**.

---

## Data Modeling Explanation
The mart is structured as a Star Schema centered on the `fct_service_requests` table.

* **`fct_service_requests` (Fact Table)**
  * **Grain:** One row per individual 311 Service Request (`sr_number`).
  * **Role:** Stores request resolution duration metrics (`resolution_time_hours`, `resolution_time_days`) and foreign keys to dimension tables. Uses an incremental `merge` strategy on `sr_number`.
* **`dim_ward` (SCD Type 2 Dimension)**
  * **Grain:** One row per Ward ID per Map Version (`ward_map_key`).
  * **Role:** Tracks historical ward boundary redistricting (e.g., 2023 map updates). Requests join against the map version active at their `created_date`.
* **`dim_location` (Dimension)**
  * **Grain:** Unique spatial grouping (`community_area_id`, `zip_code`, `police_beat`).
  * **Role:** Deduplicates geographic and administrative boundary metadata.
* **`dim_service_type` (Dimension)**
  * **Grain:** Unique service type classification (`sr_type`, `sr_short_code`).
  * **Role:** Normalizes department ownership and categorization hierarchy.
* **`dim_date` (Dimension)**
  * **Grain:** One row per calendar day generated via date spine.

---

## Airflow DAG & Pipeline Explanation

The workflow is implemented using two Airflow DAGs:

### 1. `chicago311pipeline` (Automated Daily ELT)
* **`start` / `finish`:** `EmptyOperator` nodes framing pipeline execution.
* **`extract_bronze`:** Pulls incremental API records into Parquet format using a stored watermark.
* **`check_extraction`:** `BranchPythonOperator` that proceeds to ingestion if a valid Parquet path exists, or skips to `finish` if no new data was fetched.
* **`load_bronze`:** Loads Parquet files into the DuckDB `raw_chicago311` table.
* **`validate_ingestion`:** Validates schema and row counts prior to transformation.
* **`dbt_transform`:** `DbtTaskGroup` running staging models, snapshots, dimensions, incremental facts, and dbt quality tests.

### 2. `chicago311_manual_pipeline` (Manual Pipeline)
* Triggered manually (`schedule=None`) with parameter `file_path` to process local bulk CSV uploads.

---

## dbt Models, Snapshots & Data Quality Tests

### Staging & Snapshots
* **`stg_chicago311`:** Standardizes text field casing (`UPPER(TRIM())`), parses timestamps, formats booleans, and replaces blank strings with `NULL`.
* **`snp_dim_ward`:** SCD Type 2 snapshot tracking ward map versions (`effective_from`, `effective_to`).
* **`snp_service_requests`:** Snapshot tracking changes in status and resolution timestamps over time.

### Implemented dbt Tests
1. **Uniqueness & Non-Null:** Applied to `sr_number` on `stg_chicago311` and `fct_service_requests`.
2. **Referential Integrity:** Foreign key checks connecting `fct_service_requests` to `dim_ward`, `dim_location`, and `dim_service_type`.
3. **Domain Validity Check (`closed_date >= created_date`):** Custom SQL test ensuring resolution date does not precede creation date.
4. **Coordinate Bounding Test:** Custom test ensuring `latitude` (41.6 to 42.1) and `longitude` (-87.9 to -87.5) reside within Chicago boundaries.
5. **`community_area_id` Non-Null:** Set to `severity: warn` to flag unmapped legacy records without failing the pipeline.

---

## How to Run

### 1. Prerequisites
* **Docker** installed and running on your host machine.
* A free **Socrata App Token** registered in the Airflow UI under **Admin → Connections** (`socrata_chicago311`) to avoid API rate limiting.
(set up the `.env` and `docker compose up -d` before, to start Airflow)
2. **Dataset directory:** Put the downlowed `.csv` in the `./data/datasets/` directory to (Optional).
3. **Config file** Change the `DEFAULT_START_WATERMARK` constant to your demand in `airflow/dags/config.py`
3. **Manual DBT Execution:**
```
cd /opt/airflow/chicago311dbt

dbt snapshot
dbt run
dbt test
dbt docs generate
dbt docs serve --port 8081
```

# Deliverable Question Solutions & Findings

**Q1: Resolution Speeds Across Community Areas**
* Model Used: `fct_service_requests` joined with `dim_location`, `dim_service_type`, and `dim_date`.

* Analysis: Aggregates `MEDIAN(resolution_time_days)` grouped by `community_area_id` and `date_year` for pothole and graffiti request types to compute year-over-year changes and operational trends.

* Finding: Community areas 55 (Hegewisch) and 74 (Mount Greenwood) recorded the longest median resolution times, taking approximately 282.1 days and 255.1 days respectively in 2018. Outlying South and Far Southwest side districts consistently experienced longer resolution windows compared to central areas.

q1_slowest_resolution_community_areas:
| community_area_id | current_year | current_year_medi... | previous_year_med... | yoy_change_days | trend     |
| ----------------- | ------------ | -------------------- | -------------------- | --------------- | --------- |
|                55 |         2018 |               282.10 |                      |                 | Unchanged |
|                74 |         2018 |               255.13 |                      |                 | Unchanged |
|                21 |         2018 |               221.44 |                      |                 | Unchanged |
|                76 |         2018 |               220.75 |                      |                 | Unchanged |
|                47 |         2018 |               218.13 |                      |                 | Unchanged |
...|    ...         | ... | ...    |   ...  |   ...     |
|                25 |         2019 |                50.42 |               145.08 |          -94.67 | Improved  |
|                47 |         2024 |                46.04 |                 4.00 |           42.04 | Worsened  |
|                18 |         2019 |                42.50 |               143.04 |         -100.54 | Improved  |
|                76 |         2019 |                40.10 |               220.75 |         -180.65 | Improved  |
|                47 |         2019 |                38.06 |               218.13 |         -180.06 | Improved  |



**Q2: Duplicate & Reopened Complaint Ratios**
* Model Used: `fct_service_requests` joined with `dim_service_type`.

* Analysis: Uses the LAG() window function partitioned by service request type and street address to detect repeated complaints logged within a 3-day window, calculating total versus short-window duplicate requests.

* Finding: Aircraft Noise Complaints and 311 Information Only requests recorded near 100% duplicate ratios (~2.54M and ~5.10M duplicates respectively) due to repeated automated/high-volume logging at centralized locations. Among actionable field infrastructure issues, Open Fire Hydrants (38.8%) and Traffic Signal Outages (31.7%) generated the highest duplicate proportions.

q2_highest_duplicate_complaints:
| sr_type              | total_requests | duplicate_requests | duplicate_ratio |
| -------------------- | -------------- | ------------------ | --------------- |
| AIRCRAFT NOISE CO... |        2545499 |            2544283 |          1.000… |
| 311 INFORMATION O... |        5105829 |            5100127 |          0.999… |
| OPEN FIRE HYDRANT... |          52513 |              20387 |          0.388… |
| DIVVY BIKE PARKIN... |          14495 |               5139 |          0.354… |
| TRAFFIC SIGNAL OU... |         184969 |              58713 |          0.317… |

**Q3: Impact of 2023 Ward Redistricting**

* Model Used: `fct_service_requests` joining `dim_ward` via `ward_map_key` and `dim_date`.

* Analysis: Filters requests between 2022 and 2026 and separates volume by the effective map version (2015_MAP vs. 2023_MAP) tied to each request at creation time. Aggregates total requests per ward under each map version to compute the net volume shift (vol_2023_map - vol_2015_map).

* Finding: Comparing requests logged under the historical 2015_MAP versus the new 2023_MAP reveals a dramatic upward shift in recorded service requests across all top wards following the redistricting rollout. Ward 1 saw the largest shift, moving from 29,046 requests under the 2015 map to 76,509 under the 2023 map (a net increase of +47,463 requests). Wards 2 through 5 similarly experienced request volume increases of 120% to 160% under the updated boundaries, reflecting expanded geographical jurisdictions or higher request density within the newly redrawn ward lines.

q3_ward_redistricting_comparison:
| ward_id | historical_map_vo... | current_map_volume | volume_shift |
| ------- | -------------------- | ------------------ | ------------ |
|       1 |                29046 |              76509 |        47463 |
|       2 |                13854 |              31550 |        17696 |
|       3 |                16866 |              42107 |        25241 |
|       4 |                15794 |              40959 |        25165 |
|       5 |                15115 |              38502 |        23387 |
