# Batch ETL — Airflow

Airflow DAGs that run scheduled batch jobs to extract platform activity from Postgres and Kafka, transform it into training features, and load everything into Apache Iceberg tables stored in MinIO. This is the data pipeline that feeds the model training notebook.

## What It Does

Three DAGs:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `redpanda_event_aggregation` | Every hour | Consumes the last hour of events from Kafka, aggregates into 5-min windows, writes to Iceberg |
| `etl_moderation` | Daily | Extracts Postgres tables → computes 26 training features → loads into Iceberg `moderation.training_data` |
| `verify_pipeline` | Manual | Validates the Iceberg table exists and prints row count + sample data |

---

## DAG 1: `redpanda_event_aggregation`

Reads raw view, comment, and flag events from RedPanda (Kafka), groups them into 5-minute windows by image, and writes the aggregated counts to Iceberg.

**Tasks:**
1. `consume_and_aggregate_events` — pulls last 1 hour of events from Kafka topics
2. `write_view_windows` — writes aggregated view windows to Iceberg
3. `write_comment_windows` — writes aggregated comment windows to Iceberg
4. `write_flag_windows` — writes aggregated flag windows to Iceberg

**Output tables** (Iceberg):
- `event_aggregations.view_windows_5min`
- `event_aggregations.comment_windows_5min`
- `event_aggregations.flag_windows_5min`

Schema: `image_id`, `window_start`, `window_end`, `event_count`, `processed_at`

---

## DAG 2: `etl_moderation` (main training pipeline)

The core ETL. Pulls all platform data from Postgres, engineers 26 features per image, and loads a labeled training dataset into Iceberg.

**Tasks:**
1. `extract_data` — dumps `users`, `images`, `comments`, `flags`, `image_milestones` tables from Postgres to MinIO as Parquet files (`s3://gourmetgram-datalake/raw/{table}/latest.parquet`)
2. `transform_features` — joins the tables, computes features at decision points (0, 5, 30 minutes after upload), assigns label = "flagged within 24 hours"
3. `load_iceberg` — writes processed data to Iceberg table `moderation.training_data`

**Output table** (Iceberg): `moderation.training_data`

The `load_iceberg` task runs in an **isolated virtualenv** to avoid a SQLAlchemy version conflict between Airflow 2.x (requires `<2.0`) and PyIceberg (requires `>=2.0`).

---

## Iceberg Catalog

| Setting | Value |
|---------|-------|
| Type | SQL (Postgres-backed) |
| Catalog name | `gourmetgram` |
| Warehouse | `s3://gourmetgram-datalake/warehouse` |
| Namespaces | `moderation`, `event_aggregations` |

---

## Input / Output Summary

| What | From | To |
|------|------|-----|
| Postgres tables | `gourmetgram` DB | MinIO Parquet (`raw/`) |
| Kafka events | `gourmetgram.views/comments/flags` topics | Iceberg windowed tables |
| Training features | MinIO Parquet | Iceberg `moderation.training_data` |

---

## How to Run

**1. Access Airflow UI** — `http://localhost:8080` (login: `admin` / `admin`)

**2. Trigger DAGs manually in this order:**
- First: `redpanda_event_aggregation` (populates windowed event data)
- Then: `etl_moderation` (builds the training dataset)

**3. Verify the Iceberg table was populated:**
```bash
docker exec airflow-scheduler python /opt/airflow/dags/verify_pipeline.py
```

---

## Known Issues

- **UniqueViolation on `load_iceberg`**: Happens if two DAG runs execute simultaneously. Clear the failed task and re-run with only one active run.
- **Dependency conflicts**: If you modify the `load_iceberg` task, update the `requirements` list *inside* `etl_moderation.py`, not the top-level `requirements.txt`.

---

## Files

```
batch-etl-airflow/
  Dockerfile
  requirements.txt
  dags/
    etl_moderation.py            # main daily ETL DAG
    redpanda_event_aggregation.py  # hourly Kafka windowing DAG
    verify_pipeline.py           # manual Iceberg validation script
```
