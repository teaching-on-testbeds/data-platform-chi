
::: {.cell .markdown}

## Persist aggregated history to Iceberg

In the real-time stage, we built low-latency features from streaming events. That is good for online risk scoring, but training needs durable history that we can recompute consistently.

In this lab design, reconstructing view-related history from PostgreSQL is effectively impossible, because we do not store every raw view event there. PostgreSQL keeps current application state (for example total view counters), not complete per-view event history.

For comments and flags, retrospective counting from PostgreSQL is possible in principle. But we still prefer not to depend on that for training, because repeated time-window queries over large tables are expensive and can drift from the online feature logic.

This creates a model-training problem: we want feature values at arbitrary anchor times after upload (for example at upload time, +5 minutes, +30 minutes), but current-state tables alone do not reliably provide that.

So in this stage, we persist aggregated event history to Iceberg tables. We will materialize window aggregates at 5-minute intervals and use those durable aggregates as a stable source for training features.

:::

::: {.cell .markdown}

### Step 1: Start batch workflow services (Airflow)

We will use Airflow to orchestrate batch jobs that write to Iceberg. Bring up Airflow, along with Nimtable for viewing Iceberg tables:

```bash
# run on node-data
docker compose -f /home/cc/data-platform-chi/docker/docker-compose.yaml up -d \
  airflow-init airflow-webserver airflow-scheduler nimtable nimtable-web
```

The first startup can take a while. We use Airflow to orchestrate data workflows, and the Airflow image must include libraries for Iceberg and storage/database access (for example PyIceberg, Arrow, and related clients). That setup happens during the container image build, so initial bring-up is slower than the earlier services.

Open Airflow:

* `http://A.B.C.D:8080`
* username: `admin`
* password: `gourmetgram_airflow`

After login:

1. Click DAGs in the top navigation if you are not already on the DAG list page.
2. Confirm you can see both pipeline DAGs:

* `redpanda_event_aggregation`
* `moderation_training`

At this point they may appear paused (toggle off). That is expected before we trigger runs in the next steps.



:::

::: {.cell .markdown}

### Step 2: Run event aggregation DAG first

The `redpanda_event_aggregation` DAG reads `views`, `comments`, and `flags` events from Redpanda, groups them into 5-minute windows, and writes those window counts to Iceberg tables in `event_aggregations`.

By default, this DAG is scheduled hourly (every hour on the hour). In production we would normally let the scheduler run it at those boundaries, but in this lab we trigger it early so we can see the full flow immediately.

This DAG also keeps a watermark in an Airflow Variable (`redpanda_event_aggregation_checkpoint`) using Kafka offsets per topic partition. On the first run, if that Variable does not exist yet, the DAG performs a historical backfill from earliest available offsets. After a successful run, it saves the next offsets as the new watermark so later runs continue incrementally.

In Airflow UI:

1. Open DAG `redpanda_event_aggregation`
2. Trigger a run by clicking the  ▶
3. Open the DAG run (for example from Recent Tasks/Graph) and wait until tasks are green (success).

If this is your first time running a DAG in Airflow, inspect it step by step:

1. Open the DAG, then click Graph to see task order and dependencies.
2. Click one task box (for example `consume_and_aggregate_events`).
3. In the task popup, click Logs to open that task's execution log.
4. Repeat for each task (`write_view_windows`, `write_comment_windows`, `write_flag_windows`, `commit_checkpoint`) so you can see what each step read/wrote.
5. Use Grid view to quickly check retries, durations, and final status across all tasks in the run.

This DAG is structured as:

```python
dag = DAG(
    'redpanda_event_aggregation',
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

t1_consume >> t2_write_views >> t3_write_comments >> t4_write_flags >> t5_commit_checkpoint
```

Watermark/backfill logic in this DAG is:

```python
CHECKPOINT_VAR = "redpanda_event_aggregation_checkpoint"

checkpoint = Variable.get(CHECKPOINT_VAR, default_var=None, deserialize_json=True)
if checkpoint is None:
    # First run: historical backfill
    start_offsets = consumer.beginning_offsets(partitions)
else:
    # Incremental runs: resume from saved watermark
    start_offsets = checkpoint["offsets"]

# Freeze a run boundary so this run is deterministic
end_offsets = consumer.end_offsets(partitions)

# Consume [start_offset, end_offset) for each topic partition
# ...aggregate into 5-minute windows...

# Only after all Iceberg writes succeed:
Variable.set(CHECKPOINT_VAR, {"offsets": next_offsets}, serialize_json=True)
```

Here:

* `t1_consume`: reads `views/comments/flags` events from the current watermark up to this run boundary and bins them into 5-minute windows.
* `t2_write_views`, `t3_write_comments`, `t4_write_flags`: write those windowed aggregates to Iceberg tables.
* `t5_commit_checkpoint`: saves the next Kafka offsets to the Airflow Variable watermark after all writes succeed.

This DAG reads streaming events and writes 5-minute aggregate windows to Iceberg tables.

After this DAG succeeds, we should be able to confirm new Iceberg window tables:

* `event_aggregations.view_windows_5min`
* `event_aggregations.comment_windows_5min`
* `event_aggregations.flag_windows_5min`

Check these in Nimtable:  Open `http://A.B.C.D:3000` and log in (`admin` / `gourmetgram_nimtable`).

In Nimtable, connect the Iceberg catalog, which is hosted in PostgreSQL:

1. From the left sidebar, click Catalogs.
2. Click Add Catalog -> Connect Catalog.
3. Fill in:
   * Connection Method: `JDBC + S3`
   * Catalog Name: `gourmetgram`
   * Catalog Endpoint: `jdbc:postgresql://postgres:5432/iceberg_catalog`
   * Warehouse: `s3://gourmetgram-datalake/warehouse`
   * S3 Endpoint: `http://minio:9000`
   * Access Key: `admin`
   * Secret Key: `gourmetgram_minio`
4. Open Advanced Options and add:
   * `jdbc.user` = `user`
   * `jdbc.password` = `gourmetgram_postgres`
5. Click Connect Catalog.

Then, 

1. Open Catalogs -> `gourmetgram`.
2. Open namespace `event_aggregations`.
3. Confirm the three `*_windows_5min` tables are visible.

We are just browsing a catalog of data that is in the lakehouse, but the underlying storage is in MinIO.

To see the underlying storage right after this Redpanda aggregation run:

1. Open MinIO Console at `http://A.B.C.D:9001` and log in (`admin` / `gourmetgram_minio`).
2. Click Object Browser, then open bucket `gourmetgram-datalake`.
3. Open folder `warehouse/event_aggregations/` and then one table folder such as `view_windows_5min/`.
4. Open `data/` to see Parquet files that store the data.
5. Open `metadata/` to see Iceberg metadata files (schemas, snapshots, file manifests).

Also inspect the watermark Variable in PostgreSQL via Adminer:

1. Open Adminer at `http://A.B.C.D:5050`.
2. Login with:
   * System: `PostgreSQL`
   * Server: `postgres`
   * Username: `user`
   * Password: `gourmetgram_postgres`
   * Database: `airflow`
3. Open table `variable`.
4. Click Select data.
5. Add filter `key = redpanda_event_aggregation_checkpoint`.
6. Click Select.

How to interpret the row:

* `id`: internal primary key for this Airflow Variable row.
* `key`: Variable name (`redpanda_event_aggregation_checkpoint`).
* `val`: stored Variable value.
* `description`: optional note (often `NULL`).
* `is_encrypted`: whether Airflow encrypted `val` with its Fernet key.

Assuming `is_encrypted` is `1`, so `val` appears as encrypted text in Adminer. To view the decoded JSON watermark payload (per-topic-partition offsets), query through Airflow itself:

```bash
# run on node-data
docker exec airflow-webserver airflow variables get redpanda_event_aggregation_checkpoint
```

:::

::: {.cell .markdown}

### Step 3: Run main training-data DAG

The `moderation_training` DAG builds the model-training dataset. It reads application data from PostgreSQL, joins in the 5-minute Iceberg aggregates we just produced, computes training features at multiple decision points, and writes the final dataset to Iceberg (`moderation.training_data`).

This DAG is scheduled daily. In normal operation we would let the daily schedule handle it, but in this lab we trigger it now so we can validate the full batch path end to end.

Its task flow is:

```python
t1_extract_data >> t2_transform_features >> t3_load_iceberg
```

Here:

* `t1_extract_data`: snapshots `users`, `images`, `comments`, and `flags` from PostgreSQL into MinIO raw parquet paths.
* `t2_transform_features`: applies governance filters, combines application snapshots with Iceberg window tables (`event_aggregations.*_windows_5min`), and computes training features and labels.
* `t3_load_iceberg`: writes the resulting training dataset to Iceberg table `moderation.training_data`.

In `t2_transform_features`, we also enforce training data policy by only selecting eligible candidates for training. For example:

```python
# examples from the DAG logic
# exclude test accounts
uploader_is_test = images['is_test_account'].fillna(False)
# exclude content from minors
uploader_is_child = images['year_of_birth'].notna() & (
    (images['created_at'].dt.year - images['year_of_birth']) < 18
)
# exclude deleted content
image_is_deleted = images['deleted_at'].notna()

eligible_images = images[(~uploader_is_test) & (~uploader_is_child) & (~image_is_deleted)]
```

We apply similar policy filters for comment/flag data, then create labels and features from the filtered data.

> Although it is not implemented in this lab, we should think about what would happen if content is deleted after it already entered `moderation.training_data`. We do not need a full table rebuild:  Iceberg supports row-level correction workflows through snapshot updates: the pipeline can maintain exclusion keys (for example deleted `image_id` values), then execute an Iceberg `DELETE` or equivalent rewrite that removes matching rows in a new snapshot. Training readers use the latest snapshot, so excluded rows are no longer visible in current scans while table history is still preserved.

In Airflow UI:

1. Open DAG `moderation_training`
2. Trigger a run
3. Open the run and wait until tasks are green (success).

This builds the training dataset by combining application data and windowed aggregates, then writes output to Iceberg.

After this DAG succeeds, verify the training table in Nimtable and MinIO.

In Nimtable:

1. Open `http://A.B.C.D:3000` and log in (`admin` / `gourmetgram_nimtable`).
2. Open Catalogs -> `gourmetgram`.
3. Open namespace `moderation`.
4. Open table `training_data`.
5. Confirm schema and preview rows are visible.

In MinIO:

1. Open `http://A.B.C.D:9001` and log in (`admin` / `gourmetgram_minio`).
2. Open bucket `gourmetgram-datalake` -> `warehouse/moderation/training_data/`.
3. Confirm both `data/` and `metadata/` folders contain files.
4. Optionally compare timestamps to confirm new files were written by this run.

:::
