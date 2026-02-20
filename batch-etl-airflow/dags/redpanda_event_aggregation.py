from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import logging
import json

# DAG default arguments
default_args = {
    'owner': 'gourmetgram',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'redpanda_event_aggregation',
    default_args=default_args,
    description='Aggregate Redpanda events into 5-minute windows for training',
    schedule_interval=timedelta(hours=1),  # Run every hour
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
)

# Path to isolated iceberg venv python (built in Dockerfile)
ICEBERG_PYTHON = '/home/airflow/iceberg_venv/bin/python.real'
CHECKPOINT_VAR = 'redpanda_event_aggregation_checkpoint'


def load_checkpoint():
    checkpoint = Variable.get(CHECKPOINT_VAR, default_var=None, deserialize_json=True)
    if checkpoint is None:
        return None
    if not isinstance(checkpoint, dict):
        logging.warning("Checkpoint variable is not a JSON object. Ignoring and bootstrapping from earliest.")
        return None
    if 'offsets' not in checkpoint:
        logging.warning("Checkpoint variable missing 'offsets'. Ignoring and bootstrapping from earliest.")
        return None
    return checkpoint


def save_checkpoint(offsets, run_id):
    payload = {
        'version': 1,
        'offsets': offsets,
        'updated_at': datetime.utcnow().isoformat() + 'Z',
        'last_run_id': run_id,
        'bootstrap': 'earliest',
    }
    Variable.set(CHECKPOINT_VAR, payload, serialize_json=True)

# Inline script that runs inside the iceberg venv subprocess
ICEBERG_WRITE_SCRIPT = '''
import sys, json, logging

logging.basicConfig(level=logging.INFO)

# Read input from stdin
input_data = json.loads(sys.stdin.read())
table_name = input_data["table_name"]
records = input_data["records"]

logging.info(f"=== Writing {table_name} to Iceberg ===")

if not records:
    logging.info("No data to write")
    sys.exit(0)

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog

df = pd.DataFrame(records)
df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
df["processed_at"] = pd.to_datetime(df["processed_at"], utc=True)
pa_table = pa.Table.from_pandas(df)

catalog = load_catalog("gourmetgram")
namespace = "event_aggregations"
identifier = f"{namespace}.{table_name}"

try:
    catalog.create_namespace(namespace)
except Exception:
    pass

try:
    table = catalog.load_table(identifier)
    table.append(pa_table)
except Exception:
    schema = pa.schema([
        pa.field("image_id", pa.string()),
        pa.field("window_start", pa.timestamp("us", tz="UTC")),
        pa.field("window_end", pa.timestamp("us", tz="UTC")),
        pa.field("event_count", pa.int64()),
        pa.field("processed_at", pa.timestamp("us", tz="UTC")),
    ])
    table = catalog.create_table(identifier, schema=schema)
    table.append(pa_table)

logging.info(f"Wrote {len(df)} rows to {identifier}")
'''


def consume_and_aggregate_events(**kwargs):
    from kafka import KafkaConsumer
    from collections import defaultdict
    from datetime import datetime, timezone

    logging.info("=== Starting Redpanda Event Aggregation ===")

    # Initialize Kafka consumer
    bootstrap_servers = 'redpanda:9092'
    topics = ['gourmetgram.views', 'gourmetgram.comments', 'gourmetgram.flags']

    logging.info(f"Connecting to Redpanda at {bootstrap_servers}")
    logging.info(f"Subscribing to topics: {topics}")

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Manual commit after successful processing
        consumer_timeout_ms=1000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='airflow_event_aggregator'
    )

    # Assignment happens lazily
    consumer.poll(timeout_ms=1000)
    partitions = sorted(consumer.assignment(), key=lambda tp: (tp.topic, tp.partition))
    if not partitions:
        consumer.close()
        raise RuntimeError("No Kafka partitions assigned for event aggregation topics")

    logging.info(f"Assigned partitions: {partitions}")

    checkpoint = load_checkpoint()
    checkpoint_offsets = checkpoint.get('offsets', {}) if checkpoint else {}
    is_first_run = checkpoint is None

    if is_first_run:
        logging.info("No checkpoint found. Bootstrapping from earliest offsets.")
    else:
        logging.info("Loaded checkpoint from Airflow Variable '%s'.", CHECKPOINT_VAR)

    beginning_offsets = consumer.beginning_offsets(partitions)
    end_offsets = consumer.end_offsets(partitions)
    start_offsets = {}

    for tp in partitions:
        topic_offsets = checkpoint_offsets.get(tp.topic, {})
        stored = topic_offsets.get(str(tp.partition))
        start_offset = int(stored) if stored is not None else beginning_offsets.get(tp, 0)
        end_offset = end_offsets.get(tp, start_offset)

        if start_offset > end_offset:
            logging.warning(
                "Checkpoint offset %s is ahead of end offset %s for %s. Clamping to end.",
                start_offset,
                end_offset,
                tp,
            )
            start_offset = end_offset

        start_offsets[tp] = start_offset
        consumer.seek(tp, start_offset)
        logging.info(
            "Partition %s start_offset=%s end_offset=%s",
            tp,
            start_offset,
            end_offset,
        )

    # Aggregation buckets: {image_id: {bucket_start: count}}
    view_windows = defaultdict(lambda: defaultdict(int))
    comment_windows = defaultdict(lambda: defaultdict(int))
    flag_windows = defaultdict(lambda: defaultdict(int))

    # Event counters
    events_read = 0
    events_processed = 0
    events_skipped = 0

    active_partitions = {
        tp for tp in partitions if start_offsets[tp] < end_offsets.get(tp, start_offsets[tp])
    }

    if not active_partitions:
        logging.info("No new offsets to process for this run.")

    logging.info("Starting to consume events from checkpoint to run boundary...")
    empty_polls = 0

    while active_partitions:
        polled = consumer.poll(timeout_ms=1000, max_records=1000)

        if not polled:
            empty_polls += 1
        else:
            empty_polls = 0

        for _, messages in polled.items():
            for message in messages:
                events_read += 1
                event = None

                try:
                    event = message.value
                    topic = message.topic

                    # Parse timestamp from event
                    if topic == 'gourmetgram.views':
                        timestamp_str = event.get('viewed_at')
                        image_id = event.get('image_id')
                    elif topic == 'gourmetgram.comments':
                        timestamp_str = event.get('created_at')
                        image_id = event.get('image_id')
                    elif topic == 'gourmetgram.flags':
                        timestamp_str = event.get('created_at')
                        image_id = event.get('image_id')
                        # Skip comment flags (no image_id)
                        if not image_id:
                            events_skipped += 1
                            continue
                    else:
                        events_skipped += 1
                        continue

                    # Parse timestamp
                    if isinstance(timestamp_str, str):
                        # Handle ISO format with or without timezone
                        if timestamp_str.endswith('Z'):
                            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        elif '+' in timestamp_str or timestamp_str.endswith('00:00'):
                            timestamp = datetime.fromisoformat(timestamp_str)
                        else:
                            timestamp = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
                    else:
                        # Assume it's already a datetime
                        timestamp = timestamp_str
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)

                    events_processed += 1

                    # Calculate 5-minute bucket
                    # Floor to nearest 5-minute interval
                    bucket_start = timestamp.replace(
                        minute=(timestamp.minute // 5) * 5,
                        second=0,
                        microsecond=0
                    )

                    # Increment count for this (image_id, bucket) pair
                    if topic == 'gourmetgram.views':
                        view_windows[str(image_id)][bucket_start] += 1
                    elif topic == 'gourmetgram.comments':
                        comment_windows[str(image_id)][bucket_start] += 1
                    elif topic == 'gourmetgram.flags':
                        flag_windows[str(image_id)][bucket_start] += 1

                except Exception as e:
                    logging.error(f"Error processing event: {e}")
                    logging.error(f"Event: {event}")
                    events_skipped += 1
                    continue

        remaining = set()
        for tp in active_partitions:
            position = consumer.position(tp)
            if position < end_offsets[tp]:
                remaining.add(tp)
        active_partitions = remaining

        if empty_polls >= 30 and active_partitions:
            consumer.close()
            raise RuntimeError(
                f"Timed out waiting for records before run boundary. Remaining partitions: {active_partitions}"
            )

    next_offsets = {}
    for tp in partitions:
        position = consumer.position(tp)
        bounded_position = min(position, end_offsets[tp])
        bounded_position = max(bounded_position, start_offsets[tp])
        next_offsets.setdefault(tp.topic, {})[str(tp.partition)] = int(bounded_position)

    logging.info("Next checkpoint offsets: %s", next_offsets)

    consumer.close()

    logging.info(f"=== Consumption Complete ===")
    logging.info(f"Events read: {events_read}")
    logging.info(f"Events processed: {events_processed}")
    logging.info(f"Events skipped: {events_skipped}")
    logging.info(f"View windows: {sum(len(buckets) for buckets in view_windows.values())}")
    logging.info(f"Comment windows: {sum(len(buckets) for buckets in comment_windows.values())}")
    logging.info(f"Flag windows: {sum(len(buckets) for buckets in flag_windows.values())}")

    # Convert to DataFrame format for downstream tasks
    def to_records(windows_dict):
        """Convert nested dict to list of records for DataFrame"""
        rows = []
        for image_id, buckets in windows_dict.items():
            for bucket_start, count in buckets.items():
                rows.append({
                    'image_id': image_id,
                    'window_start': bucket_start.isoformat(),
                    'window_end': (bucket_start + timedelta(minutes=5)).isoformat(),
                    'event_count': count,
                    'processed_at': datetime.now(timezone.utc).isoformat()
                })
        return rows

    view_records = to_records(view_windows)
    comment_records = to_records(comment_windows)
    flag_records = to_records(flag_windows)

    logging.info(f"Pushing {len(view_records)} view records to XCom")
    logging.info(f"Pushing {len(comment_records)} comment records to XCom")
    logging.info(f"Pushing {len(flag_records)} flag records to XCom")

    # Push to XCom for downstream tasks
    ti = kwargs['ti']
    ti.xcom_push(key='view_windows', value=view_records)
    ti.xcom_push(key='comment_windows', value=comment_records)
    ti.xcom_push(key='flag_windows', value=flag_records)
    ti.xcom_push(key='next_offsets', value=next_offsets)
    ti.xcom_push(key='is_first_run', value=is_first_run)


def _run_iceberg_write(table_name, records):
    """Run the Iceberg write in an isolated subprocess using the iceberg venv."""
    import json
    import subprocess

    logging.info(f"Writing {table_name}: {len(records) if records else 0} records")

    if not records:
        logging.info(f"No {table_name} data to write")
        return

    # Build a minimal, clean environment for the subprocess
    subprocess_env = {
        'HOME': os.environ.get('HOME', '/home/airflow'),
        'PATH': '/usr/local/bin:/usr/bin:/bin',
        'PYTHONNOUSERSITE': '1',
    }
    # Pass through PyIceberg catalog config env vars
    for key, val in os.environ.items():
        if key.startswith('PYICEBERG_CATALOG__'):
            subprocess_env[key] = val

    input_data = json.dumps({'table_name': table_name, 'records': records})

    result = subprocess.run(
        [ICEBERG_PYTHON, '-I', '-c', ICEBERG_WRITE_SCRIPT],
        input=input_data,
        capture_output=True,
        text=True,
        env=subprocess_env,
    )

    # Log stdout/stderr from subprocess
    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            logging.info(f"[iceberg] {line}")
    if result.stderr:
        for line in result.stderr.strip().split('\n'):
            logging.info(f"[iceberg] {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Iceberg write failed for {table_name} (exit {result.returncode}):\n{result.stderr}"
        )


def write_view_windows(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key='view_windows', task_ids='consume_and_aggregate_events')
    _run_iceberg_write('view_windows_5min', records)


def write_comment_windows(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key='comment_windows', task_ids='consume_and_aggregate_events')
    _run_iceberg_write('comment_windows_5min', records)


def write_flag_windows(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key='flag_windows', task_ids='consume_and_aggregate_events')
    _run_iceberg_write('flag_windows_5min', records)


def commit_checkpoint(**kwargs):
    ti = kwargs['ti']
    dag_run = kwargs.get('dag_run')

    next_offsets = ti.xcom_pull(key='next_offsets', task_ids='consume_and_aggregate_events')
    if not next_offsets:
        raise RuntimeError("Missing next_offsets in XCom; refusing to advance checkpoint")

    run_id = dag_run.run_id if dag_run else 'manual_unknown_run'
    save_checkpoint(next_offsets, run_id)
    logging.info("Checkpoint committed to Airflow Variable '%s' for run %s", CHECKPOINT_VAR, run_id)


# Define tasks
t1_consume = PythonOperator(
    task_id='consume_and_aggregate_events',
    python_callable=consume_and_aggregate_events,
    dag=dag,
)

t2_write_views = PythonOperator(
    task_id='write_view_windows',
    python_callable=write_view_windows,
    dag=dag,
)

t3_write_comments = PythonOperator(
    task_id='write_comment_windows',
    python_callable=write_comment_windows,
    dag=dag,
)

t4_write_flags = PythonOperator(
    task_id='write_flag_windows',
    python_callable=write_flag_windows,
    dag=dag,
)

t5_commit_checkpoint = PythonOperator(
    task_id='commit_checkpoint',
    python_callable=commit_checkpoint,
    dag=dag,
)

# Define dependencies
# Consume events first, then write each table sequentially.
# This avoids first-run catalog table creation races in SQL catalog backends.
t1_consume >> t2_write_views >> t3_write_comments >> t4_write_flags >> t5_commit_checkpoint
