from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import logging

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
)

# Path to isolated iceberg venv python (built in Dockerfile)
ICEBERG_PYTHON = '/home/airflow/iceberg_venv/bin/python.real'

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
    import json
    from kafka import KafkaConsumer, TopicPartition
    from collections import defaultdict
    from datetime import datetime, timezone
    import pandas as pd

    logging.info("=== Starting Redpanda Event Aggregation ===")

    # Calculate time range: last hour
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    logging.info(f"Processing events from {start_time} to {end_time}")

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
        consumer_timeout_ms=30000,  # 30 second timeout for polling
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='airflow_event_aggregator'
    )

    # Get all partitions and seek to timestamp
    partitions = consumer.assignment()
    if not partitions:
        # Assignment happens lazily, trigger with poll
        consumer.poll(timeout_ms=1000)
        partitions = consumer.assignment()

    logging.info(f"Assigned partitions: {partitions}")

    # Seek to start timestamp
    start_timestamp_ms = int(start_time.timestamp() * 1000)
    offset_dict = consumer.offsets_for_times(
        {tp: start_timestamp_ms for tp in partitions}
    )

    for tp, offset_and_timestamp in offset_dict.items():
        if offset_and_timestamp:
            logging.info(f"Seeking {tp} to offset {offset_and_timestamp.offset}")
            consumer.seek(tp, offset_and_timestamp.offset)
        else:
            logging.warning(f"No offset found for {tp} at timestamp {start_time}")

    # Aggregation buckets: {image_id: {bucket_start: count}}
    view_windows = defaultdict(lambda: defaultdict(int))
    comment_windows = defaultdict(lambda: defaultdict(int))
    flag_windows = defaultdict(lambda: defaultdict(int))

    # Event counters
    events_read = 0
    events_in_range = 0
    events_skipped = 0

    logging.info("Starting to consume events...")

    for message in consumer:
        events_read += 1

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

            # Skip events outside our time range
            if timestamp < start_time:
                continue  # Haven't reached start yet
            if timestamp >= end_time:
                # We've passed the end time, stop consuming
                break

            events_in_range += 1

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

    consumer.close()

    logging.info(f"=== Consumption Complete ===")
    logging.info(f"Events read: {events_read}")
    logging.info(f"Events in time range: {events_in_range}")
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

# Define dependencies
# Consume events first, then write each table sequentially.
# This avoids first-run catalog table creation races in SQL catalog backends.
t1_consume >> t2_write_views >> t3_write_comments >> t4_write_flags
