from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging

default_args = {
    'owner': 'gourmetgram',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'moderation_training',
    default_args=default_args,
    description='ETL pipeline for moderation model training data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def extract_data(**kwargs):
    logging.info("Extracting data from Postgres...")
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import s3fs
    
    # Tables to extract
    tables = ['users', 'images', 'comments', 'flags']
    
    # Postgres Hook
    pg_hook = PostgresHook(postgres_conn_id='gourmetgram_postgres')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # MinIO (S3) configuration
    s3_endpoint = 'http://minio:9000'
    s3 = s3fs.S3FileSystem(
        key='admin',
        secret='gourmetgram_minio',
        client_kwargs={'endpoint_url': s3_endpoint}
    )
    bucket = 'gourmetgram-datalake'
    
    # Create bucket if not exists
    if not s3.exists(bucket):
        s3.mkdir(bucket)
    
    # Extraction Loop
    for table in tables:
        logging.info(f"Extracting table: {table}")
        df = pd.read_sql_table(table, engine)
        
        # Add extraction metadata? No, just raw dump for now.
        
        # Define output path
        # Using a fixed path for simplicity in this batch job
        output_path = f"s3://{bucket}/raw/{table}/latest.parquet"
        
        logging.info(f"Writing {len(df)} rows to {output_path}")
        df.to_parquet(
            output_path,
            index=False,
            storage_options={
                "key": "admin",
                "secret": "gourmetgram_minio",
                "client_kwargs": {"endpoint_url": s3_endpoint}
            }
        )


def transform_features(**kwargs):
    logging.info("Transforming features...")
    import pandas as pd
    import s3fs
    import numpy as np

    s3_endpoint = 'http://minio:9000'
    bucket = 'gourmetgram-datalake'
    storage_options = {
        "key": "admin",
        "secret": "gourmetgram_minio",
        "client_kwargs": {"endpoint_url": s3_endpoint}
    }

    # Load Raw Data from MinIO
    logging.info("Loading raw data from MinIO...")
    images = pd.read_parquet(f"s3://{bucket}/raw/images/latest.parquet", storage_options=storage_options)
    users = pd.read_parquet(f"s3://{bucket}/raw/users/latest.parquet", storage_options=storage_options)
    comments = pd.read_parquet(f"s3://{bucket}/raw/comments/latest.parquet", storage_options=storage_options)
    flags = pd.read_parquet(f"s3://{bucket}/raw/flags/latest.parquet", storage_options=storage_options)

    # Ensure timestamps are datetime
    images['created_at'] = pd.to_datetime(images['created_at'], utc=True)
    comments['created_at'] = pd.to_datetime(comments['created_at'], utc=True)
    flags['created_at'] = pd.to_datetime(flags['created_at'], utc=True)
    users['created_at'] = pd.to_datetime(users['created_at'], utc=True)

    # Merge images with users for user features
    images = images.merge(users[['id', 'created_at']], left_on='user_id', right_on='id', suffixes=('', '_user'))
    images.rename(columns={'created_at_user': 'user_created_at'}, inplace=True)

    # Load windowed aggregations from Iceberg
    logging.info("Loading windowed aggregations from Iceberg...")
    try:
        from pyiceberg.catalog import load_catalog
        catalog = load_catalog("gourmetgram")
        view_windows_df = catalog.load_table("event_aggregations.view_windows_5min").scan().to_pandas()
        comment_windows_df = catalog.load_table("event_aggregations.comment_windows_5min").scan().to_pandas()
        flag_windows_df = catalog.load_table("event_aggregations.flag_windows_5min").scan().to_pandas()

        # Ensure timestamps are datetime and string IDs
        view_windows_df['window_start'] = pd.to_datetime(view_windows_df['window_start'], utc=True)
        view_windows_df['window_end'] = pd.to_datetime(view_windows_df['window_end'], utc=True)
        view_windows_df['image_id'] = view_windows_df['image_id'].astype(str)

        comment_windows_df['window_start'] = pd.to_datetime(comment_windows_df['window_start'], utc=True)
        comment_windows_df['window_end'] = pd.to_datetime(comment_windows_df['window_end'], utc=True)
        comment_windows_df['image_id'] = comment_windows_df['image_id'].astype(str)

        flag_windows_df['window_start'] = pd.to_datetime(flag_windows_df['window_start'], utc=True)
        flag_windows_df['window_end'] = pd.to_datetime(flag_windows_df['window_end'], utc=True)
        flag_windows_df['image_id'] = flag_windows_df['image_id'].astype(str)

        logging.info(f"Loaded {len(view_windows_df)} view windows, {len(comment_windows_df)} comment windows, {len(flag_windows_df)} flag windows")
        use_iceberg = True
    except Exception as e:
        raise RuntimeError(f"Could not load required Iceberg window tables: {e}")

    # Initialize Feature List
    features_list = []

    # Decision Points (minutes after upload)
    decision_points = [0, 5, 30]

    logging.info(f"Processing {len(images)} images for decision points {decision_points}...")

    for idx, img in images.iterrows():
        image_id = str(img['id'])
        upload_time = img['created_at']

        # 1. Calculate Label: Flagged within 24h?
        img_flags = flags[flags['image_id'] == image_id]
        flags_24h = img_flags[
            (img_flags['created_at'] >= upload_time) &
            (img_flags['created_at'] <= upload_time + pd.Timedelta(hours=24))
        ]
        label = 1 if len(flags_24h) > 0 else 0

        # 2. Calculate Features at each Decision Point (31 features matching inference)
        for minutes in decision_points:
            cutoff_time = upload_time + pd.Timedelta(minutes=minutes)

            # Get windowed counts from Iceberg
            img_views = view_windows_df[
                (view_windows_df['image_id'] == image_id) &
                (view_windows_df['window_start'] >= upload_time) &
                (view_windows_df['window_end'] <= cutoff_time)
            ]

            img_comments_windows = comment_windows_df[
                (comment_windows_df['image_id'] == image_id) &
                (comment_windows_df['window_start'] >= upload_time) &
                (comment_windows_df['window_end'] <= cutoff_time)
            ]

            img_flags_windows = flag_windows_df[
                (flag_windows_df['image_id'] == image_id) &
                (flag_windows_df['window_start'] >= upload_time) &
                (flag_windows_df['window_end'] <= cutoff_time)
            ]

            # Last 5 minutes
            last_5min = cutoff_time - pd.Timedelta(minutes=5)
            views_5min_windows = img_views[img_views['window_end'] > last_5min]
            views_5min = int(views_5min_windows['event_count'].sum()) if not views_5min_windows.empty else 0

            comments_5min_windows = img_comments_windows[img_comments_windows['window_end'] > last_5min]
            comments_5min = int(comments_5min_windows['event_count'].sum()) if not comments_5min_windows.empty else 0

            flags_5min_windows = img_flags_windows[img_flags_windows['window_end'] > last_5min]
            flags_5min = int(flags_5min_windows['event_count'].sum()) if not flags_5min_windows.empty else 0

            # Last 1 hour
            last_1hr = cutoff_time - pd.Timedelta(hours=1)
            views_1hr_windows = img_views[img_views['window_end'] > last_1hr]
            views_1hr = int(views_1hr_windows['event_count'].sum()) if not views_1hr_windows.empty else 0

            comments_1hr_windows = img_comments_windows[img_comments_windows['window_end'] > last_1hr]
            comments_1hr = int(comments_1hr_windows['event_count'].sum()) if not comments_1hr_windows.empty else 0

            flags_1hr_windows = img_flags_windows[img_flags_windows['window_end'] > last_1hr]
            flags_1hr = int(flags_1hr_windows['event_count'].sum()) if not flags_1hr_windows.empty else 0

            total_flags = int(img_flags_windows['event_count'].sum()) if not img_flags_windows.empty else 0

            # Derived features (matching inference, without 1-min features)
            view_velocity_per_min = float(views_5min / 5.0) if views_5min > 0 else 0.0
            comment_to_view_ratio = float(comments_1hr / max(views_1hr, 1))
            # Engagement score using 5-min windows (weight comments 5x)
            recent_engagement_score = float(views_5min + (comments_5min * 5))

            # Content features
            caption = img.get('caption', '')
            caption_length = len(caption) if caption and pd.notna(caption) else 0
            has_caption = 1 if caption and pd.notna(caption) and len(caption) > 0 else 0

            # User features (filter by cutoff_time to prevent data leakage)
            user_image_count = len(images[
                (images['user_id'] == img['user_id']) &
                (images['created_at'] <= cutoff_time)
            ])
            user_created = img.get('user_created_at', upload_time)
            user_age_days = int((upload_time - user_created).total_seconds() / 86400) if pd.notna(user_created) else 0

            # Temporal features
            time_since_upload_seconds = minutes * 60
            hour_of_day = upload_time.hour
            day_of_week = upload_time.dayofweek
            is_weekend = 1 if upload_time.dayofweek >= 5 else 0

            # Category (will be one-hot encoded later)
            category = img.get('category', 'Unknown')
            if pd.isna(category):
                category = 'Unknown'

            # Build feature row
            features_list.append({
                'image_id': image_id,
                'decision_point_minutes': minutes,

                # Temporal (4)
                'time_since_upload_seconds': time_since_upload_seconds,
                'hour_of_day': hour_of_day,
                'day_of_week': day_of_week,
                'is_weekend': is_weekend,

                # Window Aggregates (6)
                'views_5min': views_5min,
                'views_1hr': views_1hr,
                'comments_5min': comments_5min,
                'comments_1hr': comments_1hr,
                'flags_5min': flags_5min,
                'flags_1hr': flags_1hr,
                'total_flags': total_flags,

                # Derived (3)
                'view_velocity_per_min': view_velocity_per_min,
                'comment_to_view_ratio': comment_to_view_ratio,
                'recent_engagement_score': recent_engagement_score,

                # Content (2)
                'caption_length': caption_length,
                'has_caption': has_caption,

                # User (2)
                'user_image_count': user_image_count,
                'user_age_days': user_age_days,

                # Category (stored as string, one-hot encoded later in training)
                'category': str(category),

                # Label
                'label_needs_moderation_24h': label
            })

    # Create DataFrame
    df_features = pd.DataFrame(features_list)

    # Log feature summary
    logging.info(f"Generated {len(df_features)} training samples")
    logging.info(f"Features: {df_features.columns.tolist()}")
    logging.info(f"Label distribution: {df_features['label_needs_moderation_24h'].value_counts().to_dict()}")

    # Save Processed Data
    output_path = f"s3://{bucket}/processed/training_data.parquet"
    logging.info(f"Saving {len(df_features)} training rows to {output_path}")

    df_features.to_parquet(
        output_path,
        index=False,
        storage_options=storage_options
    )


import os

# Path to isolated iceberg venv python (built in Dockerfile)
ICEBERG_PYTHON = '/home/airflow/iceberg_venv/bin/python.real'

# Inline script that runs inside the iceberg venv subprocess
ICEBERG_LOAD_SCRIPT = '''
import sys, json, logging

logging.basicConfig(level=logging.INFO)

# Read input from stdin
input_data = json.loads(sys.stdin.read())
input_path = input_data["input_path"]
s3_endpoint = input_data["s3_endpoint"]
s3_key = input_data["s3_key"]
s3_secret = input_data["s3_secret"]

logging.info("=== Loading training data into Iceberg ===")

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog

storage_options = {
    "key": s3_key,
    "secret": s3_secret,
    "client_kwargs": {"endpoint_url": s3_endpoint}
}

try:
    df = pd.read_parquet(input_path, storage_options=storage_options)
except Exception as e:
    logging.warning(f"No data found at {input_path} or error reading: {e}")
    sys.exit(0)

if df.empty:
    logging.info("No data to load.")
    sys.exit(0)

pa_table = pa.Table.from_pandas(df)

logging.info("Connecting to catalog...")
catalog = load_catalog("gourmetgram")

namespace = "moderation"
table_name = "training_data"
identifier = f"{namespace}.{table_name}"

try:
    catalog.create_namespace(namespace)
    logging.info(f"Namespace '{namespace}' created or exists.")
except Exception:
    pass

try:
    table = catalog.load_table(identifier)
    logging.info(f"Table {identifier} exists. Appending data...")
    table.append(pa_table)
except Exception:
    logging.info(f"Table {identifier} does not exist. Creating...")
    table = catalog.create_table(identifier, schema=pa_table.schema)
    table.append(pa_table)

logging.info(f"Successfully loaded {len(df)} rows into Iceberg table {identifier}")
'''


def load_iceberg(**kwargs):
    import json
    import subprocess

    logging.info("Loading into Iceberg via isolated subprocess...")

    s3_endpoint = 'http://minio:9000'
    bucket = 'gourmetgram-datalake'
    input_path = f"s3://{bucket}/processed/training_data.parquet"

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

    input_data = json.dumps({
        'input_path': input_path,
        's3_endpoint': s3_endpoint,
        's3_key': 'admin',
        's3_secret': 'gourmetgram_minio',
    })

    result = subprocess.run(
        [ICEBERG_PYTHON, '-I', '-c', ICEBERG_LOAD_SCRIPT],
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
            f"Iceberg load failed (exit {result.returncode}):\n{result.stderr}"
        )


t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_features',
    python_callable=transform_features,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_iceberg',
    python_callable=load_iceberg,
    dag=dag,
)

t1 >> t2 >> t3
