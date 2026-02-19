FROM apache/airflow:2.11.0

USER root
# Install system dependencies if needed (e.g. for psycopg2)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Create isolated venv for pyiceberg (needs sqlalchemy>=2.0, incompatible with Airflow's <2.0)
# The DAG calls python.real with -I flag (isolated mode) to prevent user site-packages leaking 
# This is to make sure that there are no conflicts between pyiceberg's dependencies and Airflow's dependencies
RUN python -m venv /home/airflow/iceberg_venv \
    && /home/airflow/iceberg_venv/bin/pip install --no-cache-dir \
       'pyiceberg[s3fs,sql-postgres]==0.8.0' \
       'sqlalchemy>=2.0' \
       'pandas' \
       'pyarrow' \
       'psycopg2-binary' \
    && /home/airflow/iceberg_venv/bin/pip install --no-cache-dir --upgrade \
       'fsspec>=2024.10.0' \
       's3fs' \
    && cp /home/airflow/iceberg_venv/bin/python /home/airflow/iceberg_venv/bin/python.real
