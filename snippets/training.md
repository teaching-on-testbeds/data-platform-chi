
::: {.cell .markdown}

## Train a moderation model from Iceberg

In the previous stage, we created `moderation.training_data` as an Iceberg table. Now can we use that table to train a model. An inference service with this model could potentially replace the heuristic "risk scoring service" that we introduced in the real time data pipeline.

We are going to train a classifier in Python with scikit-learn: we will read Iceberg tables through PyIceberg, then trains a logistic regression incrementally using an `SGDClassifier`.

:::

::: {.cell .markdown}

Run this on `node-data`:

```bash
# run on node-data
docker compose -f /home/cc/data-platform-chi/docker/docker-compose.yaml up -d --build jupyter
```

The `jupyter` service in compose is:

```yaml
jupyter:
  build:
    context: ../model_training
    dockerfile: Dockerfile
  container_name: jupyter
  ports:
    - "8888:8888"
  environment:
    - PYICEBERG_CATALOG__GOURMETGRAM__TYPE=sql
    - PYICEBERG_CATALOG__GOURMETGRAM__URI=postgresql+psycopg2://user:gourmetgram_postgres@postgres:5432/iceberg_catalog
    - PYICEBERG_CATALOG__GOURMETGRAM__S3__ENDPOINT=http://minio:9000
    - PYICEBERG_CATALOG__GOURMETGRAM__S3__ACCESS_KEY_ID=admin
    - PYICEBERG_CATALOG__GOURMETGRAM__S3__SECRET_ACCESS_KEY=gourmetgram_minio
    - PYICEBERG_CATALOG__GOURMETGRAM__WAREHOUSE=s3://gourmetgram-datalake/warehouse
  volumes:
    - ../model_training/workspace:/home/jovyan/work
```

Here,

* `build` uses the Jupyter image from `model_training/Dockerfile`.
* `ports` exposes notebook UI on `8888`.
* `PYICEBERG_CATALOG__...` points PyIceberg at the SQL catalog in PostgreSQL and warehouse files in MinIO.
* `volumes` mounts `model_training/workspace` into `/home/jovyan/work`, so notebooks and saved artifacts persist in the lab directory.


After the service is up, get the Jupyter token:

```bash
# run on node-data
docker exec jupyter jupyter server list
```

Open the URL in your local browser, replacing `localhost` with your floating IP:

* `http://A.B.C.D:8888/tree?token=...`

Then, in Jupyter:

1. Open `training.ipynb`.
2. Run cells top to bottom.
3. Confirm training completes successfully.


The notebook uses PyIceberg to load data from the lakehouse, e.g.:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("gourmetgram")
table = catalog.load_table("moderation.training_data")
```

Note that it does not have to specify the exact location in S3 where the data is saved - it finds out from the catalog.

We don't have a lot of data, because we haven't been running our "service" very long! But we consider that in the future, we will have a large training data set that does not fit in memory. So, we stream Arrow batches and train incrementally:

```python
scanner = table.scan()
batches = scanner.to_arrow_batch_reader()

for batch in batches:
    df_batch = batch.to_pandas()
    model.partial_fit(X_batch, y_batch, classes=[0, 1])
```

Note thatIceberg also gives us dataset versioning for training reproducibility. Every table write creates a new snapshot. Before training, we can record the snapshot id for `moderation.training_data` and save it with model artifacts. Later, we can point to the exact snapshot used for that model version.

```python
catalog = load_catalog("gourmetgram")
table = catalog.load_table("moderation.training_data")

snapshot_id = table.metadata.current_snapshot_id
print(f"Training from Iceberg snapshot: {snapshot_id}")
```

That means model version `N` can be traced back to a specific Iceberg snapshot, not just a vague time window.

:::

::: {.cell .markdown}

At the end of the notebook, we save artifacts locally in the mounted workspace. Look in the `models` directory and confirm these files exist:

* `model.joblib`
* `scaler.joblib`
* `encoder.joblib`
* `metadata.json`

Open `metadata.json` and note `snapshot_id`: this is the exact Iceberg snapshot used for the training run.

:::
