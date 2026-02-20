# Model Training

This directory contains the Jupyter environment for training from Iceberg.

## What we use

- Notebook: `workspace/training.ipynb`
- Input table: `moderation.training_data` (written by Airflow DAG `moderation_training`)
- Method: streaming batches from PyIceberg + `SGDClassifier.partial_fit`
- Output artifacts (local): `workspace/models/model.joblib`, `workspace/models/scaler.joblib`, `workspace/models/encoder.joblib`, `workspace/models/metadata.json`

## Notes

- We intentionally use only the streaming training path.
- We do not generate synthetic large tables in this lab flow.
- We do not upload model artifacts to MinIO from this notebook step.
