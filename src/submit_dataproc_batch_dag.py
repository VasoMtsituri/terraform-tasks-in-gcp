from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

# [START define_batch_config_with_custom_image]
# Configuration for the Dataproc Serverless for Spark Batch Job
# NOTE: Replace 'your-project-id', 'your-region', 'your-dataproc-bucket' and the
# CUSTOM_IMAGE_URI with your actual values.
PROJECT_ID = "central-catcher-471811-s3"
REGION = "us-central1"  # e.g., "us-central1"
SPARK_FILE_URI = "gs://bog_reports_tmp_gcs_bucket/main_dataproc_file.py"
ADDITIONAL_PYTHON_FILE_URIS = ["gs://bog_reports_tmp_gcs_bucket/excel_file_opener.py"]
BATCH_ID = "serverless-pyspark-batch-{{ ds_nodash }}"  # Unique ID based on the run date

# The URI for your custom Dataproc Serverless image stored in Artifact Registry.
# Format: AR_REGION-docker.pkg.dev/PROJECT_ID/REPOSITORY_NAME/IMAGE_NAME:TAG
CUSTOM_IMAGE_URI = (
    "us-docker.pkg.dev/central-catcher-471811-s3/gcr.io/custom_dataproc_cluster:1.0"
)

# The configuration for the Serverless Batch job
SPARK_BATCH_CONFIG = {
    "pyspark_batch": {
        # The GCS URI of your main Python file
        "main_python_file_uri": SPARK_FILE_URI,
        "python_file_uris": ADDITIONAL_PYTHON_FILE_URIS
    },
    "runtime_config": {
        # The Spark version to use. This should match the base version of your custom image.
        "version": "2.3",
        # Use the custom image stored in Artifact Registry
        "container_image": CUSTOM_IMAGE_URI
    }
}

with DAG(
        dag_id="dataproc_serverless_pyspark_custom_image",
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        schedule=None,
        catchup=False,
        tags=["dataproc", "serverless", "spark", "custom-image"],
        doc_md="""
    ### Dataproc Serverless Custom Image Runner DAG
    This DAG submits a Dataproc Serverless for Spark batch job using a custom image
    from Artifact Registry.
    """,
) as dag:
    submit_spark_batch = DataprocCreateBatchOperator(
        task_id="submit_spark_batch_job_custom_image",
        project_id=PROJECT_ID,
        region=REGION,
        batch=SPARK_BATCH_CONFIG,
        batch_id=BATCH_ID,
    )

submit_spark_batch
