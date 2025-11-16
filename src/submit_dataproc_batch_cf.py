import json

import functions_framework
from google.cloud import dataproc_v1 as dataproc

PROJECT_ID = "central-catcher-471811-s3"
REGION = "us-central1"
SPARK_FILE_URI = "gs://bog_reports_tmp_gcs_bucket/main_dataproc_file.py"
ADDITIONAL_PYTHON_FILE_URIS = ["gs://bog_reports_tmp_gcs_bucket/excel_file_opener.py"]

# The URI for your custom Dataproc Serverless image stored in Artifact Registry.
# Format: AR_REGION-docker.pkg.dev/PROJECT_ID/REPOSITORY_NAME/IMAGE_NAME:TAG
CUSTOM_IMAGE_URI = "us-docker.pkg.dev/central-catcher-471811-s3/gcr.io/custom_dataproc_cluster:1.0"

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

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    batch_id = f"spark-custom-image-job-{event_id}"
    args = {"args": [f"--input-file={data['name']}"]}
    SPARK_BATCH_CONFIG["pyspark_batch"].update(args)

    # 4. Initialize the Dataproc client and submit the job
    try:
        dataproc_client = dataproc.BatchControllerClient(
            client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"})

        operation = dataproc_client.create_batch(
            request={
                "parent": f"projects/{PROJECT_ID}/regions/{REGION}",
                "batch": SPARK_BATCH_CONFIG,
                "batch_id": batch_id,
            }
        )

        print(f"Waiting for Dataproc Serverless job {batch_id} to start...")
        # Note: operation.result() would wait for the job to *finish*.
        # For a Cloud Function, we submit and immediately return.

        return json.dumps({
            "status": "success",
            "job_name": batch_id,
            "message": f"Dataproc Serverless job {batch_id} submitted successfully."
        }), 200

    except Exception as e:
        print(f"Error submitting Dataproc job: {e}")
        return json.dumps({
            "status": "error",
            "message": f"Failed to submit Dataproc job: {str(e)}"
        }), 500
