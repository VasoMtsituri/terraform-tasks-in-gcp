from google.cloud import dataproc_v1 as dataproc
from google.api_core.exceptions import GoogleAPIError

PROJECT_ID = 'central-catcher-471811-s3'
REGION = 'us-central1'
DATAPROC_CLUSTER = 'test-cluster-bq-conn'
PYSPARK_FILE_URI = 'gs://amex-reports/main_prod.py'


def submit_spark_job(project_id, region, cluster_name,
                     main_python_file_uri=None, args=None):
    """Submits a Spark or PySpark job to a Dataproc cluster."""

    client = dataproc.JobControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'})

    job_details = {'placement': {'cluster_name': cluster_name}, 'pyspark_job': {
        'main_python_file_uri': main_python_file_uri,
        'args': args or []
    }}

    try:
        operation = client.submit_job_as_operation(
            project_id=project_id, region=region, job=job_details
        )
        print(f'Job submitted. Operation ID: {operation}')
        result = operation.result()  # Waits for the job to complete
        print(f'Job finished. State: {result.status.state}')

        if result.status.details:
            print(f'Job details: {result.status.details}')

        return result.reference.job_id
    except GoogleAPIError as e:
        print(f'Error submitting job: {e}')

        return None

def execute_cf(request):
    job_id = submit_spark_job(
        project_id=PROJECT_ID, region=REGION, cluster_name=DATAPROC_CLUSTER,
        main_python_file_uri=PYSPARK_FILE_URI
    )


    if job_id:
        print(f'Successfully submitted job with ID: {job_id}')

        return 'OK'
    else:
        print('Failed to submit job')

        return 'Failed to submit job'
