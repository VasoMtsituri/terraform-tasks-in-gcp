from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator

with DAG(
        dag_id='update_dim_merchants_preview',
        start_date=datetime(2025, 10, 16),
        schedule_interval=None,
        catchup=False,
        tags=['SubmitSparkJobRunner'],
) as dag:
    cf_invoke = CloudFunctionInvokeFunctionOperator(
        task_id='invoke_spark_job_submitter',
        project_id='central-catcher-471811-s3',
        function_id='submit-spark-job-cf',
        input_data={},
        location='us-central1')

cf_invoke
