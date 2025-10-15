from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

PROJECT_ID = 'central-catcher-471811-s3'
GCS_BUCKET = 'bog_reports_tmp_gcs_bucket'

spark = SparkSession \
    .builder \
    .appName('spark-bigquery-demo') \
    .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
spark.conf.set('temporaryGcsBucket', GCS_BUCKET)

# Load data from BigQuery.
merchants = spark.read.format('bigquery') \
    .load(f'{PROJECT_ID}.dimensions.dim_merchants') \

first_merchant = merchants.limit(1)
df = first_merchant.withColumn("batch_timestamp", current_date())

df.write.format('bigquery') \
  .option('table', f'{PROJECT_ID}.dimensions.dim_merchants_preview_from_dataproc') \
  .mode('append') \
  .save()
