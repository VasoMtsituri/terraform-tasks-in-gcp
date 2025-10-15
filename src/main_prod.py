from pyspark.sql import SparkSession

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
words = spark.read.format('bigquery') \
    .load('central-catcher-471811-s3.dimensions.dim_merchants') \

words.printSchema()
words.show()
