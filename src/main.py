from pyspark.sql import SparkSession

PROJECT_ID = "central-catcher-471811-s3"

def update_dim_merchants_table(requests):
    spark = SparkSession \
        .builder \
        .appName('spark-bigquery-demo') \
        .config('spark.jars', '/Users/vasomtsituri/PycharmProjects/terraform-tasks-in-gcp/spark-bigquery-latest_2.12.jar') \
        .getOrCreate()

    # Use the Cloud Storage bucket for temporary BigQuery export data used
    # by the connector.
    bucket = 'bog_reports_tmp_gcs_bucket'
    spark.conf.set('temporaryGcsBucket', bucket)

    # Load data from BigQuery.
    words = spark.read.format('bigquery') \
        .load('central-catcher-471811-s3.dimensions.dim_merchants') \

    words.printSchema()
    words.show()


if __name__ == '__main__':
    update_dim_merchants_table('some')
