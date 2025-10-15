import pandas_gbq

PROJECT_ID = "central-catcher-471811-s3"

def update_dim_merchants_table(requests):


    # Your SQL query to fetch data from BigQuery
    sql_query = """
                SELECT
                    *
                FROM `central-catcher-471811-s3.dimensions.dim_merchants`
                """

    df = pandas_gbq.read_gbq(sql_query, project_id=PROJECT_ID)
    print(f"Data (with shape: {df.shape}) loaded successfully:")
    print(df.head())

    return 'OK'

if __name__ == '__main__':
    update_dim_merchants_table('some')
