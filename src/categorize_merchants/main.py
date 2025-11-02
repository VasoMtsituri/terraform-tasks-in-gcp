import pandas as pd
import pandas_gbq
from google import genai

from utils_google import read_gcp_secret

PROJECT_ID = "central-catcher-471811-s3"
GCS_BUCKET = "bog_reports_tmp_gcs_bucket"
MERCHANTS_TABLE_BQ = f"{PROJECT_ID}.dimensions.dim_merchants"
GEMINI_API_KEY = "gemini-api-key"
GEMINI_MODEL = "gemini-2.5-flash"
MERCHANTS_CATEGORIZED_TABLE_BQ = f"{PROJECT_ID}.dimensions.dim_merchants_categorized"


def categorize_merchants(request):
    df = pandas_gbq.read_gbq(query_or_table=MERCHANTS_TABLE_BQ,
                             project_id=PROJECT_ID)

    gemini_api_key = read_gcp_secret(project_id=PROJECT_ID,
                                     secret_id=GEMINI_API_KEY)

    client = genai.Client(api_key=gemini_api_key)
    results = []

    for index, merchant_row in df.iterrows():
        merchant_name = merchant_row["merchant_name"]
        merchant_address = merchant_row["merchant_address"]
        gemini_prompt = f"""You are an expert entity classifier.
            Your sole task is to determine the single, most specific category for
            a given merchant based on its name and address, ignoring any surrounding
            text or details.\n\n\n\n**Constraint:** Your output **must** be a single word,
            in lowercase, that represents the merchant's category. Do not include articles
            (a, an, the), punctuation, or any other words, explanations, or qualifiers.
             \n\n\n\n**Merchant Data:**\n\nName: {merchant_name}\n\nAddress:
            {merchant_address}\n\n\n\n**Examples of Desired Output Format:
            **\n\ncafe\n\nrestaurant\n\nbookstore\n\npharmacy\n\nsupermarket
            \n\nbakery\n\ngym\n\nhairdresser\n\nelectronics\n\nmuseum\n\n\n\n**Output Category:**"""

        merchant_category = client.models.generate_content(model=GEMINI_MODEL,
                                                           contents=gemini_prompt).text

        id_category_mapping = {
            "merchant_id": merchant_row["id"],
            "category": merchant_category
        }
        results.append(id_category_mapping)

    # Join results back to dataframe and save to BigQuery
    results_df = pd.DataFrame(results)
    final_df = df.merge(results_df, left_on="id", right_on="merchant_id")
    # Add new columns to final_df to include timestamp
    final_df["categorized_at"] = pd.Timestamp.now(tz="UTC")
    pandas_gbq.to_gbq(final_df,
                      destination_table=MERCHANTS_CATEGORIZED_TABLE_BQ,
                      project_id=PROJECT_ID, if_exists="replace")


    return 'OK'
