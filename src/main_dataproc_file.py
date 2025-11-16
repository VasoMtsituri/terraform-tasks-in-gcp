import argparse

from excel_file_opener import read_excel_from_gcs

def parse_arguments():
    """Parses the command-line arguments passed from the Cloud Function."""
    parser = argparse.ArgumentParser(description="Dataproc Serverless Spark Job.")
    parser.add_argument(
        '--input-file',
        type=str,
        required=True,
        help="GCS path for input file name"
    )

    return parser.parse_args()

args = parse_arguments()
# Log the extracted metadata
print("--- Extracted Metadata ---")
print(f"Input Path: {args.input_file}")

df = read_excel_from_gcs(gcs_path='gs://bog_reports_tmp_gcs_bucket/dummy.xlsx')

print(df.head())
print(df.dtypes)