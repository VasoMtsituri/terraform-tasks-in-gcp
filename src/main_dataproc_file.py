from excel_file_opener import read_excel_from_gcs

df = read_excel_from_gcs(gcs_path='gs://bog_reports_tmp_gcs_bucket/dummy.xlsx')

print(df.head())
print(df.dtypes)