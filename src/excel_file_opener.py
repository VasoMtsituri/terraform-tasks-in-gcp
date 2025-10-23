import pandas as pd

def read_excel_from_gcs(gcs_path: str) -> pd.DataFrame:
    df = pd.read_excel(gcs_path, engine='openpyxl')

    return df
