from io import StringIO
from typing import Optional

import pandas as pd
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

from utils_google import read_gcp_secret

GCP_PROJECT_ID = 'central-catcher-471811-s3'
SHAREPOINT_CLIENT_ID = 'sharepoint_client_id'
SHAREPOINT_CLIENT_SECRET = 'sharepoint_client_secret'

def build_file_relative_url(site_name: str, source_directory: str, file_name: str) -> str:
    """Takes some of the properties from config and returns valid relative URL for the file"""
    return f"/sites/{site_name}/{source_directory}/{file_name}"

def try_get_file(self, file_relative_url: str) -> Optional[File]:
        """Downloads file as File under the file_relative_url given"""
        try:
            return (
                self.context.get_file_by_server_relative_url(file_relative_url)
                .get()
                .execute_query()
            )
        except ClientRequestException as e:
            if e.response.status_code == 404:
                print('Failed to download file: 404')
            else:
                print(f"Other ClientRequestException raised: {e.response.text}!!!")
            return None

file_relative_url = build_file_relative_url(site_name='test_site', file_name='6026001080.csv')
client_id = read_gcp_secret(project_id=GCP_PROJECT_ID, secret_id=SHAREPOINT_CLIENT_ID)
client_secret = read_gcp_secret(project_id=GCP_PROJECT_ID, secret_id=SHAREPOINT_CLIENT_SECRET)

credentials = ClientCredential(client_id, client_secret)
client = ClientContext('site_url').with_credentials(credentials)

client.web.get().execute_query()
print(f"✅ Successfully authenticated and connected to {client.web.title}.\n")

file = File.open_binary(client.web.context, file_relative_url)

if not file:
    print('Error retrieving file')

df = pd.read_csv(StringIO(file.read().decode()))

print(df)
