from google.cloud import secretmanager
import google_crc32c


def read_gcp_secret(project_id: str, secret_id: str, version_id: str = 'latest') -> str:
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. '5') or an alias (e.g. 'latest').

    Args:
        project_id (str): GCP project ID.
        secret_id (str): Secret ID.
        version_id (str): Secret version ID. Defaults to 'latest'.

    Returns:
        str: The GCP secret requested.
    """

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f'projects/{project_id}/secrets/{secret_id}/versions/{version_id}'

    # Access the secret version.
    response = client.access_secret_version(request={'name': name})

    # Verify payload checksum.
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print('Data corruption detected.')

        return ''

    # Return the secret payload.
    payload = response.payload.data.decode()
    print('Secret retrieved successfully.')

    return payload
