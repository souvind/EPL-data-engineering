# load.py
# Upload local directory to Azure Blob / ADLS Gen2. Uses azure-storage-blob.
import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient, ContentSettings
import argparse

def upload_dir_to_container(local_dir, container_name, connection_string, remote_prefix=""):
    service = BlobServiceClient.from_connection_string(connection_string)
    container = service.get_container_client(container_name)
    try:
        container.create_container()
    except Exception:
        pass
    local_dir = Path(local_dir)
    for path in local_dir.rglob("*"):
        if path.is_file():
            blob_path = (remote_prefix + "/" + str(path.relative_to(local_dir))).lstrip("/")
            print(f"Uploading {path} -> {blob_path}")
            with open(path, "rb") as data:
                content_settings = ContentSettings(content_type="application/octet-stream")
                container.upload_blob(name=blob_path, data=data, overwrite=True, content_settings=content_settings)
    print("Upload complete.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-path", required=True, help="Local directory to upload (e.g., data/processed)")
    parser.add_argument("--container-name", required=True)
    parser.add_argument("--connection-string", default=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))
    parser.add_argument("--remote-prefix", default="")
    args = parser.parse_args()

    if not args.connection_string:
        raise SystemExit("Provide AZURE_STORAGE_CONNECTION_STRING via env var or --connection-string")

    upload_dir_to_container(args.local_path, args.container_name, args.connection_string, args.remote_prefix)
