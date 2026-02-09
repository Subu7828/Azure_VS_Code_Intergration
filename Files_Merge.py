"""
merge_blobs.py
Downloads two text blobs from an Azure blob container, merges them (simple concatenation),
and uploads the merged result as a new blob.

Before running:
- Install azure-storage-blob (pip install azure-storage-blob)
- Set environment variables for CONNECTION_STRING and CONTAINER_NAME
- Replace blob names below.
"""

from azure.storage.blob import BlobServiceClient
import os

# ---------- CONFIGURATION ----------
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

BLOB_1 = "Subu1.txt/Subu1.txt"   # first uploaded file name
BLOB_2 = "Subu1.txt/Subu2.txt"   # second uploaded file name
MERGED_BLOB = "merged_result.txt"  # name for the merged output
# ----------------------------------

def download_blob_to_text(blob_service_client, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    stream = blob_client.download_blob()
    return stream.content_as_text(encoding='utf-8')

def upload_text_as_blob(blob_service_client, container_name, blob_name, text_data):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(text_data, overwrite=True)
    print(f"Uploaded merged blob: {blob_name}")

def main():
    if not CONNECTION_STRING or not CONTAINER_NAME:
        raise SystemExit("Please set AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_CONTAINER_NAME environment variables.")

    bsc = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    print("Downloading blobs...")
    text1 = download_blob_to_text(bsc, CONTAINER_NAME, BLOB_1)
    text2 = download_blob_to_text(bsc, CONTAINER_NAME, BLOB_2)

    print("Merging contents...")
    merged = text1 + "\n" + text2

    print("Uploading merged result...")
    upload_text_as_blob(bsc, CONTAINER_NAME, MERGED_BLOB, merged)

    print("Done!")

if __name__ == "__main__":
    main()
