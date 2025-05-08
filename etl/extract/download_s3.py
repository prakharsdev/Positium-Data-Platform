import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load env variables from .env
load_dotenv()

# S3 credentials & config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX")  # usually "testdata"

# Local target folders
LOCAL_BASE_PATH = "data/raw"
EVENTS_PATH = os.path.join(LOCAL_BASE_PATH, "events")
CELLS_PATH = os.path.join(LOCAL_BASE_PATH, "cells")

os.makedirs(EVENTS_PATH, exist_ok=True)
os.makedirs(CELLS_PATH, exist_ok=True)

# Initialize S3 client for Oracle-compatible endpoint
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=AWS_S3_ENDPOINT,
    config=Config(signature_version="s3v4")
)

def download_files_from_prefix(prefix, local_dir):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue

            relative_path = os.path.relpath(key, prefix)
            local_path = os.path.join(local_dir, relative_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            print(f"Downloading {key} to {local_path}...")
            s3.download_file(S3_BUCKET, key, local_path)

def main():
    print("Downloading events parquet files...")
    download_files_from_prefix(f"{S3_KEY_PREFIX}/events/", EVENTS_PATH)

    print("Downloading cells parquet files...")
    download_files_from_prefix(f"{S3_KEY_PREFIX}/cells/", CELLS_PATH)

    print("All S3 files downloaded.")

if __name__ == "__main__":
    main()
