import logging
import os
import zipfile
from pathlib import Path
from typing import Any, Optional

import boto3
import botocore

logger = logging.getLogger(__name__)


# TODO:
ENABLE_UPLOAD = os.environ.get("ENABLE_UPLOAD", "0")
BUCKET_REGION = os.environ.get("BUCKET_REGION")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
BUCKET_ACCESS_KEY_ID = os.environ.get("BUCKET_ACCESS_KEY_ID")
BUCKET_ACCESS_KEY_SECRET = os.environ.get("BUCKET_ACCESS_KEY_SECRET")


def _get_client() -> Any:
    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=f"https://{BUCKET_REGION}.digitaloceanspaces.com/",
        config=botocore.config.Config(s3={"addressing_style": "virtual"}),
        region_name=BUCKET_REGION,
        aws_access_key_id=BUCKET_ACCESS_KEY_ID,
        aws_secret_access_key=BUCKET_ACCESS_KEY_SECRET,
    )


def upload_file(local_path: str, content_type: str) -> str:
    bucket_path = os.path.relpath(local_path, "data")
    logger.info(f"Uploading file {local_path}")
    client = _get_client()
    client.upload_file(
        local_path,
        BUCKET_NAME,
        bucket_path,
        ExtraArgs={"ACL": "public-read", "ContentType": content_type},
    )
    return (
        f"https://{BUCKET_NAME}.{BUCKET_REGION}.digitaloceanspaces.com/" + bucket_path
    )


def upload_json(json_path: str) -> str:
    return upload_file(json_path, content_type="application/json")


def upload_csv(csv_path: str) -> str:
    directory = Path(csv_path)
    zip_file_path = f"{csv_path}_csv.zip"
    with zipfile.ZipFile(zip_file_path, mode="w") as archive:
        for file_path in directory.rglob("*"):
            archive.write(file_path, arcname=file_path.relative_to(directory))
    return upload_file(zip_file_path, content_type="application/zip")


def upload_xlsx(xlsx_path: str) -> str:
    return upload_file(
        xlsx_path,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # noqa: E501
    )


def upload_files(
    json_path: Optional[str] = None,
    csv_path: Optional[str] = None,
    xlsx_path: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    # TODO: Option to delete local files once uploaded?
    # TODO: Exception handling
    if not bool(int(ENABLE_UPLOAD)):
        logger.info("Upload is disabled, skipping")
        return None, None, None
    logger.info("Uploading files")
    if json_path:
        json_public_url = upload_json(json_path)
        logger.info(f"Uploaded JSON file to {json_public_url}")
    if csv_path:
        csv_public_url = upload_csv(csv_path)
        logger.info(f"Uploaded CSV zip file to {csv_public_url}")
    if xlsx_path:
        xlsx_public_url = upload_xlsx(xlsx_path)
        logger.info(f"Uploaded XLSX file to {xlsx_public_url}")
    return json_public_url, csv_public_url, xlsx_public_url
