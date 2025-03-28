import logging
import os
import zipfile
from pathlib import Path
from typing import Any, Optional

import boto3
import botocore

logger = logging.getLogger(__name__)


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


def _upload_file(local_path: str, bucket_path: str, content_type: str) -> Optional[str]:
    try:
        logger.info(f"Uploading file {local_path}")
        client = _get_client()
        client.upload_file(
            local_path,
            BUCKET_NAME,
            bucket_path,
            ExtraArgs={"ACL": "public-read", "ContentType": content_type},
        )
        public_url = (
            f"https://{BUCKET_NAME}.{BUCKET_REGION}.digitaloceanspaces.com/"
            + bucket_path
        )
        logger.info(f"Uploaded to {public_url}")
        return public_url
    except Exception as e:
        logger.warning(f"Failed to upload {local_path} with error {e}")
        return None


def _upload_json(dataset_id: str, json_path: str) -> Optional[str]:
    return _upload_file(
        local_path=json_path,
        bucket_path=f"{dataset_id}/{dataset_id}.json",
        content_type="application/json",
    )


def _upload_csv(dataset_id: str, csv_path: str) -> Optional[str]:
    try:
        directory = Path(csv_path)
        zip_file_path = f"{csv_path}_csv.zip"
        with zipfile.ZipFile(zip_file_path, mode="w") as archive:
            for file_path in directory.rglob("*"):
                archive.write(file_path, arcname=file_path.relative_to(directory))
    except Exception as e:
        logger.warning(f"Failed to zip {csv_path} with error {e}")
        return None
    return _upload_file(
        local_path=zip_file_path,
        bucket_path=f"{dataset_id}/{dataset_id}_csv.zip",
        content_type="application/zip",
    )


def _upload_xlsx(dataset_id: str, xlsx_path: str) -> Optional[str]:
    return _upload_file(
        local_path=xlsx_path,
        bucket_path=f"{dataset_id}/{dataset_id}.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # noqa: E501
    )


def upload_files(
    dataset_id: str,
    json_path: Optional[str] = None,
    csv_path: Optional[str] = None,
    xlsx_path: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not bool(int(os.environ.get("ENABLE_UPLOAD", "0"))):
        logger.info("Upload is disabled, skipping")
        return None, None, None
    json_public_url = _upload_json(dataset_id, json_path) if json_path else None
    csv_public_url = _upload_csv(dataset_id, csv_path) if csv_path else None
    xlsx_public_url = _upload_xlsx(dataset_id, xlsx_path) if xlsx_path else None
    return json_public_url, csv_public_url, xlsx_public_url


def delete_files_for_dataset(dataset_id: str) -> None:
    logger.info(f"Deleting files for dataset {dataset_id}")
    try:
        client = _get_client()
        response = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=dataset_id)
        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
            client.delete_objects(
                Bucket=BUCKET_NAME, Delete={"Objects": objects_to_delete}
            )
    except Exception as e:
        logger.warning(f"Failed to delete files with error {e}")
