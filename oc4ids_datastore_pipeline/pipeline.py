import datetime
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import flattentool
import requests
from libcoveoc4ids.api import oc4ids_json_output

from oc4ids_datastore_pipeline.database import (
    Dataset,
    delete_dataset,
    get_dataset_ids,
    save_dataset,
)
from oc4ids_datastore_pipeline.notifications import send_notification
from oc4ids_datastore_pipeline.registry import (
    fetch_registered_datasets,
    get_license_title_from_url,
)
from oc4ids_datastore_pipeline.storage import delete_files_for_dataset, upload_files

logger = logging.getLogger(__name__)


class ProcessDatasetError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class ValidationError(ProcessDatasetError):
    def __init__(self, errors_count: int, errors: list[str]):
        message = f"Dataset has {errors_count} validation errors: {str(errors)}"
        super().__init__(message)


def download_json(url: str) -> Any:
    logger.info(f"Downloading json from {url}")
    try:
        r = requests.get(url)
        r.raise_for_status()
        response_size = len(r.content)
        logger.info(f"Downloaded {url} ({response_size} bytes)")
        return r.json()
    except Exception as e:
        raise ProcessDatasetError(f"Download failed: {str(e)}")


def validate_json(dataset_id: str, json_data: dict[str, Any]) -> None:
    logger.info(f"Validating dataset {dataset_id}")
    try:
        validation_result = oc4ids_json_output(json_data=json_data)
        validation_errors_count = validation_result["validation_errors_count"]
        validation_errors = validation_result["validation_errors"]
        if validation_errors_count > 0:
            raise ValidationError(
                errors_count=validation_errors_count,
                errors=validation_errors,
            )
        logger.info(f"Dataset {dataset_id} is valid")
    except Exception as e:
        raise ProcessDatasetError(f"Validation failed: {str(e)}")


def write_json_to_file(file_name: str, json_data: dict[str, Any]) -> str:
    logger.info(f"Writing dataset to file {file_name}")
    try:
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "w") as file:
            json.dump(json_data, file, indent=4)
        logger.info(f"Finished writing to {file_name}")
        return file_name
    except Exception as e:
        raise ProcessDatasetError(f"Error writing dataset to file: {e}")


def transform_to_csv_and_xlsx(json_path: str) -> tuple[Optional[str], Optional[str]]:
    logger.info(f"Transforming {json_path}")
    try:
        path = Path(json_path)
        flattentool.flatten(
            json_path,
            output_name=str(path.parent / path.stem),
            root_list_path="projects",
            main_sheet_name="projects",
        )  # type: ignore[no-untyped-call]
        csv_path = str(path.parent / path.stem)
        xlsx_path = f"{path.parent / path.stem}.xlsx"
        logger.info(f"Transformed to CSV at {csv_path}")
        logger.info(f"Transformed to XLSX at {xlsx_path}")
        return csv_path, xlsx_path
    except Exception as e:
        logger.warning(f"Failed to transform JSON to CSV and XLSX: {e}")
        return None, None


def save_dataset_metadata(
    dataset_id: str,
    source_url: str,
    json_data: dict[str, Any],
    json_url: Optional[str],
    csv_url: Optional[str],
    xlsx_url: Optional[str],
) -> None:
    logger.info(f"Saving metadata for dataset {dataset_id}")
    try:
        publisher_name = json_data.get("publisher", {}).get("name", "")
        license_url = json_data.get("license", None)
        license_title = get_license_title_from_url(license_url) if license_url else None
        dataset = Dataset(
            dataset_id=dataset_id,
            source_url=source_url,
            publisher_name=publisher_name,
            license_url=license_url,
            license_title=license_title,
            json_url=json_url,
            csv_url=csv_url,
            xlsx_url=xlsx_url,
            updated_at=datetime.datetime.now(datetime.UTC),
        )
        save_dataset(dataset)
    except Exception as e:
        raise ProcessDatasetError(f"Failed to update metadata for dataset: {e}")


def process_dataset(dataset_id: str, source_url: str) -> None:
    logger.info(f"Processing dataset {dataset_id}")
    json_data = download_json(source_url)
    validate_json(dataset_id, json_data)
    json_path = write_json_to_file(
        file_name=f"data/{dataset_id}/{dataset_id}.json",
        json_data=json_data,
    )
    csv_path, xlsx_path = transform_to_csv_and_xlsx(json_path)
    json_public_url, csv_public_url, xlsx_public_url = upload_files(
        dataset_id, json_path=json_path, csv_path=csv_path, xlsx_path=xlsx_path
    )
    save_dataset_metadata(
        dataset_id=dataset_id,
        source_url=source_url,
        json_data=json_data,
        json_url=json_public_url,
        csv_url=csv_public_url,
        xlsx_url=xlsx_public_url,
    )
    logger.info(f"Processed dataset {dataset_id}")


def process_deleted_datasets(registered_datasets: dict[str, str]) -> None:
    stored_datasets = get_dataset_ids()
    deleted_datasets = stored_datasets - registered_datasets.keys()
    for dataset_id in deleted_datasets:
        logger.info(f"Dataset {dataset_id} is no longer in the registry, deleting")
        delete_dataset(dataset_id)
        delete_files_for_dataset(dataset_id)


def process_registry() -> None:
    registered_datasets = fetch_registered_datasets()
    process_deleted_datasets(registered_datasets)
    errors: list[dict[str, Any]] = []
    for dataset_id, url in registered_datasets.items():
        try:
            process_dataset(dataset_id, url)
        except Exception as e:
            logger.warning(f"Failed to process dataset {dataset_id} with error {e}")
            errors.append(
                {"dataset_id": dataset_id, "source_url": url, "message": str(e)}
            )
    if errors:
        logger.error(
            f"Errors while processing registry: {json.dumps(errors, indent=4)}"
        )
        send_notification(errors)
    logger.info("Finished processing all datasets")


def run() -> None:
    process_registry()
