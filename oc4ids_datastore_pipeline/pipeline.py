import datetime
import json
import logging
import os
from typing import Any

import requests
from libcoveoc4ids.api import oc4ids_json_output

from oc4ids_datastore_pipeline.database import Dataset, save_dataset
from oc4ids_datastore_pipeline.registry import (
    fetch_registered_datasets,
    get_license_name_from_url,
)

logger = logging.getLogger(__name__)


def download_json(url: str) -> Any:
    logger.info(f"Downloading json from {url}")
    try:
        r = requests.get(url)
        r.raise_for_status()
        response_size = len(r.content)
        logger.info(f"Downloaded {url} ({response_size} bytes)")
        return r.json()
    except Exception as e:
        raise Exception("Download failed", e)


def validate_json(dataset_name: str, json_data: dict[str, Any]) -> None:
    logger.info(f"Validating dataset {dataset_name}")
    try:
        validation_result = oc4ids_json_output(json_data=json_data)
        validation_errors_count = validation_result["validation_errors_count"]
        if validation_errors_count > 0:
            raise Exception(f"Dataset has {validation_errors_count} validation errors")
        logger.info(f"Dataset {dataset_name} is valid")
    except Exception as e:
        raise Exception("Validation failed", e)


def write_json_to_file(file_name: str, json_data: dict[str, Any]) -> str:
    logger.info(f"Writing dataset to file {file_name}")
    try:
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "w") as file:
            json.dump(json_data, file, indent=4)
        logger.info(f"Finished writing to {file_name}")
        return file_name
    except Exception as e:
        raise Exception("Error while writing to JSON file", e)


def save_dataset_metadata(
    dataset_name: str, source_url: str, json_data: dict[str, Any], json_url: str
) -> None:
    logger.info(f"Saving metadata for dataset {dataset_name}")
    publisher_name = json_data.get("publisher", {}).get("name", "")
    license_url = json_data.get("license", None)
    license_name = get_license_name_from_url(license_url) if license_url else None
    dataset = Dataset(
        dataset_id=dataset_name,
        source_url=source_url,
        publisher_name=publisher_name,
        license_url=license_url,
        license_name=license_name,
        json_url=json_url,
        updated_at=datetime.datetime.now(datetime.UTC),
    )
    save_dataset(dataset)


def process_dataset(dataset_name: str, dataset_url: str) -> None:
    logger.info(f"Processing dataset {dataset_name}")
    try:
        json_data = download_json(dataset_url)
        validate_json(dataset_name, json_data)
        json_url = write_json_to_file(f"data/{dataset_name}.json", json_data)
        save_dataset_metadata(
            dataset_name=dataset_name,
            source_url=dataset_url,
            json_data=json_data,
            json_url=json_url,
        )
        logger.info(f"Processed dataset {dataset_name}")
    except Exception as e:
        logger.warning(f"Failed to process dataset {dataset_name} with error {e}")


def process_datasets() -> None:
    registered_datasets = fetch_registered_datasets()
    for name, url in registered_datasets.items():
        process_dataset(name, url)


def run() -> None:
    process_datasets()
