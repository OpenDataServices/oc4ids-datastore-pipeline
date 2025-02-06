import json
import logging
import os
from typing import Any

import requests
from libcoveoc4ids.api import oc4ids_json_output

logger = logging.getLogger(__name__)

REGISTERED_DATASETS = {
    "uganda_gpp": "https://gpp.ppda.go.ug/adminapi/public/api/open-data/v1/infrastructure/projects/download?format=json",  # noqa: E501
    "ghana_cost_sekondi_takoradi": "https://costsekondi-takoradigh.org/uploads/projectJson.json",  # noqa: E501
    "mexico_cost_jalisco": "http://www.costjalisco.org.mx/jsonprojects",
    "mexico_nuevo_leon": "http://si.nl.gob.mx/siasi_ws/api/edcapi/DescargarProjectPackage",  # noqa: E501
    "indonesia_cost_west_lombok": "https://intras.lombokbaratkab.go.id/oc4ids",
    "ukraine_cost_ukraine": "https://portal.costukraine.org/data.json",
    "malawi_cost_malawi": "https://ippi.mw/api/projects/query",
}


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


def validate_json(dataset_name: str, json_data: Any) -> None:
    logger.info(f"Validating dataset {dataset_name}")
    try:
        validation_result = oc4ids_json_output(json_data=json_data)
        validation_errors_count = validation_result["validation_errors_count"]
        if validation_errors_count > 0:
            raise Exception(f"Dataset has {validation_errors_count} validation errors")
        logger.info(f"Dataset {dataset_name} is valid")
    except Exception as e:
        raise Exception("Validation failed", e)


def write_json_to_file(file_name: str, json_data: Any) -> None:
    logger.info(f"Writing dataset to file {file_name}")
    try:
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "w") as file:
            json.dump(json_data, file, indent=4)
        logger.info(f"Finished writing to {file_name}")
    except Exception as e:
        raise Exception("Error while writing to JSON file", e)


def process_dataset(dataset_name: str, dataset_url: str) -> None:
    logger.info(f"Processing dataset {dataset_name}")
    try:
        json_data = download_json(dataset_url)
        validate_json(dataset_name, json_data)
        write_json_to_file(f"data/{dataset_name}.json", json_data)
        logger.info(f"Processed dataset {dataset_name}")
    except Exception as e:
        logger.warning(f"Failed to process dataset {dataset_name} with error {e}")


def process_datasets() -> None:
    for name, url in REGISTERED_DATASETS.items():
        process_dataset(name, url)


def run() -> None:
    process_datasets()
