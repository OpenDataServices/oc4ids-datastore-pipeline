import logging
from typing import Any

import requests

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


def process_dataset(dataset_name: str, dataset_url: str) -> None:
    logger.info(f"Processing dataset {dataset_name}")
    try:
        download_json(dataset_url)
    except Exception as e:
        logger.warning(f"Failed to process dataset {dataset_name} with error {e}")


def process_datasets() -> None:
    for name, url in REGISTERED_DATASETS.items():
        process_dataset(name, url)


def run() -> None:
    process_datasets()
