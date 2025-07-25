import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)


_license_mappings = None


def fetch_registered_datasets() -> dict[str, dict[str, str]]:
    logger.info("Fetching registered datasets list from registry")
    try:
        url = "https://opendataservices.github.io/oc4ids-registry/datatig/type/dataset/records_api.json"  # noqa: E501
        r = requests.get(url)
        r.raise_for_status()
        json_data = r.json()
        registered_datasets = {}
        for key, value in json_data["records"].items():
            r_data = requests.get(value["api_url"])
            r_data.raise_for_status()
            r_data_json = r_data.json()
            registered_datasets[key] = {
                "source_url": r_data_json["fields"]["url"]["value"],
                "country": r_data_json["fields"]["country"]["value"],
                "portal_title": r_data_json["fields"]["portal_title"]["value"],
                "portal_url": r_data_json["fields"]["portal_url"]["value"],
            }
        registered_datasets_count = len(registered_datasets)
        logger.info(f"Fetched URLs for {registered_datasets_count} datasets")
    except Exception as e:
        raise Exception("Failed to fetch datasets list from registry", e)
    if registered_datasets_count < 1:
        raise Exception(
            "Zero datasets returned from registry, likely an upstream error"
        )
    return registered_datasets


def fetch_license_mappings() -> dict[str, dict[str, Optional[str]]]:
    logger.info("Fetching license mappings from registry")
    try:
        url = "https://opendataservices.github.io/oc4ids-registry/datatig/type/license/records_api.json"  # noqa: E501
        r = requests.get(url)
        r.raise_for_status()
        json_data = r.json()
        return {
            urls["fields"]["url"]["value"]: {
                "title": license["fields"]["title"]["value"],
                "title_short": license["fields"]["title_short"]["value"],
            }
            for license in json_data["records"].values()
            for urls in license["fields"]["urls"]["values"]
        }
    except Exception as e:
        logger.warning(
            "Failed to fetch license mappings from registry, with error: " + str(e),
        )
        return {}


def get_license_title_from_url(
    url: str, force_refresh: Optional[bool] = False
) -> tuple[Optional[str], Optional[str]]:
    global _license_mappings
    if force_refresh or (_license_mappings is None):
        _license_mappings = fetch_license_mappings()
    license_titles = _license_mappings.get(url, {})
    return license_titles.get("title"), license_titles.get("title_short")
