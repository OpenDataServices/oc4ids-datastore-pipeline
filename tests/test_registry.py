from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from oc4ids_datastore_pipeline.registry import (
    fetch_license_mappings,
    fetch_registered_datasets,
    get_license_name_from_url,
)


def test_fetch_registered_datasets(mocker: MockerFixture) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "records": {
            "test_dataset": {"fields": {"url": {"value": "https://test_dataset.json"}}}
        }
    }
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.return_value = mock_response

    result = fetch_registered_datasets()

    assert result == {"test_dataset": "https://test_dataset.json"}


def test_fetch_registered_datasets_raises_failure_exception(
    mocker: MockerFixture,
) -> None:
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.side_effect = Exception("Mocked exception")

    with pytest.raises(Exception) as exc_info:
        fetch_registered_datasets()

    assert "Failed to fetch datasets list from registry" in str(exc_info.value)
    assert "Mocked exception" in str(exc_info.value)


def test_fetch_license_mappings(mocker: MockerFixture) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "records": {
            "license_1": {
                "fields": {
                    "title": {"value": "License 1"},
                    "urls": {
                        "values": [
                            {
                                "fields": {
                                    "url": {"value": "https://license_1.com/license"}
                                }
                            },
                            {
                                "fields": {
                                    "url": {
                                        "value": "https://license_1.com/different_url"
                                    }
                                }
                            },
                        ]
                    },
                }
            },
            "license_2": {
                "fields": {
                    "title": {"value": "License 2"},
                    "urls": {
                        "values": [
                            {
                                "fields": {
                                    "url": {"value": "https://license_2.com/license"}
                                }
                            },
                        ]
                    },
                }
            },
        }
    }
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.return_value = mock_response

    result = fetch_license_mappings()

    assert result == {
        "https://license_1.com/license": "License 1",
        "https://license_1.com/different_url": "License 1",
        "https://license_2.com/license": "License 2",
    }


def test_fetch_license_mappings_catches_exception(
    mocker: MockerFixture,
) -> None:
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.side_effect = Exception("Mocked exception")

    result = fetch_license_mappings()

    assert result == {}


def test_get_license_name_from_url(mocker: MockerFixture) -> None:
    patch_license_mappings = mocker.patch(
        "oc4ids_datastore_pipeline.registry.fetch_license_mappings"
    )
    patch_license_mappings.return_value = {
        "https://license_1.com/license": "License 1",
        "https://license_2.com/license": "License 2",
    }

    license_name = get_license_name_from_url(
        "https://license_2.com/license", force_refresh=True
    )

    assert license_name == "License 2"


def test_get_license_name_from_url_not_in_mapping(mocker: MockerFixture) -> None:
    patch_license_mappings = mocker.patch(
        "oc4ids_datastore_pipeline.registry.fetch_license_mappings"
    )
    patch_license_mappings.return_value = {
        "https://license_1.com/license": "License 1",
    }

    license_name = get_license_name_from_url(
        "https://license_2.com/license", force_refresh=True
    )

    assert license_name is None
