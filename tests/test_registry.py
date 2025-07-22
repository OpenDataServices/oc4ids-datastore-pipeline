from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from oc4ids_datastore_pipeline.registry import (
    fetch_license_mappings,
    fetch_registered_datasets,
    get_license_title_from_url,
)


def test_fetch_registered_datasets(mocker: MockerFixture) -> None:
    mock_response = MagicMock()
    mock_response.json.side_effect = [
        {
            "records": {
                "test_dataset": {
                    "api_url": "http://www.example.com",
                    "fields": {
                        "url": {"value": "https://test_dataset.json"},
                        "country": {"value": "ab"},
                    },
                }
            }
        },
        {
            "fields": {
                "url": {"value": "https://test_dataset.json"},
                "country": {"value": "ab"},
                "portal_title": {"value": "Our Portal"},
                "portal_url": {"value": "https://our.portal"},
            }
        },
    ]
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.return_value = mock_response

    result = fetch_registered_datasets()

    assert result == {
        "test_dataset": {
            "source_url": "https://test_dataset.json",
            "country": "ab",
            "portal_title": "Our Portal",
            "portal_url": "https://our.portal",
        }
    }


def test_fetch_registered_datasets_raises_failure_exception(
    mocker: MockerFixture,
) -> None:
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.side_effect = Exception("Mocked exception")

    with pytest.raises(Exception) as exc_info:
        fetch_registered_datasets()

    assert "Failed to fetch datasets list from registry" in str(exc_info.value)
    assert "Mocked exception" in str(exc_info.value)


def test_fetch_registered_datasets_raises_exception_when_no_datasets(
    mocker: MockerFixture,
) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = {"records": {}}
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.return_value = mock_response

    with pytest.raises(Exception) as exc_info:
        fetch_registered_datasets()

    assert "Zero datasets returned from registry" in str(exc_info.value)


def test_fetch_license_mappings(mocker: MockerFixture) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "records": {
            "license_1": {
                "fields": {
                    "title": {"value": "License 1"},
                    "title_short": {"value": "L1"},
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
                    "title_short": {"value": "L2"},
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
        "https://license_1.com/license": {
            "title": "License 1",
            "title_short": "L1",
        },
        "https://license_1.com/different_url": {
            "title": "License 1",
            "title_short": "L1",
        },
        "https://license_2.com/license": {
            "title": "License 2",
            "title_short": "L2",
        },
    }


def test_fetch_license_mappings_catches_exception(
    mocker: MockerFixture,
) -> None:
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.side_effect = Exception("Mocked exception")

    result = fetch_license_mappings()

    assert result == {}


def test_get_license_title_from_url(mocker: MockerFixture) -> None:
    patch_license_mappings = mocker.patch(
        "oc4ids_datastore_pipeline.registry.fetch_license_mappings"
    )
    patch_license_mappings.return_value = {
        "https://license_1.com/license": {
            "title": "License 1",
            "title_short": "L1",
        },
        "https://license_2.com/license": {
            "title": "License 2",
            "title_short": "L2",
        },
    }

    license_title = get_license_title_from_url(
        "https://license_2.com/license", force_refresh=True
    )

    assert license_title == ("License 2", "L2")


def test_get_license_title_from_url_not_in_mapping(mocker: MockerFixture) -> None:
    patch_license_mappings = mocker.patch(
        "oc4ids_datastore_pipeline.registry.fetch_license_mappings"
    )
    patch_license_mappings.return_value = {
        "https://license_1.com/license": {
            "title": "License 1",
            "title_short": "L1",
        },
    }

    license_title = get_license_title_from_url(
        "https://license_2.com/license", force_refresh=True
    )

    assert license_title == (None, None)


def test_get_license_name_from_url_short_name_not_in_mapping(
    mocker: MockerFixture,
) -> None:
    patch_license_mappings = mocker.patch(
        "oc4ids_datastore_pipeline.registry.fetch_license_mappings"
    )
    patch_license_mappings.return_value = {
        "https://license_2.com/license": {
            "title": "License 2",
        },
    }

    license_title = get_license_title_from_url(
        "https://license_2.com/license", force_refresh=True
    )

    assert license_title == ("License 2", None)
