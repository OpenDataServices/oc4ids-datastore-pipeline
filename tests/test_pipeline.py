import os
import tempfile
from textwrap import dedent
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from oc4ids_datastore_pipeline.pipeline import (
    download_json,
    fetch_registered_datasets,
    process_dataset,
    validate_json,
    write_json_to_file,
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


def test_download_json_raises_failure_exception(mocker: MockerFixture) -> None:
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.side_effect = Exception("Mocked exception")

    with pytest.raises(Exception) as exc_info:
        download_json(url="https://test_dataset.json")

    assert "Download failed" in str(exc_info.value)
    assert "Mocked exception" in str(exc_info.value)


def test_validate_json_raises_failure_exception(mocker: MockerFixture) -> None:
    patch_oc4ids_json_output = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.oc4ids_json_output"
    )
    patch_oc4ids_json_output.side_effect = Exception("Mocked exception")

    with pytest.raises(Exception) as exc_info:
        validate_json(dataset_name="test_dataset", json_data={})

    assert "Validation failed" in str(exc_info.value)
    assert "Mocked exception" in str(exc_info.value)


def test_validate_json_raises_validation_errors_exception(
    mocker: MockerFixture,
) -> None:
    patch_oc4ids_json_output = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.oc4ids_json_output"
    )
    patch_oc4ids_json_output.return_value = {"validation_errors_count": 2}

    with pytest.raises(Exception) as exc_info:
        validate_json(dataset_name="test_dataset", json_data={})

    assert "Validation failed" in str(exc_info.value)
    assert "Dataset has 2 validation errors" in str(exc_info.value)


def test_write_json_to_file_writes_in_correct_format() -> None:
    with tempfile.TemporaryDirectory() as dir:
        file_name = os.path.join(dir, "test_dataset.json")
        write_json_to_file(file_name=file_name, json_data={"key": "value"})

        expected = dedent(
            """\
            {
                "key": "value"
            }"""
        )
        with open(file_name) as file:
            assert file.read() == expected


def test_write_json_to_file_raises_failure_exception(mocker: MockerFixture) -> None:
    patch_json_dump = mocker.patch("oc4ids_datastore_pipeline.pipeline.json.dump")
    patch_json_dump.side_effect = Exception("Mocked exception")

    with pytest.raises(Exception) as exc_info:
        with tempfile.TemporaryDirectory() as dir:
            file_name = os.path.join(dir, "test_dataset.json")
            write_json_to_file(file_name=file_name, json_data={"key": "value"})

            assert "Error while writing to JSON file" in str(exc_info.value)
            assert "Mocked exception" in str(exc_info.value)


def test_process_dataset_catches_exception(mocker: MockerFixture) -> None:
    patch_download_json = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.download_json"
    )
    patch_download_json.side_effect = Exception("Download failed")

    process_dataset("test_dataset", "https://test_dataset.json")
