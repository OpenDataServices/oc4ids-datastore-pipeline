import pytest
from pytest_mock import MockerFixture

from oc4ids_datastore_pipeline.pipeline import (
    download_json,
    process_dataset,
    validate_json,
)


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


def test_process_dataset_catches_exception(mocker: MockerFixture) -> None:
    patch_download_json = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.download_json"
    )
    patch_download_json.side_effect = Exception("Download failed")

    process_dataset("test_dataset", "https://test_dataset.json")
