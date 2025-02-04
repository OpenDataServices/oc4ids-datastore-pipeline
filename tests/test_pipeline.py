import pytest
from pytest_mock import MockerFixture

from oc4ids_datastore_pipeline.pipeline import download_json, process_dataset


def test_download_json_raises_failure_exception(mocker: MockerFixture) -> None:
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.side_effect = Exception("Mocked exception")

    with pytest.raises(Exception) as exc_info:
        download_json(url="https://test_dataset.json")

    assert "Download failed" in str(exc_info.value)
    assert "Mocked exception" in str(exc_info.value)


def test_process_dataset_catches_exception(mocker: MockerFixture) -> None:
    patch_download_json = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.download_json"
    )
    patch_download_json.side_effect = Exception("Download failed")

    process_dataset("test_dataset", "https://test_dataset.json")
