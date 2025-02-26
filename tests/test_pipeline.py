import os
import tempfile
from textwrap import dedent

import pytest
from pytest_mock import MockerFixture

from oc4ids_datastore_pipeline.pipeline import (
    ProcessDatasetError,
    download_json,
    process_dataset,
    process_deleted_datasets,
    process_registry,
    transform_to_csv_and_xlsx,
    validate_json,
    write_json_to_file,
)


def test_download_json_raises_failure_exception(mocker: MockerFixture) -> None:
    patch_get = mocker.patch("oc4ids_datastore_pipeline.pipeline.requests.get")
    patch_get.side_effect = Exception("Mocked exception")

    with pytest.raises(ProcessDatasetError) as exc_info:
        download_json(dataset_id="test_dataset", url="https://test_dataset.json")

    assert "Download failed" in str(exc_info.value)
    assert "Mocked exception" in str(exc_info.value)


def test_validate_json_raises_failure_exception(mocker: MockerFixture) -> None:
    patch_oc4ids_json_output = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.oc4ids_json_output"
    )
    patch_oc4ids_json_output.side_effect = Exception("Mocked exception")

    with pytest.raises(ProcessDatasetError) as exc_info:
        validate_json(dataset_id="test_dataset", json_data={})

    assert "Validation failed" in str(exc_info.value)
    assert "Mocked exception" in str(exc_info.value)


def test_validate_json_raises_validation_errors_exception(
    mocker: MockerFixture,
) -> None:
    patch_oc4ids_json_output = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.oc4ids_json_output"
    )
    patch_oc4ids_json_output.return_value = {
        "validation_errors_count": 2,
        "validation_errors": [
            [
                '{"message": "Non-unique id values"}',
                [
                    {
                        "path": "projects/22/parties",
                        "value": "test_value",
                    },
                    {"path": "projects/30/parties", "value": "test_value"},
                ],
            ]
        ],
    }

    with pytest.raises(ProcessDatasetError) as exc_info:
        validate_json(dataset_id="test_dataset", json_data={})

    assert "Validation failed" in str(exc_info.value)
    assert "Dataset has 2 validation errors" in str(exc_info.value)
    assert "Non-unique id values" in str(exc_info.value)


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

    with pytest.raises(ProcessDatasetError) as exc_info:
        with tempfile.TemporaryDirectory() as dir:
            file_name = os.path.join(dir, "test_dataset.json")
            write_json_to_file(file_name=file_name, json_data={"key": "value"})

    assert "Error writing dataset to file" in str(exc_info.value)
    assert "Mocked exception" in str(exc_info.value)


def test_transform_to_csv_and_xlsx_returns_correct_paths(mocker: MockerFixture) -> None:
    mocker.patch("oc4ids_datastore_pipeline.pipeline.flattentool.flatten")

    csv_path, xlsx_path = transform_to_csv_and_xlsx("dir/dataset/dataset.json")

    assert csv_path == "dir/dataset/dataset"
    assert xlsx_path == "dir/dataset/dataset.xlsx"


def test_transform_to_csv_and_xlsx_catches_exception(mocker: MockerFixture) -> None:
    patch_flatten = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.flattentool.flatten"
    )
    patch_flatten.side_effect = Exception("Mocked exception")

    csv_path, xlsx_path = transform_to_csv_and_xlsx("dir/dataset/dataset.json")

    assert csv_path is None
    assert xlsx_path is None


def test_process_deleted_datasets(mocker: MockerFixture) -> None:
    patch_get_dataset_ids = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.get_dataset_ids"
    )
    patch_get_dataset_ids.return_value = ["old_dataset", "test_dataset"]
    patch_delete_dataset = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.delete_dataset"
    )
    patch_delete_files_for_dataset = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.delete_files_for_dataset"
    )

    registered_datasets = {"test_dataset": "https://test_dataset.json"}
    process_deleted_datasets(registered_datasets)

    patch_delete_dataset.assert_called_once_with("old_dataset")
    patch_delete_files_for_dataset.assert_called_once_with("old_dataset")


def test_process_dataset_raises_failure_exception(mocker: MockerFixture) -> None:
    patch_download_json = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.download_json"
    )
    patch_download_json.side_effect = ProcessDatasetError("Download failed: Exception")

    with pytest.raises(ProcessDatasetError) as exc_info:
        process_dataset("test_dataset", "https://test_dataset.json")

    assert "Download failed: Exception" in str(exc_info.value)


def test_process_registry_catches_exception(mocker: MockerFixture) -> None:
    patch_fetch_registered_datasets = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.fetch_registered_datasets"
    )
    patch_fetch_registered_datasets.return_value = {
        "test_dataset": "https://test_dataset.json"
    }
    mocker.patch("oc4ids_datastore_pipeline.pipeline.process_deleted_datasets")
    patch_process_dataset = mocker.patch(
        "oc4ids_datastore_pipeline.pipeline.process_dataset"
    )
    patch_process_dataset.side_effect = Exception("Mocked exception")
    mocker.patch("oc4ids_datastore_pipeline.pipeline.send_notification")

    process_registry()
