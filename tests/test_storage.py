import os
import tempfile
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from oc4ids_datastore_pipeline.storage import upload_files


@pytest.fixture(autouse=True)
def mock_client(mocker: MockerFixture) -> Any:
    os.environ["ENABLE_UPLOAD"] = "1"
    mock_boto3_client = MagicMock()
    patch_boto3_client = mocker.patch(
        "oc4ids_datastore_pipeline.storage.boto3.session.Session.client"
    )
    patch_boto3_client.return_value = mock_boto3_client
    return mock_boto3_client


def test_upload_files_upload_disabled(mock_client: MagicMock) -> None:
    os.environ["ENABLE_UPLOAD"] = "0"

    json_public_url, csv_public_url, xlsx_public_url = upload_files(
        "test_dataset",
        json_path="dataset.json",
        csv_path="dataset_csv.zip",
        xlsx_path="dataset.xlsx",
    )

    mock_client.assert_not_called()
    assert json_public_url is None
    assert csv_public_url is None
    assert xlsx_public_url is None


def test_upload_files_nothing_to_upload(mock_client: MagicMock) -> None:
    json_public_url, csv_public_url, xlsx_public_url = upload_files("test_dataset")

    mock_client.assert_not_called()
    assert json_public_url is None
    assert csv_public_url is None
    assert xlsx_public_url is None


def test_upload_files_json(mock_client: MagicMock) -> None:
    json_public_url, csv_public_url, xlsx_public_url = upload_files(
        "test_dataset", json_path="data/test_dataset/test_dataset.json"
    )

    mock_client.upload_file.assert_called_once_with(
        "data/test_dataset/test_dataset.json",
        "test-bucket",
        "test_dataset/test_dataset.json",
        ExtraArgs={"ACL": "public-read", "ContentType": "application/json"},
    )
    assert (
        json_public_url
        == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset.json"  # noqa: E501
    )
    assert csv_public_url is None
    assert xlsx_public_url is None


def test_upload_files_json_catches_exception(mock_client: MagicMock) -> None:
    mock_client.upload_file.side_effect = [Exception("Mock exception"), None, None]

    with tempfile.TemporaryDirectory() as csv_dir:
        json_public_url, csv_public_url, xlsx_public_url = upload_files(
            "test_dataset",
            json_path="data/test_dataset/test_dataset.json",
            csv_path=csv_dir,
            xlsx_path="data/test_dataset/test_dataset.xlsx",
        )
        assert json_public_url is None
        assert (
            csv_public_url
            == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset_csv.zip"  # noqa: E501
        )
        assert (
            xlsx_public_url
            == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset.xlsx"  # noqa: E501
        )


def test_upload_files_csv(mock_client: MagicMock) -> None:
    with tempfile.TemporaryDirectory() as csv_dir:
        json_public_url, csv_public_url, xlsx_public_url = upload_files(
            "test_dataset", csv_path=csv_dir
        )

    mock_client.upload_file.assert_called_once_with(
        f"{csv_dir}_csv.zip",
        "test-bucket",
        "test_dataset/test_dataset_csv.zip",
        ExtraArgs={"ACL": "public-read", "ContentType": "application/zip"},
    )
    assert json_public_url is None
    assert (
        csv_public_url
        == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset_csv.zip"  # noqa: E501
    )
    assert xlsx_public_url is None


def test_upload_files_csv_catches_exception(mock_client: MagicMock) -> None:
    mock_client.upload_file.side_effect = [None, Exception("Mock exception"), None]

    with tempfile.TemporaryDirectory() as csv_dir:
        json_public_url, csv_public_url, xlsx_public_url = upload_files(
            "test_dataset",
            json_path="data/test_dataset/test_dataset.json",
            csv_path=csv_dir,
            xlsx_path="data/test_dataset/test_dataset.xlsx",
        )
        assert (
            json_public_url
            == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset.json"  # noqa: E501
        )
        assert csv_public_url is None
        assert (
            xlsx_public_url
            == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset.xlsx"  # noqa: E501
        )


def test_upload_files_xlsx(mock_client: MagicMock) -> None:
    json_public_url, csv_public_url, xlsx_public_url = upload_files(
        "test_dataset", xlsx_path="data/test_dataset/test_dataset.xlsx"
    )

    mock_client.upload_file.assert_called_once_with(
        "data/test_dataset/test_dataset.xlsx",
        "test-bucket",
        "test_dataset/test_dataset.xlsx",
        ExtraArgs={
            "ACL": "public-read",
            "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # noqa: E501
        },
    )
    assert json_public_url is None
    assert csv_public_url is None
    assert (
        xlsx_public_url
        == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset.xlsx"  # noqa: E501
    )


def test_upload_files_xlsx_catches_exception(mock_client: MagicMock) -> None:
    mock_client.upload_file.side_effect = [None, None, Exception("Mock exception")]

    with tempfile.TemporaryDirectory() as csv_dir:
        json_public_url, csv_public_url, xlsx_public_url = upload_files(
            "test_dataset",
            json_path="data/test_dataset/test_dataset.json",
            csv_path=csv_dir,
            xlsx_path="data/test_dataset/test_dataset.xlsx",
        )
        assert (
            json_public_url
            == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset.json"  # noqa: E501
        )
        assert (
            csv_public_url
            == "https://test-bucket.test-region.digitaloceanspaces.com/test_dataset/test_dataset_csv.zip"  # noqa: E501
        )
        assert xlsx_public_url is None
