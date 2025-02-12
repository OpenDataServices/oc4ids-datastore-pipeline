import datetime
from typing import Any, Generator

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import create_engine

from oc4ids_datastore_pipeline.database import (
    Base,
    Dataset,
    delete_dataset,
    get_dataset_ids,
    save_dataset,
)


@pytest.fixture(autouse=True)
def before_and_after_each(mocker: MockerFixture) -> Generator[Any, Any, Any]:
    engine = create_engine("sqlite:///:memory:")
    patch_get_engine = mocker.patch("oc4ids_datastore_pipeline.database.get_engine")
    patch_get_engine.return_value = engine
    Base.metadata.create_all(engine)
    yield
    engine.dispose()


def test_save_dataset() -> None:
    dataset = Dataset(
        dataset_id="test_dataset",
        source_url="https://test_dataset.json",
        publisher_name="test_publisher",
        json_url="data/test_dataset.json",
        updated_at=datetime.datetime.now(datetime.UTC),
    )
    save_dataset(dataset)

    assert get_dataset_ids() == ["test_dataset"]


def test_delete_dataset() -> None:
    dataset = Dataset(
        dataset_id="test_dataset",
        source_url="https://test_dataset.json",
        publisher_name="test_publisher",
        json_url="data/test_dataset.json",
        updated_at=datetime.datetime.now(datetime.UTC),
    )
    save_dataset(dataset)

    assert get_dataset_ids() == ["test_dataset"]

    delete_dataset("test_dataset")

    assert get_dataset_ids() == []
