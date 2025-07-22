import datetime
import logging
import os
from typing import Optional

from sqlalchemy import (
    DateTime,
    Engine,
    String,
    create_engine,
    delete,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

logger = logging.getLogger(__name__)

_engine = None


class Base(DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = "dataset"

    dataset_id: Mapped[str] = mapped_column(String, primary_key=True)
    source_url: Mapped[str] = mapped_column(String)
    publisher_name: Mapped[str] = mapped_column(String)
    publisher_country: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    license_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    license_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    license_title_short: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    json_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    csv_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    xlsx_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))
    portal_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    portal_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(os.environ["DATABASE_URL"])
    return _engine


def save_dataset(dataset: Dataset) -> None:
    with Session(get_engine()) as session:
        session.merge(dataset)
        session.commit()


def delete_dataset(dataset_id: str) -> None:
    with Session(get_engine()) as session:
        session.execute(delete(Dataset).where(Dataset.dataset_id == dataset_id))
        session.commit()


def get_dataset_ids() -> list[str]:
    with Session(get_engine()) as session:
        return [
            dataset_id for dataset_id in session.scalars(select(Dataset.dataset_id))
        ]
