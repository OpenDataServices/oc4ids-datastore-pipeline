import datetime
import logging
import os

from sqlalchemy import (
    DateTime,
    Engine,
    String,
    create_engine,
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
    json_url: Mapped[str] = mapped_column(String)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(os.environ["DATABASE_URL"])
    return _engine


def save_dataset(dataset: Dataset) -> None:
    with Session(get_engine()) as session:
        session.merge(dataset)
        session.commit()
