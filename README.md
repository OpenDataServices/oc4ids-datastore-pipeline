# OC4IDS Datastore Pipeline

A Python application to validate and store published OC4IDS datasets.

## Local Development

### Prerequisites

- Python 3.12
- Postgres

### Install Python requirements

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements_dev.txt
```

### Set database enrivonment variable

```
export DATABASE_URL="postgresql://oc4ids_datastore@localhost/oc4ids_datastore"
```

### Run database migrations

```
alembic upgrade head
```

### S3 environment variables

<!-- TODO: Document -->

### Run app

```
pip install -e .
oc4ids-datastore-pipeline
```

### Run linting and type checking

```
black oc4ids_datastore_pipeline/ tests/
isort oc4ids_datastore_pipeline/ tests/
flake8 oc4ids_datastore_pipeline/ tests/
mypy oc4ids_datastore_pipeline/ tests/
```

### Run tests

```
pytest
```

### Generating new database migrations

```
alembic revision --autogenerate -m "<MESSAGE HERE>"
```
