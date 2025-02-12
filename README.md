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

To enable files to be uploaded to S3-compatible storage, the following environment variables must be set:

- `ENABLE_UPLOAD`: 1 to enable, 0 to disable
- `BUCKET_REGION`:
- `BUCKET_NAME`
- `BUCKET_ACCESS_KEY_ID`
- `BUCKET_ACCESS_KEY_SECRET`

To make this easier, the project uses [`python-dotenv`](https://github.com/theskumar/python-dotenv) to load environment variables from a config file.
For local development, create a file called `.env.local`, which will be used by default.
You can change which file is loaded setting the environment variable `APP_ENV`.
For example the tests set `APP_ENV=test`, which loads variables from `.env.test`.

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
