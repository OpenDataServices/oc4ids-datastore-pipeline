# OC4IDS Datastore Pipeline

A Python application to validate and store published OC4IDS datasets.

## Local Development

### Prerequisites

- Python 3.12

### Install Python requirements

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements_dev.txt
```

### Run app

```
pip install -e .
oc4ids-datastore-pipeline
```

### Run linting and type checking

```
black oc4ids_datastore_pipeline/
isort oc4ids_datastore_pipeline/
flake8 oc4ids_datastore_pipeline/
mypy oc4ids_datastore_pipeline/
```

### Run tests

```
pytest
```
