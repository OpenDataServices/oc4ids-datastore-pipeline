FROM python:3.12-slim

RUN apt-get update \
  && apt-get install -y libpq-dev gcc

WORKDIR /oc4ids_datastore_pipeline

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN pip install .

ENTRYPOINT ["sh", "-c", "alembic upgrade head && oc4ids-datastore-pipeline"]
