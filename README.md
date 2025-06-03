# OC4IDS Datastore Pipeline

A Python application to validate and store published OC4IDS datasets.

## Local Development via Dev Containers or Docker Compose

You can open this repository in a dev container to get an environment complete with Postgres database.

If you prefer to use Docker Compose, you can instead run:

```
docker compose -f docker-compose.dev.yml up -d
docker compose -f docker-compose.dev.yml exec app bash
docker compose -f docker-compose.dev.yml stop
```

### Run database migrations

```
alembic upgrade head
```

### DigitalOcean Spaces bucket setup

If enabled, the pipeline will upload the files to a [DigitalOcean Spaces](https://www.digitalocean.com/products/spaces) bucket.

#### Create the bucket

First create the bucket with DigitalOcean.

If doing this via the UI, take the following steps:

1. Choose any region
2. Enable CDN
3. Choose any bucket name
4. Click "Create a Spaces Bucket"

#### Create access key

After the bucket is created, create an access key in DigitalOcean.

If doing this via the UI, take the following steps:

1. Go to your bucket
2. Go to settings
3. Under "Access Keys" click "Create Access Key"
4. Set the access scope to "Limited Access"
5. Select your bucket from the list and set "Permissions" to "Read/Write/Delete"
6. Choose any name
7. Click "Create Access Key"

Securely store the access key ID and secret.

#### Set the required environment variables

Once you have created the bucket and access key, set the following environment variables for the pipeline:

- `ENABLE_UPLOAD`: 1 to enable, 0 to disable
- `BUCKET_REGION`: e.g. `fra1`
- `BUCKET_NAME`: e.g. `my-bucket`
- `BUCKET_ACCESS_KEY_ID`: e.g. `access-key-id`
- `BUCKET_ACCESS_KEY_SECRET`: e.g. `access-key-secret`

To make this easier, the project uses [`python-dotenv`](https://github.com/theskumar/python-dotenv) to load environment variables from a config file.
For local development, create a file called `.env.local`, which will be used by default.
You can change which file is loaded setting the environment variable `APP_ENV`.
For example the tests set `APP_ENV=test`, which loads variables from `.env.test`.

### Email notification environment variables

To send failure notifications by email, the following environment variables must be set:

- `NOTIFICATIONS_ENABLED`: 1 to enable, 0 to disable
- `NOTIFICATIONS_SMTP_HOST`
- `NOTIFICATIONS_SMTP_PORT`
- `NOTIFICATIONS_SMTP_SSL_ENABLED`: 1 to enable, 0 to disable
- `NOTIFICATIONS_SENDER_EMAIL`
- `NOTIFICATIONS_RECEIVER_EMAIL`

### Run app

```
oc4ids-datastore-pipeline
```
### Access Database

From inside the dev container or Docker container:

```
psql postgresql://postgres:postgres@localhost:5432/postgres
```

Connecting from outside:
* If using a dev container or Docker Compose locally the same command should work
* In GitHub Codespaces, we're not sure how to access the port

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

## Releasing

To publish a new version, raise a PR to `main` updating the version in `pyproject.toml`. Once merged, create a git tag and GitHub release for the new version, with naming `vX.Y.Z`. This will trigger a docker image to to be built and pushed, tagged with the version and `latest`.
