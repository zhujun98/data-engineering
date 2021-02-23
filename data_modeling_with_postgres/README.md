# Data Modeling with PostgreSQL


## Installing dependencies

```sh
pip install pandas psycopg2
```

## Running Postgres locally

```sh
# Pull Postgres image from DockerHub.
docker pull postgres

# Start a Postgres server.
docker run --name postgres-server -e POSTGRES_PASSWORD=student -e POSTGRES_USER=student -p 127.0.0.1:5432:5432 -d postgres
```

## Getting started


