# ACME Delivery Analytics Pipeline

## Overview

This project implements a Dockerized analytics pipeline using:
- PostgreSQL source OLTP database with `pg_cron` synthetic data generation
- Debezium CDC connector for change capture
- Kafka and Kafka Connect for event streaming
- Analytics warehouse PostgreSQL database with a star schema
- FastAPI REST API for analytics queries
- Optional monitoring and SQL tooling via Docker Compose profiles

The repository is fully Dockerized so the application can be run with a single `docker compose` command.

## Prerequisites

- Docker Engine installed
- Docker Compose plugin available
- macOS or Linux environment

## Run the application

Start the full stack:

```bash
docker compose up --build
```

Start detached:

```bash
docker compose up --build -d
```

Stop and remove containers:

```bash
docker compose down
```

## Services

| Service | URL / Port | Purpose |
|---|---|---|
| API | http://localhost:8000 | Analytics REST API |
| API docs | http://localhost:8000/docs | Swagger UI |
| Metabase | http://localhost:3000 | BI dashboard |
| Kafka Connect | http://localhost:8083 | Debezium connector REST API |
| Source PostgreSQL | `localhost:5432` | OLTP operations database |
| Analytics PostgreSQL | `localhost:5433` | Analytics warehouse |

Optional tools via profiles:
| pgAdmin | http://localhost:5050 | PostgreSQL UI |
| Kafka UI | http://localhost:8080 | Kafka cluster monitoring |

## Docker Compose profiles

Use the `sql-tools` profile for database tooling and validation:

```bash
docker compose --profile sql-tools up -d pgadmin
```

Run Great Expectations tests on the analytics warehouse:

```bash
docker compose --profile sql-tools run --rm great_expectations
```

Start Kafka UI monitoring:

```bash
docker compose --profile monitoring up -d kafka-ui
```

## API usage

Health check:

```bash
curl http://localhost:8000/health
```

Fetch open orders grouped by delivery date and status:

```bash
curl "http://localhost:8000/analytics/orders?status=open"
```

Fetch top delivery dates by open order count:

```bash
curl "http://localhost:8000/analytics/orders/top?limit=3&status=open"
```

Fetch pending items by product:

```bash
curl "http://localhost:8000/analytics/orders/product?status=open"
```

Fetch top customers by open order count:

```bash
curl "http://localhost:8000/analytics/orders/customers?limit=3&status=open"
```

## Database connection details

### Source DB (`finance_db`)

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `finance_db` |
| User | `finance_db_user` |
| Password | `1234` |

### Analytics DB (`analytics_db`)

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5433` |
| Database | `analytics_db` |
| User | `analytics_user` |
| Password | `analytics_1234` |

### Local psql commands

```bash
psql -h localhost -p 5432 -U finance_db_user -d finance_db
psql -h localhost -p 5433 -U analytics_user -d analytics_db
```

## Optional UI tools

### pgAdmin

Open pgAdmin at `http://localhost:5050` and sign in with:

- Email: `admin@acme.com`
- Password: `admin123`

Add database servers using internal Docker hostnames:

| DB | Host | Port | User | Password |
|---|---|---|---|---|
| Source | `transactions-db` | `5432` | `finance_db_user` | `1234` |
| Analytics | `analytics-db` | `5432` | `analytics_user` | `analytics_1234` |

### Metabase

Open Metabase at `http://localhost:3000` and add a PostgreSQL database with:

- Host: `analytics-db`
- Port: `5432`
- Database: `analytics_db`
- Username: `analytics_user`
- Password: `analytics_1234`

### Kafka UI

Open Kafka UI at `http://localhost:8080` after starting the monitoring profile:

```bash
docker compose --profile monitoring up -d kafka-ui
```

## Data quality validation

Run the data quality checks against the analytics warehouse:

```bash
docker compose --profile sql-tools run --rm great_expectations
```

## Troubleshooting

If the API is not ready, check container logs:

```bash
docker compose logs -f api analytics-db ingestion kafka-connect
```

If Kafka Connect is not reachable:

```bash
curl http://localhost:8083/connectors
```

If the analytics database has no tables, inspect the initialization script and container logs:

```bash
docker logs deel-data-engineering-task-analytics-db-1
```

## Notes

- The pipeline is designed to load operational changes from the source database into the analytics warehouse using CDC.
- The API exposes aggregated business metrics on open orders, delivery date ranking, product backlog, and top customers.
- The repository is Docker-native, so minimal local setup is required beyond Docker and Docker Compose.
