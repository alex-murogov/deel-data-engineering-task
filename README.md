# ACME Delivery Analytics Pipeline

## Overview

Real-time analytics pipeline for ACME Delivery Services built on top of a pre-existing transactional PostgreSQL database. Changes to orders, customers, and products are captured via Debezium CDC, streamed through Kafka, transformed into a star-schema dimensional model, and exposed through a FastAPI analytics layer.

```
transactions-db (PostgreSQL)
       │  logical replication
       ▼
  kafka-connect (Debezium)
       │  Kafka topics
       ▼
     kafka
       │  consumer
       ▼
  ingestion service ──► analytics-db (PostgreSQL, star schema)
                                │
                                ▼
                           api (FastAPI :8000)
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes `docker compose`)
- Ports `5432`, `5433`, `2181`, `9092`, `8083`, `8000` must be free on your machine

---

## Running the Application

```bash
docker compose up --build
```

All services start automatically in dependency order. The pipeline is ready when you see ingestion logs printing processed messages. End-to-end startup typically takes 30–60 seconds.

To stop and remove containers:

```bash
docker compose down
```

To rebuild a specific service after code changes:

```bash
docker compose build --no-cache <service>
docker compose up -d --force-recreate <service>
```

---

## Services

| Service | Image / Build | Port | Description |
|---|---|---|---|
| `transactions-db` | `./docker/postgres-db` | `5432` | Source OLTP database (`finance_db`). Runs `pg_cron` to generate synthetic orders continuously. |
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | `2181` | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | `9092` | Message broker |
| `kafka-connect` | `debezium/connect:2.4` | `8083` | Debezium Kafka Connect cluster |
| `debezium-init` | `curlimages/curl` | — | One-shot container that registers the Debezium connector via REST |
| `analytics-db` | `postgres:15` | `5433` | Analytics data warehouse (`analytics_db`). Initialized with the star schema DDL. |
| `ingestion` | `./ingestion` | — | Python Kafka consumer that transforms CDC events and loads the dimensional model |
| `api` | `./api` | `8000` | FastAPI analytics layer querying `analytics-db` |

### Source Database Credentials

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `finance_db` |
| User | `finance_db_user` |
| Password | `1234` |

### Analytics Database Credentials

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5433` |
| Database | `analytics_db` |
| User | `analytics_user` |
| Password | `analytics_1234` |

---

## Kafka Topics

| Topic | Source Table |
|---|---|
| `finance_db.operations.customers` | `operations.customers` |
| `finance_db.operations.products` | `operations.products` |
| `finance_db.operations.orders` | `operations.orders` |
| `finance_db.operations.order_items` | `operations.order_items` |

---

## Dimensional Model (Star Schema)

All tables live in the `analytics` schema of `analytics-db`.

```
dim_date ◄──────────────────── fact_orders ────────────────► dim_customer (SCD2)
                                      │                            │
                                      └──── dim_order_status       │
                                                                   │
dim_date ◄────────── fact_order_items ─────────────────────────────┘
                            │
                    dim_product (SCD2)
                    dim_order_status
                    dim_customer (SCD2)
```

### Dimensions

| Table | Type | Description |
|---|---|---|
| `dim_date` | Static lookup | Pre-populated 2020-01-01 → 2030-12-31. Surrogate key is `YYYYMMDD` integer. |
| `dim_order_status` | Static lookup | `PENDING`, `PROCESSING`, `REPROCESSING` (open), `COMPLETED` (closed). |
| `dim_customer` | SCD Type 2 | Full history of customer attribute changes via `valid_from` / `valid_to` / `is_current`. |
| `dim_product` | SCD Type 2 | Full history of product price and name changes. |

### Facts

| Table | Grain | Notable Columns |
|---|---|---|
| `fact_orders` | One row per order | `delivery_date_sk`, `status_sk` kept current via upsert |
| `fact_order_items` | One row per order line | `line_value` is a generated column (`quantity * unity_price`); `status_sk` cascades from parent order status changes |

### SCD Type 2 Upsert Logic

When a CDC event arrives for a customer or product:

1. If the natural key is new → insert the first row (`is_current = TRUE`, `valid_to = NULL`).
2. If tracked attributes changed → expire the current row (`valid_to = event_ts`, `is_current = FALSE`) and insert a new current row.
3. If no tracked attributes changed → no-op.

This ensures both current state and full history are queryable at any point in time.

---

## API Endpoints

Base URL: `http://localhost:8000`

### `GET /health`

Liveness probe.

```json
{"status": "ok"}
```

---

### `GET /analytics/orders`

Open orders grouped by delivery date and status.

**Response**

```json
[
  {
    "delivery_date": "2025-06-01",
    "status": "PENDING",
    "order_count": 12
  }
]
```

---

### `GET /analytics/orders/top?limit=3`

Top N delivery dates ranked by open order count.

| Parameter | Default | Range |
|---|---|---|
| `limit` | `3` | 1–100 |

**Response**

```json
[
  {"delivery_date": "2025-06-01", "order_count": 42}
]
```

---

### `GET /analytics/orders/product`

Pending item quantities grouped by product (open orders only).

**Response**

```json
[
  {
    "product_id": 7,
    "product_name": "Widget A",
    "pending_quantity": 150,
    "item_count": 30
  }
]
```

---

### `GET /analytics/orders/customers?limit=3`

Top N customers ranked by number of open orders.

| Parameter | Default | Range |
|---|---|---|
| `limit` | `3` | 1–100 |

**Response**

```json
[
  {
    "customer_id": 42,
    "customer_name": "Acme Corp",
    "order_count": 17
  }
]
```

---

## Design Decisions

- **CDC over batch polling**: Debezium logical replication captures every row-level change with sub-second latency, keeping the analytics model continuously up to date without polling the source.
- **SCD Type 2 for customers and products**: Preserves historical context so analytics can reflect the state of the world at the time each order was placed, not just current attribute values.
- **Status cascade on orders**: When an order's status changes, `fact_order_items` rows for that order are updated in the same transaction, keeping item-level reporting consistent with order-level status without duplicating event processing logic.
- **Retry buffer for out-of-order events**: Kafka occasionally delivers `order_items` before the parent order or product is visible in the analytics DB. A deque-backed retry buffer re-processes these items after the current batch is flushed, avoiding data loss without requiring complex dead-letter queue infrastructure.
- **`line_value` as a generated column**: Computed at write time so reads pay no runtime multiplication cost and the value is always consistent with the stored `quantity` and `unity_price`.
