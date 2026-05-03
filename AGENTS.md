# AGENTS.md — AI Agent Context for ACME Delivery Analytics Pipeline

This file provides full context for AI agents working on this project. Read it before making any changes.

---

## Project Purpose

Real-time analytics pipeline for ACME Delivery Services. The source OLTP database is continuously updated by `pg_cron` stored procedures. Changes are captured via Debezium CDC, streamed through Kafka, and loaded into a PostgreSQL star-schema analytics database. A FastAPI layer exposes analytics endpoints. Metabase provides dashboards.

---

## Repository Layout

```
.
├── api/                        FastAPI service
│   ├── main.py                 All endpoints + status filter logic
│   ├── Dockerfile
│   └── requirements.txt
├── ingestion/                  Kafka consumer / CDC transformer
│   ├── consumer.py             Core ETL logic
│   ├── Dockerfile
│   └── requirements.txt
├── db-scripts/
│   ├── initialize_db_ddl.sql   Source DB schema + pg_cron jobs (DO NOT EDIT — pre-built)
│   └── analytics_init.sql      Analytics star schema DDL (dim + fact tables)
├── debezium/
│   ├── connectors/finance-db-connector.json   Debezium connector config
│   └── init-connectors.sh      Registers connector via REST on startup
├── docker/postgres-db/         Custom Postgres image with pg_cron
│   └── Dockerfile
├── docker-compose.yaml         Full stack definition
├── diagrams/database-diagram.png
├── README.md                   User-facing documentation
├── TASK.md                     Original task requirements
└── AGENTS.md                   This file
```

---

## Services

| Service | Image | Internal host | Port (host) | Role |
|---|---|---|---|---|
| `transactions-db` | custom postgres:15 + pg_cron | `transactions-db` | `5432` | Source OLTP DB |
| `zookeeper` | cp-zookeeper:7.5.0 | `zookeeper` | `2181` | Kafka coordination |
| `kafka` | cp-kafka:7.5.0 | `kafka` | `9092` | Message broker |
| `kafka-connect` | debezium/connect:2.4 | `kafka-connect` | `8083` | CDC connector host |
| `debezium-init` | curlimages/curl | — | — | One-shot: registers Debezium connector |
| `analytics-db` | postgres:15 | `analytics-db` | `5433` | Analytics warehouse |
| `ingestion` | custom python:3.11-slim | — | — | Kafka consumer → analytics-db |
| `api` | custom python:3.11-slim | — | `8000` | FastAPI analytics layer |
| `metabase` | metabase/metabase:v0.51.4 | — | `3000` | BI dashboards |
| `pgadmin` | dpage/pgadmin4 | — | `5050` | DB admin UI (profile: sql-tools) |
| `kafka-ui` | provectuslabs/kafka-ui | — | `8080` | Kafka browser (profile: monitoring) |

### Starting optional services
```bash
docker compose --profile sql-tools up -d pgadmin
docker compose --profile monitoring up -d kafka-ui
```

---

## Credentials

### Source DB (`transactions-db`)
- Host (external): `localhost:5432` | Host (internal): `transactions-db:5432`
- User: `finance_db_user` / Password: `1234` / DB: `finance_db`
- CDC user: `cdc_user` / Password: `cdc_1234` (used by Debezium only)

### Analytics DB (`analytics-db`)
- Host (external): `localhost:5433` | Host (internal): `analytics-db:5432`
- User: `analytics_user` / Password: `analytics_1234` / DB: `analytics_db`

---

## Source Database Schema (`operations` schema)

```sql
customers    (customer_id PK, customer_name, is_active, customer_address, updated_at, ...)
products     (product_id PK, product_name, barcode, unity_price DECIMAL, is_active, updated_at, ...)
orders       (order_id PK, order_date DATE, delivery_date DATE, customer_id, status VARCHAR, updated_at, ...)
order_items  (order_item_id PK, order_id, product_id, quanity INTEGER, updated_at, ...)
```

### Critical source-schema quirk
**`order_items.quanity` is a typo** in the original schema (missing 'n'). The ingestion consumer reads it as `row.get("quanity")`. Never correct this to `quantity` in the consumer — it would break ingestion silently.

### pg_cron jobs (auto-runs every 1-2 minutes)
- `update_customers(5)` — upserts 5 random customers every 2 min
- `update_products(10)` — upserts 10 random products (with random price changes) every 2 min
- `generate_orders(100)` — generates 100 orders (new or status updates) with up to 25 items each, every 1 min

Order statuses generated: `PENDING`, `PROCESSING`, `REPROCESSING`, `COMPLETED`.  
New orders always start as `PENDING`. Existing orders may be updated to `PROCESSING`, `COMPLETED`, or `REPROCESSING`.

---

## Kafka Topics

| Topic | Source Table |
|---|---|
| `finance_db.operations.customers` | `operations.customers` |
| `finance_db.operations.products` | `operations.products` |
| `finance_db.operations.orders` | `operations.orders` |
| `finance_db.operations.order_items` | `operations.order_items` |

### Debezium message format
Every message is a JSON object (schemas disabled):
```json
{
  "before": { ... } or null,
  "after":  { ... } or null,
  "op":     "c" | "u" | "d" | "r",
  "source": { ... }
}
```
- `op=r` = snapshot read, `c` = insert, `u` = update, `d` = delete

### Debezium type encodings
| PostgreSQL type | Debezium wire format | Decode |
|---|---|---|
| `DATE` | integer days since 1970-01-01 | `date(1970,1,1) + timedelta(days=value)` |
| `TIMESTAMP(3)` | integer milliseconds since epoch | `datetime.utcfromtimestamp(value / 1_000)` |
| `DECIMAL` | `{"scale": N, "value": "<base64>"}` | `int.from_bytes(b64decode(value), "big", signed=True) / 10**scale` |

---

## Analytics Schema (`analytics` schema in `analytics-db`)

### Dimensions

| Table | Type | Key columns |
|---|---|---|
| `dim_date` | Static lookup | `date_sk` (YYYYMMDD int), `full_date`, `year/month/quarter/week/day_*`, `is_weekend` |
| `dim_order_status` | Static lookup | `status_sk`, `status_code`, `is_open` |
| `dim_customer` | SCD Type 2 | `customer_sk` (surrogate), `customer_id` (natural), `valid_from`, `valid_to`, `is_current` |
| `dim_product` | SCD Type 2 | `product_sk` (surrogate), `product_id` (natural), `unity_price`, `valid_from`, `valid_to`, `is_current` |

### Facts

| Table | Grain | Key columns |
|---|---|---|
| `fact_orders` | One row per order | `order_id` (PK), `customer_sk`, `order_date_sk`, `delivery_date_sk`, `status_sk`, `updated_at` |
| `fact_order_items` | One row per order line | `order_item_id` (PK), `order_id`, `product_sk`, `customer_sk`, `delivery_date_sk`, `status_sk`, `quantity`, `unity_price`, `line_value` (generated) |

### `dim_order_status` seed values
| status_code | is_open |
|---|---|
| PENDING | TRUE |
| PROCESSING | TRUE |
| REPROCESSING | TRUE |
| COMPLETED | FALSE |

### SCD Type 2 logic (dim_customer, dim_product)
1. Natural key not found → INSERT new row (`is_current=TRUE`, `valid_to=NULL`)
2. Natural key found, tracked attributes changed → UPDATE existing row (`valid_to=event_ts`, `is_current=FALSE`) + INSERT new row
3. Natural key found, no attribute change → no-op

---

## Ingestion Consumer (`ingestion/consumer.py`)

### Processing order
1. All 4 topics consumed in one Kafka poll batch
2. Each message committed to analytics-db individually (per-message transaction)
3. `handle_order_item` returns `False` if parent order or product is not yet in the DB → pushed to `retry_items` deque
4. After batch: retry deque is flushed; items still failing re-enter deque for next cycle

### Status cascade
When `handle_order` processes an order update, it also runs:
```sql
UPDATE analytics.fact_order_items SET status_sk=<new> WHERE order_id=<id>
```
This keeps item-level status consistent with the order without re-processing items.

### Price comparison (float precision)
Product SCD2 change detection uses `round(float(price), 6)` to avoid false positives from base64 decode vs PostgreSQL DECIMAL precision differences.

---

## API (`api/main.py`)

Base URL: `http://localhost:8000`

### Status filter (all endpoints)
All endpoints accept `?status=` with these values:

| Value | SQL effect |
|---|---|
| `open` (default) | `WHERE ds.is_open` |
| `closed` | `WHERE NOT ds.is_open` |
| `PENDING` / `PROCESSING` / `REPROCESSING` / `COMPLETED` | `WHERE ds.status_code = '<value>'` |

Invalid values return HTTP 422 with a descriptive message.

### Endpoints

| Method | Path | Extra params | Description |
|---|---|---|---|
| GET | `/health` | — | Liveness probe |
| GET | `/analytics/orders` | `?status=open` | Orders by delivery date + status |
| GET | `/analytics/orders/top` | `?limit=3&status=open` | Top N delivery dates by order count |
| GET | `/analytics/orders/product` | `?status=open` | Pending items grouped by product |
| GET | `/analytics/orders/customers` | `?limit=3&status=open` | Top N customers by order count |

---

## Common Operations

### Rebuild a single service after code change
```bash
docker compose build --no-cache <service>
docker compose up -d --force-recreate <service>
```

### View ingestion logs
```bash
docker logs -f deel-data-engineering-task-ingestion-1
```

### Check Debezium connector
```bash
curl http://localhost:8083/connectors/finance-db-connector/status
```

### Query analytics DB directly
```bash
psql -h localhost -p 5433 -U analytics_user -d analytics_db
```

### Check consumer group lag
```bash
docker exec deel-data-engineering-task-kafka-1 \
  kafka-consumer-groups --describe --group analytics-consumer --bootstrap-server localhost:29092
```

---

## Design Decisions

- **CDC over batch polling**: Debezium logical replication captures every row-level change with sub-second latency, keeping the analytics model continuously up to date without polling the source.
- **SCD Type 2 for customers and products**: Preserves historical context so analytics can reflect the state of the world at the time each order was placed, not just current attribute values.
- **Status cascade on orders**: When an order's status changes, `fact_order_items` rows for that order are updated in the same transaction, keeping item-level reporting consistent with order-level status without duplicating event processing logic.
- **Retry buffer for out-of-order events**: Kafka occasionally delivers `order_items` before the parent order or product is visible in the analytics DB. A deque-backed retry buffer re-processes these items after the current batch is flushed, avoiding data loss without requiring complex dead-letter queue infrastructure.
- **`line_value` as a generated column**: Computed at write time so reads pay no runtime multiplication cost and the value is always consistent with the stored `quantity` and `unity_price`.
- **`status` API filter**: All analytics endpoints accept `?status=open|closed|<code>` instead of hardcoding `is_open=TRUE`, allowing callers to query any subset of orders without separate endpoints.

---
