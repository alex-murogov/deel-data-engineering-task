# ACME Delivery Analytics Pipeline

## Quick Start

```bash
docker compose up --build
```

All services start in dependency order. Stop everything:

```bash
docker compose down
```

---

## Service URLs

| Service | URL | Notes |
|---|---|---|
| API | http://localhost:8000 | FastAPI analytics layer |
| API docs | http://localhost:8000/docs | Swagger UI |
| Metabase | http://localhost:3000 | BI dashboards |
| Kafka Connect | http://localhost:8083 | Debezium REST API |
| Source DB | `localhost:5432` | PostgreSQL OLTP |
| Analytics DB | `localhost:5433` | PostgreSQL star schema |

Optional services:

```bash
docker compose --profile monitoring up -d kafka-ui     # http://localhost:8080
docker compose --profile sql-tools up -d pgadmin       # http://localhost:5050
```

---

## Database Connections

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

### psql

```bash
# Source DB
psql -h localhost -p 5432 -U finance_db_user -d finance_db

# Analytics DB
psql -h localhost -p 5433 -U analytics_user -d analytics_db
```

### pgAdmin (`http://localhost:5050`)

Login: `admin@acme.com` / `admin123`

Add servers using the internal Docker hostnames:

| DB | Host | Port | User | Password |
|---|---|---|---|---|
| Source | `transactions-db` | `5432` | `finance_db_user` | `1234` |
| Analytics | `analytics-db` | `5432` | `analytics_user` | `analytics_1234` |

### Metabase (`http://localhost:3000`)

On first run add a PostgreSQL connection:

| Field | Value |
|---|---|
| Host | `analytics-db` |
| Port | `5432` |
| Database | `analytics_db` |
| Username | `analytics_user` |
| Password | `analytics_1234` |

---

## API Endpoints

All endpoints accept `?status=` with values: `open` (default), `closed`, `PENDING`, `PROCESSING`, `REPROCESSING`, `COMPLETED`.

| Method | Endpoint | Params | Description |
|---|---|---|---|
| GET | `/health` | — | Liveness probe |
| GET | `/analytics/orders` | `?status=open` | Orders grouped by delivery date and status |
| GET | `/analytics/orders/top` | `?limit=3&status=open` | Top N delivery dates by order count |
| GET | `/analytics/orders/product` | `?status=open` | Pending items grouped by product |
| GET | `/analytics/orders/customers` | `?limit=3&status=open` | Top N customers by order count |

---

## Analytics SQL Queries

Connect to `analytics_db` and run queries in the `analytics` schema.

### Open orders by delivery date and status

```sql
SELECT
    dd.full_date                AS delivery_date,
    ds.status_code              AS status,
    COUNT(1)                    AS order_count
FROM analytics.fact_orders fo
JOIN analytics.dim_order_status ds ON ds.status_sk = fo.status_sk
JOIN analytics.dim_date         dd ON dd.date_sk   = fo.delivery_date_sk
WHERE ds.is_open
GROUP BY dd.full_date, ds.status_code
ORDER BY dd.full_date, ds.status_code;
```

### Top 3 delivery dates with most open orders

```sql
SELECT
    dd.full_date                AS delivery_date,
    COUNT(1)                    AS order_count
FROM analytics.fact_orders fo
JOIN analytics.dim_order_status ds ON ds.status_sk = fo.status_sk
JOIN analytics.dim_date         dd ON dd.date_sk   = fo.delivery_date_sk
WHERE ds.is_open
GROUP BY dd.full_date
ORDER BY order_count DESC
LIMIT 3;
```

### Pending items by product (open orders)

```sql
SELECT
    dp.product_id,
    dp.product_name,
    COALESCE(SUM(foi.quantity), 0)  AS pending_quantity,
    COUNT(1)                        AS item_count
FROM analytics.fact_order_items foi
JOIN analytics.dim_order_status ds ON ds.status_sk  = foi.status_sk
JOIN analytics.dim_product      dp ON dp.product_sk = foi.product_sk
WHERE ds.is_open
  AND dp.is_current
GROUP BY dp.product_id, dp.product_name
ORDER BY pending_quantity DESC;
```

### Top 3 customers with most open orders

```sql
SELECT
    dc.customer_id,
    dc.customer_name,
    COUNT(DISTINCT fo.order_id)     AS order_count
FROM analytics.fact_orders fo
JOIN analytics.dim_customer     dc ON dc.customer_sk = fo.customer_sk
JOIN analytics.dim_order_status ds ON ds.status_sk   = fo.status_sk
WHERE ds.is_open
  AND dc.is_current
GROUP BY dc.customer_id, dc.customer_name
ORDER BY order_count DESC
LIMIT 3;
```

### Row counts (pipeline health check)

```sql
SELECT
    (SELECT COUNT(1) FROM analytics.fact_orders)      AS orders,
    (SELECT COUNT(1) FROM analytics.fact_order_items) AS order_items,
    (SELECT COUNT(1) FROM analytics.dim_customer)     AS customers,
    (SELECT COUNT(1) FROM analytics.dim_product)      AS products;
```

### Product price history (SCD2)

```sql
SELECT product_id, product_name, unity_price, valid_from, valid_to, is_current
FROM analytics.dim_product
WHERE product_id = <id>
ORDER BY valid_from;
```

### Backlog aging (open orders older than N days)

```sql
SELECT
    fo.order_id,
    dc.customer_name,
    dd.full_date                            AS order_date,
    ds.status_code,
    CURRENT_DATE - dd.full_date             AS days_open
FROM analytics.fact_orders fo
JOIN analytics.dim_order_status ds ON ds.status_sk = fo.status_sk
JOIN analytics.dim_date         dd ON dd.date_sk   = fo.order_date_sk
JOIN analytics.dim_customer     dc ON dc.customer_sk = fo.customer_sk
WHERE ds.is_open
  AND dc.is_current
  AND CURRENT_DATE - dd.full_date > 7
ORDER BY days_open DESC;
```

---

## Monitoring Kafka & Debezium

### Kafka UI (`http://localhost:8080`)

```bash
docker compose --profile monitoring up -d kafka-ui
```

Browse topics, connector status, consumer group lag visually.

### Debezium connector status

```bash
curl http://localhost:8083/connectors/finance-db-connector/status
```

### Kafka CLI

```bash
# List topics
docker exec deel-data-engineering-task-kafka-1 \
  kafka-topics --list --bootstrap-server localhost:29092

# Consume orders topic live
docker exec -it deel-data-engineering-task-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic finance_db.operations.orders \
  --from-beginning

# Consumer group lag
docker exec deel-data-engineering-task-kafka-1 kafka-consumer-groups \
  --describe --group analytics-consumer --bootstrap-server localhost:29092
```

### Ingestion logs

```bash
docker logs -f deel-data-engineering-task-ingestion-1
```

### Debugging

```bash
# No messages in Kafka?
docker logs deel-data-engineering-task-kafka-connect-1

# All service logs
docker compose logs -f kafka kafka-connect ingestion
```
