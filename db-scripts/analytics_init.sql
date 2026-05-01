CREATE SCHEMA IF NOT EXISTS analytics;

-- ─────────────────────────────────────────────
-- Dimension: date (pre-populated 2020-01-01 → 2030-12-31)
-- ─────────────────────────────────────────────
CREATE TABLE analytics.dim_date (
    date_sk       INTEGER      NOT NULL PRIMARY KEY,  -- YYYYMMDD
    full_date     DATE         NOT NULL UNIQUE,
    year          SMALLINT     NOT NULL,
    quarter       SMALLINT     NOT NULL,
    month         SMALLINT     NOT NULL,
    month_name    VARCHAR(12)  NOT NULL,
    week          SMALLINT     NOT NULL,
    day_of_month  SMALLINT     NOT NULL,
    day_of_week   SMALLINT     NOT NULL,  -- 0 = Sunday
    day_name      VARCHAR(12)  NOT NULL,
    is_weekend    BOOLEAN      NOT NULL
);

INSERT INTO analytics.dim_date (
    date_sk, full_date, year, quarter, month, month_name,
    week, day_of_month, day_of_week, day_name, is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(YEAR    FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH   FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Month')),
    EXTRACT(WEEK    FROM d)::SMALLINT,
    EXTRACT(DAY     FROM d)::SMALLINT,
    EXTRACT(DOW     FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Day')),
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d;

-- ─────────────────────────────────────────────
-- Dimension: order_status (lookup)
-- ─────────────────────────────────────────────
CREATE TABLE analytics.dim_order_status (
    status_sk   SMALLSERIAL  NOT NULL PRIMARY KEY,
    status_code VARCHAR(20)  NOT NULL UNIQUE,
    is_open     BOOLEAN      NOT NULL  -- FALSE only for terminal states
);

INSERT INTO analytics.dim_order_status (status_code, is_open) VALUES
    ('PENDING',      TRUE),
    ('PROCESSING',   TRUE),
    ('REPROCESSING', TRUE),
    ('COMPLETED',    FALSE);

-- ─────────────────────────────────────────────
-- Dimension: customer  (SCD Type 2)
-- ─────────────────────────────────────────────
CREATE TABLE analytics.dim_customer (
    customer_sk      BIGSERIAL    NOT NULL PRIMARY KEY,
    customer_id      BIGINT       NOT NULL,
    customer_name    VARCHAR(500) NOT NULL,
    customer_address VARCHAR(500),
    is_active        BOOLEAN      NOT NULL,
    valid_from       TIMESTAMP(3) NOT NULL,
    valid_to         TIMESTAMP(3),           -- NULL = current row
    is_current       BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_customer_bk  ON analytics.dim_customer (customer_id, is_current);
CREATE INDEX idx_dim_customer_cur ON analytics.dim_customer (customer_id) WHERE is_current;

-- ─────────────────────────────────────────────
-- Dimension: product  (SCD Type 2)
-- ─────────────────────────────────────────────
CREATE TABLE analytics.dim_product (
    product_sk   BIGSERIAL    NOT NULL PRIMARY KEY,
    product_id   BIGINT       NOT NULL,
    product_name VARCHAR(500) NOT NULL,
    barcode      VARCHAR(26)  NOT NULL,
    unity_price  DECIMAL      NOT NULL,
    is_active    BOOLEAN,
    valid_from   TIMESTAMP(3) NOT NULL,
    valid_to     TIMESTAMP(3),
    is_current   BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_product_bk  ON analytics.dim_product (product_id, is_current);
CREATE INDEX idx_dim_product_cur ON analytics.dim_product (product_id) WHERE is_current;

-- ─────────────────────────────────────────────
-- Fact: orders  (grain = one order)
-- ─────────────────────────────────────────────
CREATE TABLE analytics.fact_orders (
    order_id         BIGINT   NOT NULL PRIMARY KEY,
    customer_sk      BIGINT   NOT NULL REFERENCES analytics.dim_customer(customer_sk),
    order_date_sk    INTEGER  REFERENCES analytics.dim_date(date_sk),
    delivery_date_sk INTEGER  REFERENCES analytics.dim_date(date_sk),
    status_sk        SMALLINT NOT NULL REFERENCES analytics.dim_order_status(status_sk),
    updated_at       TIMESTAMP(3) NOT NULL
);

CREATE INDEX idx_fact_orders_delivery  ON analytics.fact_orders (delivery_date_sk, status_sk);
CREATE INDEX idx_fact_orders_customer  ON analytics.fact_orders (customer_sk, status_sk);
CREATE INDEX idx_fact_orders_status    ON analytics.fact_orders (status_sk);

-- ─────────────────────────────────────────────
-- Fact: order_items  (grain = one order line)
-- ─────────────────────────────────────────────
CREATE TABLE analytics.fact_order_items (
    order_item_id    BIGINT   NOT NULL PRIMARY KEY,
    order_id         BIGINT   NOT NULL,
    product_sk       BIGINT   NOT NULL REFERENCES analytics.dim_product(product_sk),
    customer_sk      BIGINT   NOT NULL REFERENCES analytics.dim_customer(customer_sk),
    delivery_date_sk INTEGER  REFERENCES analytics.dim_date(date_sk),
    status_sk        SMALLINT NOT NULL REFERENCES analytics.dim_order_status(status_sk),
    quantity         INTEGER,
    unity_price      DECIMAL,
    line_value       DECIMAL GENERATED ALWAYS AS (quantity * unity_price) STORED,
    updated_at       TIMESTAMP(3) NOT NULL
);

CREATE INDEX idx_fact_oi_order    ON analytics.fact_order_items (order_id);
CREATE INDEX idx_fact_oi_product  ON analytics.fact_order_items (product_sk, status_sk);
CREATE INDEX idx_fact_oi_status   ON analytics.fact_order_items (status_sk);
CREATE INDEX idx_fact_oi_delivery ON analytics.fact_order_items (delivery_date_sk, status_sk);
