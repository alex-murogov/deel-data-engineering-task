"""CDC consumer: reads Debezium topics → upserts into the analytics star schema."""
import base64
import json
import logging
import os
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS = [
    "finance_db.operations.customers",
    "finance_db.operations.products",
    "finance_db.operations.orders",
    "finance_db.operations.order_items",
]

_EPOCH = date(1970, 1, 1)


def _date_sk(days) -> int | None:
    """Debezium encodes DATE as integer days-since-epoch."""
    if days is None:
        return None
    return int((_EPOCH + timedelta(days=int(days))).strftime("%Y%m%d"))


def _to_dt(value) -> datetime | None:
    """Debezium encodes TIMESTAMP(3) as milliseconds-since-epoch."""
    if value is None:
        return None
    return datetime.utcfromtimestamp(value / 1_000)


def _decimal(value) -> float | None:
    """Decode Debezium VariableScaleDecimal {'scale': N, 'value': '<base64>'} or plain number."""
    if value is None:
        return None
    if isinstance(value, dict):
        scale = value["scale"]
        unscaled = int.from_bytes(base64.b64decode(value["value"]), byteorder="big", signed=True)
        return unscaled / (10 ** scale)
    return float(value)


# ── dim_customer (SCD Type 2) ─────────────────────────────────

def handle_customer(cur, payload: dict, op: str):
    row = payload.get("after") or payload.get("before")
    if not row:
        return

    cid = row["customer_id"]
    name = row["customer_name"]
    addr = row.get("customer_address")
    active = row["is_active"]
    ts = _to_dt(row.get("updated_at")) or datetime.utcnow()

    if op == "d":
        cur.execute(
            "UPDATE analytics.dim_customer SET valid_to=%s, is_current=FALSE "
            "WHERE customer_id=%s AND is_current=TRUE",
            (ts, cid),
        )
        return

    cur.execute(
        "SELECT customer_sk, customer_name, customer_address, is_active "
        "FROM analytics.dim_customer WHERE customer_id=%s AND is_current=TRUE",
        (cid,),
    )
    existing = cur.fetchone()

    if existing is None:
        cur.execute(
            "INSERT INTO analytics.dim_customer "
            "(customer_id, customer_name, customer_address, is_active, valid_from) "
            "VALUES (%s,%s,%s,%s,%s)",
            (cid, name, addr, active, ts),
        )
    else:
        sk, ex_name, ex_addr, ex_active = existing
        if (ex_name, ex_addr, ex_active) != (name, addr, active):
            cur.execute(
                "UPDATE analytics.dim_customer SET valid_to=%s, is_current=FALSE WHERE customer_sk=%s",
                (ts, sk),
            )
            cur.execute(
                "INSERT INTO analytics.dim_customer "
                "(customer_id, customer_name, customer_address, is_active, valid_from) "
                "VALUES (%s,%s,%s,%s,%s)",
                (cid, name, addr, active, ts),
            )


# ── dim_product (SCD Type 2) ──────────────────────────────────

def handle_product(cur, payload: dict, op: str):
    row = payload.get("after") or payload.get("before")
    if not row:
        return

    pid = row["product_id"]
    name = row["product_name"]
    barcode = row["barcode"]
    price = _decimal(row["unity_price"])
    active = row.get("is_active", True)
    ts = _to_dt(row.get("updated_at")) or datetime.utcnow()

    if op == "d":
        cur.execute(
            "UPDATE analytics.dim_product SET valid_to=%s, is_current=FALSE "
            "WHERE product_id=%s AND is_current=TRUE",
            (ts, pid),
        )
        return

    cur.execute(
        "SELECT product_sk, product_name, barcode, unity_price::text, is_active "
        "FROM analytics.dim_product WHERE product_id=%s AND is_current=TRUE",
        (pid,),
    )
    existing = cur.fetchone()

    if existing is None:
        cur.execute(
            "INSERT INTO analytics.dim_product "
            "(product_id, product_name, barcode, unity_price, is_active, valid_from) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (pid, name, barcode, price, active, ts),
        )
    else:
        sk, ex_name, ex_barcode, ex_price, ex_active = existing
        if (ex_name, ex_barcode, round(float(ex_price), 6), ex_active) != (name, barcode, round(price, 6), active):
            cur.execute(
                "UPDATE analytics.dim_product SET valid_to=%s, is_current=FALSE WHERE product_sk=%s",
                (ts, sk),
            )
            cur.execute(
                "INSERT INTO analytics.dim_product "
                "(product_id, product_name, barcode, unity_price, is_active, valid_from) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (pid, name, barcode, price, active, ts),
            )


# ── fact_orders ───────────────────────────────────────────────

def handle_order(cur, payload: dict, op: str):
    row = payload.get("after")
    if not row:
        return

    order_id = row["order_id"]
    ts = _to_dt(row.get("updated_at")) or datetime.utcnow()

    cur.execute(
        "SELECT customer_sk FROM analytics.dim_customer WHERE customer_id=%s AND is_current=TRUE",
        (row["customer_id"],),
    )
    r = cur.fetchone()
    if r is None:
        log.warning("No dim_customer for customer_id=%s — skipping order %s", row["customer_id"], order_id)
        return

    cur.execute(
        "SELECT status_sk FROM analytics.dim_order_status WHERE status_code=%s",
        (row["status"],),
    )
    s = cur.fetchone()
    if s is None:
        log.warning("Unknown status %r — skipping order %s", row["status"], order_id)
        return

    customer_sk = r[0]
    status_sk = s[0]

    cur.execute(
        """
        INSERT INTO analytics.fact_orders
            (order_id, customer_sk, order_date_sk, delivery_date_sk, status_sk, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (order_id) DO UPDATE SET
            customer_sk      = EXCLUDED.customer_sk,
            order_date_sk    = EXCLUDED.order_date_sk,
            delivery_date_sk = EXCLUDED.delivery_date_sk,
            status_sk        = EXCLUDED.status_sk,
            updated_at       = EXCLUDED.updated_at
        """,
        (order_id, customer_sk, _date_sk(row.get("order_date")),
         _date_sk(row.get("delivery_date")), status_sk, ts),
    )
    # cascade status to all items already loaded for this order
    cur.execute(
        "UPDATE analytics.fact_order_items SET status_sk=%s WHERE order_id=%s",
        (status_sk, order_id),
    )


# ── fact_order_items ──────────────────────────────────────────

def handle_order_item(cur, payload: dict, op: str) -> bool:
    """Returns False if the parent order isn't loaded yet (caller should retry)."""
    row = payload.get("after")
    if not row:
        return True

    item_id = row["order_item_id"]
    order_id = row["order_id"]
    product_id = row["product_id"]
    quantity = row.get("quanity")  # source typo preserved
    ts = _to_dt(row.get("updated_at")) or datetime.utcnow()

    cur.execute(
        "SELECT product_sk, unity_price FROM analytics.dim_product "
        "WHERE product_id=%s AND is_current=TRUE",
        (product_id,),
    )
    p = cur.fetchone()
    if p is None:
        return False  # dim_product not yet loaded; caller will retry

    cur.execute(
        "SELECT customer_sk, delivery_date_sk, status_sk FROM analytics.fact_orders WHERE order_id=%s",
        (order_id,),
    )
    o = cur.fetchone()
    if o is None:
        return False  # parent order not yet loaded; caller will retry

    product_sk, unity_price = p
    customer_sk, delivery_date_sk, status_sk = o

    cur.execute(
        """
        INSERT INTO analytics.fact_order_items
            (order_item_id, order_id, product_sk, customer_sk,
             delivery_date_sk, status_sk, quantity, unity_price, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (order_item_id) DO UPDATE SET
            product_sk       = EXCLUDED.product_sk,
            customer_sk      = EXCLUDED.customer_sk,
            delivery_date_sk = EXCLUDED.delivery_date_sk,
            status_sk        = EXCLUDED.status_sk,
            quantity         = EXCLUDED.quantity,
            unity_price      = EXCLUDED.unity_price,
            updated_at       = EXCLUDED.updated_at
        """,
        (item_id, order_id, product_sk, customer_sk,
         delivery_date_sk, status_sk, quantity, unity_price, ts),
    )
    return True


HANDLERS = {
    "finance_db.operations.customers":   handle_customer,
    "finance_db.operations.products":    handle_product,
    "finance_db.operations.orders":      handle_order,
    "finance_db.operations.order_items": handle_order_item,
}


# ── DB connection ─────────────────────────────────────────────

def _connect(retries: int = 30, delay: int = 5) -> psycopg2.extensions.connection:
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=os.environ["ANALYTICS_HOST"],
                port=int(os.environ.get("ANALYTICS_PORT", 5432)),
                dbname=os.environ["ANALYTICS_DB"],
                user=os.environ["ANALYTICS_USER"],
                password=os.environ["ANALYTICS_PASSWORD"],
            )
            conn.autocommit = False
            log.info("Connected to analytics-db")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("analytics-db not ready (%d/%d): %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError("Cannot connect to analytics-db after %d attempts" % retries)


# ── Kafka consumer ────────────────────────────────────────────

def _make_consumer(retries: int = 30, delay: int = 5) -> KafkaConsumer:
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=servers,
                group_id="analytics-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v) if v else None,
                consumer_timeout_ms=1000,
            )
            log.info("Connected to Kafka at %s", servers)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (%d/%d)", attempt, retries)
            time.sleep(delay)
    raise RuntimeError("Cannot connect to Kafka after %d attempts" % retries)


# ── Main loop ─────────────────────────────────────────────────

def main():
    conn = _connect()
    consumer = _make_consumer()

    # Deferred order_items whose parent order wasn't loaded yet
    retry_items: deque[dict] = deque()

    log.info("Starting CDC consume loop")
    while True:
        # Drain one Kafka poll batch
        for msg in consumer:
            if msg.value is None:
                continue
            payload = msg.value
            op = payload.get("op")
            if op not in ("c", "u", "d", "r"):
                continue

            handler = HANDLERS.get(msg.topic)
            if handler is None:
                continue

            try:
                with conn.cursor() as cur:
                    result = handler(cur, payload, op)
                conn.commit()
                if result is False:
                    retry_items.append(payload)
            except Exception:
                conn.rollback()
                log.exception("Error on %s op=%s payload=%s", msg.topic, op, payload)

        # Flush retry buffer (order_items whose orders have since landed)
        pending = list(retry_items)
        retry_items.clear()
        for payload in pending:
            try:
                with conn.cursor() as cur:
                    result = handle_order_item(cur, payload, payload.get("op", "c"))
                conn.commit()
                if result is False:
                    retry_items.append(payload)  # still not ready, defer again
            except Exception:
                conn.rollback()
                log.exception("Retry error for order_item payload=%s", payload)


if __name__ == "__main__":
    main()
