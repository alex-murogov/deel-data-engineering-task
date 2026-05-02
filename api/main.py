"""ACME Delivery Analytics API."""
import os
from contextlib import asynccontextmanager
from typing import Annotated

import asyncpg
from fastapi import FastAPI, Query
from pydantic import BaseModel


# ── Connection pool ───────────────────────────────────────────

_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = await asyncpg.create_pool(
        host=os.environ["ANALYTICS_HOST"],
        port=int(os.environ.get("ANALYTICS_PORT", 5432)),
        database=os.environ["ANALYTICS_DB"],
        user=os.environ["ANALYTICS_USER"],
        password=os.environ["ANALYTICS_PASSWORD"],
        min_size=2,
        max_size=10,
    )
    yield
    await _pool.close()


app = FastAPI(title="ACME Delivery Analytics", lifespan=lifespan)


# ── Response models ───────────────────────────────────────────

class OrdersByDateStatus(BaseModel):
    delivery_date: str
    status: str
    order_count: int


class TopDeliveryDate(BaseModel):
    delivery_date: str
    order_count: int


class ProductPendingItems(BaseModel):
    product_id: int
    product_name: str
    pending_quantity: int
    item_count: int


class TopCustomer(BaseModel):
    customer_id: int
    customer_name: str
    order_count: int


# ── Endpoints ─────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/analytics/orders", response_model=list[OrdersByDateStatus])
async def open_orders_by_date_status():
    """Open orders grouped by delivery date and status."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                dd.full_date::text  AS delivery_date,
                ds.status_code      AS status,
                COUNT(*)::int       AS order_count
            FROM analytics.fact_orders fo
            JOIN analytics.dim_order_status ds ON ds.status_sk = fo.status_sk
            JOIN analytics.dim_date         dd ON dd.date_sk   = fo.delivery_date_sk
            WHERE ds.is_open = TRUE
            GROUP BY dd.full_date, ds.status_code
            ORDER BY dd.full_date, ds.status_code
        """)
    return [dict(r) for r in rows]


@app.get("/analytics/orders/top", response_model=list[TopDeliveryDate])
async def top_delivery_dates(
    limit: Annotated[int, Query(ge=1, le=100)] = 3,
):
    """Top N delivery dates by open order count."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                dd.full_date::text AS delivery_date,
                COUNT(*)::int      AS order_count
            FROM analytics.fact_orders fo
            JOIN analytics.dim_order_status ds ON ds.status_sk = fo.status_sk
            JOIN analytics.dim_date         dd ON dd.date_sk   = fo.delivery_date_sk
            WHERE ds.is_open = TRUE
            GROUP BY dd.full_date
            ORDER BY order_count DESC
            LIMIT $1
        """, limit)
    return [dict(r) for r in rows]


@app.get("/analytics/orders/product", response_model=list[ProductPendingItems])
async def open_pending_items_by_product():
    """Pending item quantities grouped by product, across all open orders."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                dp.product_id                      AS product_id,
                dp.product_name                    AS product_name,
                COALESCE(SUM(foi.quantity), 0)::int AS pending_quantity,
                COUNT(*)::int                      AS item_count
            FROM analytics.fact_order_items foi
            JOIN analytics.dim_order_status ds ON ds.status_sk  = foi.status_sk
            JOIN analytics.dim_product      dp ON dp.product_sk = foi.product_sk
            WHERE ds.is_open    = TRUE
              AND dp.is_current = TRUE
            GROUP BY dp.product_id, dp.product_name
            ORDER BY pending_quantity DESC
        """)
    return [dict(r) for r in rows]


@app.get("/analytics/orders/customers", response_model=list[TopCustomer])
async def top_customers_by_open_orders(
    limit: Annotated[int, Query(ge=1, le=100)] = 3,
):
    """Top N customers ranked by number of open orders."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                dc.customer_id                    AS customer_id,
                dc.customer_name                  AS customer_name,
                COUNT(DISTINCT fo.order_id)::int  AS order_count
            FROM analytics.fact_orders fo
            JOIN analytics.dim_customer     dc ON dc.customer_sk = fo.customer_sk
            JOIN analytics.dim_order_status ds ON ds.status_sk   = fo.status_sk
            WHERE ds.is_open    = TRUE
              AND dc.is_current = TRUE
            GROUP BY dc.customer_id, dc.customer_name
            ORDER BY order_count DESC
            LIMIT $1
        """, limit)
    return [dict(r) for r in rows]
