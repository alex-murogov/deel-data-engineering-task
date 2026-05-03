"""ACME Delivery Analytics API."""
import os
from contextlib import asynccontextmanager
from typing import Annotated

import asyncpg
from fastapi import FastAPI, Query, HTTPException
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

_VALID_STATUSES = {"PENDING", "PROCESSING", "REPROCESSING", "COMPLETED"}


def _status_clause(status: str) -> tuple[str, list]:
    """Return (WHERE fragment, bind_args) for a status filter.

    Accepts 'open', 'closed', or an exact status code.
    """
    s = status.upper()
    if s == "OPEN":
        return "ds.is_open", []
    if s == "CLOSED":
        return "NOT ds.is_open", []
    if s in _VALID_STATUSES:
        return "ds.status_code = $__PH__", [status.upper()]
    raise HTTPException(
        status_code=422,
        detail=f"Invalid status '{status}'. Use 'open', 'closed', or one of: {sorted(_VALID_STATUSES)}",
    )


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
async def orders_by_date_status(
    status: Annotated[str, Query(description="'open', 'closed', or a status code")] = "open",
):
    """Orders grouped by delivery date and status."""
    clause, args = _status_clause(status)
    async with _pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT
                dd.full_date::text  AS delivery_date,
                ds.status_code      AS status,
                COUNT(1)::int       AS order_count
            FROM analytics.fact_orders fo
            JOIN analytics.dim_order_status ds ON ds.status_sk = fo.status_sk
            JOIN analytics.dim_date         dd ON dd.date_sk   = fo.delivery_date_sk
            WHERE {clause.replace('$__PH__', '$1')}
            GROUP BY dd.full_date, ds.status_code
            ORDER BY dd.full_date, ds.status_code
        """, *args)
    return [dict(r) for r in rows]


@app.get("/analytics/orders/top", response_model=list[TopDeliveryDate])
async def top_delivery_dates(
    limit: Annotated[int, Query(ge=1, le=100)] = 3,
    status: Annotated[str, Query(description="'open', 'closed', or a status code")] = "open",
):
    """Top N delivery dates by order count."""
    clause, args = _status_clause(status)
    limit_ph = f"${len(args) + 1}"
    async with _pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT
                dd.full_date::text AS delivery_date,
                COUNT(1)::int      AS order_count
            FROM analytics.fact_orders fo
            JOIN analytics.dim_order_status ds ON ds.status_sk = fo.status_sk
            JOIN analytics.dim_date         dd ON dd.date_sk   = fo.delivery_date_sk
            WHERE {clause.replace('$__PH__', '$1')}
            GROUP BY dd.full_date
            ORDER BY order_count DESC
            LIMIT {limit_ph}
        """, *args, limit)
    return [dict(r) for r in rows]


@app.get("/analytics/orders/product", response_model=list[ProductPendingItems])
async def pending_items_by_product(
    status: Annotated[str, Query(description="'open', 'closed', or a status code")] = "open",
):
    """Item quantities grouped by product."""
    clause, args = _status_clause(status)
    async with _pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT
                dp.product_id                      AS product_id,
                dp.product_name                    AS product_name,
                COALESCE(SUM(foi.quantity), 0)::int AS pending_quantity,
                COUNT(1)::int                      AS item_count
            FROM analytics.fact_order_items foi
            JOIN analytics.dim_order_status ds ON ds.status_sk  = foi.status_sk
            JOIN analytics.dim_product      dp ON dp.product_sk = foi.product_sk
            WHERE {clause.replace('$__PH__', '$1')}
              AND dp.is_current
            GROUP BY dp.product_id, dp.product_name
            ORDER BY pending_quantity DESC
        """, *args)
    return [dict(r) for r in rows]


@app.get("/analytics/orders/customers", response_model=list[TopCustomer])
async def top_customers(
    limit: Annotated[int, Query(ge=1, le=100)] = 3,
    status: Annotated[str, Query(description="'open', 'closed', or a status code")] = "open",
):
    """Top N customers ranked by order count."""
    clause, args = _status_clause(status)
    limit_ph = f"${len(args) + 1}"
    async with _pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT
                dc.customer_id                    AS customer_id,
                dc.customer_name                  AS customer_name,
                COUNT(DISTINCT fo.order_id)::int  AS order_count
            FROM analytics.fact_orders fo
            JOIN analytics.dim_customer     dc ON dc.customer_sk = fo.customer_sk
            JOIN analytics.dim_order_status ds ON ds.status_sk   = fo.status_sk
            WHERE {clause.replace('$__PH__', '$1')}
              AND dc.is_current
            GROUP BY dc.customer_id, dc.customer_name
            ORDER BY order_count DESC
            LIMIT {limit_ph}
        """, *args, limit)
    return [dict(r) for r in rows]
