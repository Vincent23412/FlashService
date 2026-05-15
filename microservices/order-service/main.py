import json
import os
import threading
import time
from decimal import Decimal
from typing import Optional
from uuid import UUID

import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Response, status
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

app = FastAPI()

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
DB_URL = os.getenv("DB_URL")
EMAIL_JOB_TOPIC = os.getenv("EMAIL_JOB_TOPIC", "email-job")
USER_CDC_TOPIC = os.getenv("USER_CDC_TOPIC", "user-db-cdc")
CDC_CONSUMER_GROUP = os.getenv("CDC_CONSUMER_GROUP", "order-user-sync")
VALID_ORDER_STATUSES = {"PENDING", "CONFIRMED", "CANCELLED"}

_producer = None
_producer_lock = threading.Lock()
_stop_event = threading.Event()
_consumer_thread = None


class OrderItemCreate(BaseModel):
    product_id: UUID
    quantity: int = Field(gt=0)


class OrderCreate(BaseModel):
    user_id: UUID
    items: list[OrderItemCreate] = Field(min_length=1)


class OrderUpdate(BaseModel):
    status: str


def get_db_connection():
    if not DB_URL:
        raise HTTPException(status_code=500, detail="DB_URL is not set")
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


def get_producer():
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=BROKERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
            )
        return _producer


def close_producer():
    global _producer
    with _producer_lock:
        if _producer is not None:
            _producer.flush(timeout=10)
            _producer.close(timeout=10)
            _producer = None


def decimal_to_str(value: Decimal) -> str:
    return format(value, "f")


def serialize_order_row(row: dict) -> dict:
    return {
        "order_id": str(row["order_id"]),
        "user_id": str(row["user_id"]),
        "status": row["status"],
        "created_at": row["created_at"].isoformat(),
    }


def fetch_order_detail(cur, order_id: str) -> Optional[dict]:
    cur.execute(
        """
        SELECT o.order_id, o.user_id, o.status, o.created_at,
               us.username, us.email
        FROM orders o
        LEFT JOIN user_snapshot us ON us.user_id = o.user_id
        WHERE o.order_id = %s
        """,
        (order_id,),
    )
    order_row = cur.fetchone()
    if order_row is None:
        return None

    cur.execute(
        """
        SELECT oi.order_item_id, oi.product_id, p.name AS product_name,
               oi.quantity, oi.price_at_purchase, oi.created_at
        FROM order_items oi
        LEFT JOIN products p ON p.product_id = oi.product_id
        WHERE oi.order_id = %s
        ORDER BY oi.created_at ASC
        """,
        (order_id,),
    )
    item_rows = cur.fetchall()
    items = []
    total_amount = Decimal("0")
    for row in item_rows:
        subtotal = row["price_at_purchase"] * row["quantity"]
        total_amount += subtotal
        items.append(
            {
                "order_item_id": str(row["order_item_id"]),
                "product_id": str(row["product_id"]),
                "product_name": row["product_name"],
                "quantity": row["quantity"],
                "price_at_purchase": decimal_to_str(row["price_at_purchase"]),
                "subtotal": decimal_to_str(subtotal),
                "created_at": row["created_at"].isoformat(),
            }
        )

    return {
        "order_id": str(order_row["order_id"]),
        "user": {
            "user_id": str(order_row["user_id"]),
            "username": order_row["username"],
            "email": order_row["email"],
        },
        "status": order_row["status"],
        "created_at": order_row["created_at"].isoformat(),
        "items": items,
        "summary": {
            "item_count": len(items),
            "total_quantity": sum(item["quantity"] for item in items),
            "total_amount": decimal_to_str(total_amount),
        },
    }


def aggregate_requested_items(items: list[OrderItemCreate]) -> list[dict]:
    merged = {}
    for item in items:
        product_id = str(item.product_id)
        merged[product_id] = merged.get(product_id, 0) + item.quantity
    return [
        {"product_id": product_id, "quantity": quantity}
        for product_id, quantity in sorted(merged.items(), key=lambda entry: entry[0])
    ]


def publish_email_job(order_detail: dict):
    event = {
        "event_type": "email.job",
        "order_id": order_detail["order_id"],
        "status": order_detail["status"],
        "created_at": order_detail["created_at"],
        "recipient": order_detail["user"],
        "order": {
            "items": order_detail["items"],
            "summary": order_detail["summary"],
        },
    }
    producer = get_producer()
    producer.send(EMAIL_JOB_TOPIC, key=order_detail["order_id"], value=event).get(timeout=10)


def consume_user_cdc_loop():
    while not _stop_event.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                USER_CDC_TOPIC,
                bootstrap_servers=BROKERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=CDC_CONSUMER_GROUP,
                value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
            )

            print(f"[order-service] CDC consumer joined group: {CDC_CONSUMER_GROUP}", flush=True)

            for msg in consumer:
                if _stop_event.is_set():
                    break

                payload = json.loads(msg.value)
                after = payload.get("payload", {}).get("after")
                if after and DB_URL:
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPSERT INTO user_snapshot (
                                    user_id,
                                    username,
                                    email,
                                    source_updated_at,
                                    synced_at
                                ) VALUES (%s, %s, %s, now(), now())
                                """,
                                (
                                    after.get("user_id"),
                                    after.get("username"),
                                    after.get("email"),
                                ),
                            )
                    print(
                        f"[order-service] synced user_snapshot for user_id={after.get('user_id')}",
                        flush=True,
                    )
        except Exception as exc:
            if _stop_event.is_set():
                break
            print(f"[order-service] CDC consumer error: {exc}; retrying in 3s...", flush=True)
            time.sleep(3)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


@app.on_event("startup")
def start_cdc_consumer():
    global _consumer_thread
    _stop_event.clear()
    _consumer_thread = threading.Thread(target=consume_user_cdc_loop, daemon=True)
    _consumer_thread.start()


@app.on_event("shutdown")
def shutdown_background_workers():
    _stop_event.set()
    if _consumer_thread is not None:
        _consumer_thread.join(timeout=10)
    close_producer()


@app.post("/orders", status_code=status.HTTP_201_CREATED)
def create_order(payload: OrderCreate):
    requested_items = aggregate_requested_items(payload.items)
    product_ids = [item["product_id"] for item in requested_items]
    product_placeholders = ", ".join(["%s"] * len(product_ids))

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, username, email
                    FROM user_snapshot
                    WHERE user_id = %s
                    """,
                    (str(payload.user_id),),
                )
                user_row = cur.fetchone()
                if user_row is None:
                    raise HTTPException(status_code=404, detail="user snapshot not found")

                cur.execute(
                    f"""
                    SELECT p.product_id, p.name, p.price, i.stock
                    FROM products p
                    JOIN inventory i ON i.product_id = p.product_id
                    WHERE p.product_id IN ({product_placeholders})
                    """,
                    product_ids,
                )
                product_rows = cur.fetchall()
                products = {str(row["product_id"]): row for row in product_rows}

                missing_product_ids = [pid for pid in product_ids if pid not in products]
                if missing_product_ids:
                    raise HTTPException(
                        status_code=404,
                        detail={"message": "products not found", "product_ids": missing_product_ids},
                    )

                cur.execute(
                    """
                    INSERT INTO orders (user_id, status)
                    VALUES (%s, 'PENDING')
                    RETURNING order_id, user_id, status, created_at
                    """,
                    (str(payload.user_id),),
                )
                order_row = cur.fetchone()
                order_id = str(order_row["order_id"])

                for item in requested_items:
                    product = products[item["product_id"]]
                    cur.execute(
                        """
                        UPDATE inventory
                        SET stock = stock - %s, updated_at = now()
                        WHERE product_id = %s AND stock >= %s
                        RETURNING stock
                        """,
                        (item["quantity"], item["product_id"], item["quantity"]),
                    )
                    reserved_inventory = cur.fetchone()
                    if reserved_inventory is None:
                        cur.execute(
                            """
                            SELECT stock
                            FROM inventory
                            WHERE product_id = %s
                            """,
                            (item["product_id"],),
                        )
                        inventory_row = cur.fetchone()
                        if inventory_row is None:
                            raise HTTPException(
                                status_code=404,
                                detail={
                                    "message": "inventory not found",
                                    "product_id": item["product_id"],
                                },
                            )
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "message": "insufficient inventory",
                                "product_id": item["product_id"],
                                "available_stock": inventory_row["stock"],
                                "requested_quantity": item["quantity"],
                            },
                        )

                    cur.execute(
                        """
                        INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            order_id,
                            item["product_id"],
                            item["quantity"],
                            product["price"],
                        ),
                    )

                order_detail = fetch_order_detail(cur, order_id)

        publish_email_job(order_detail)
    except HTTPException:
        raise
    except KafkaError as exc:
        raise HTTPException(status_code=502, detail=f"Kafka publish failed: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create order failed: {exc}") from exc

    return {
        "status": "accepted",
        "message": "Order persisted and email job queued",
        "order": order_detail,
    }


@app.get("/orders")
def list_orders(user_id: Optional[UUID] = None, status_filter: Optional[str] = None, limit: int = 100, offset: int = 0):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0")

    conditions = []
    values = []
    if user_id is not None:
        conditions.append("o.user_id = %s")
        values.append(str(user_id))
    if status_filter is not None:
        normalized_status = status_filter.upper()
        if normalized_status not in VALID_ORDER_STATUSES:
            raise HTTPException(status_code=400, detail="invalid status filter")
        conditions.append("o.status = %s")
        values.append(normalized_status)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    values.extend([limit, offset])

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT o.order_id, o.user_id, o.status, o.created_at,
                           us.username, us.email
                    FROM orders o
                    LEFT JOIN user_snapshot us ON us.user_id = o.user_id
                    {where_clause}
                    ORDER BY o.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    values,
                )
                rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"List orders failed: {exc}") from exc

    items = [
        {
            "order_id": str(row["order_id"]),
            "user": {
                "user_id": str(row["user_id"]),
                "username": row["username"],
                "email": row["email"],
            },
            "status": row["status"],
            "created_at": row["created_at"].isoformat(),
        }
        for row in rows
    ]
    return {"items": items, "limit": limit, "offset": offset}


@app.get("/orders/{order_id}")
def get_order(order_id: UUID):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                order_detail = fetch_order_detail(cur, str(order_id))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Get order failed: {exc}") from exc

    if order_detail is None:
        raise HTTPException(status_code=404, detail="order not found")

    return order_detail


@app.patch("/orders/{order_id}")
def update_order(order_id: UUID, payload: OrderUpdate):
    normalized_status = payload.status.upper()
    if normalized_status not in VALID_ORDER_STATUSES:
        raise HTTPException(status_code=400, detail="invalid status")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE orders
                    SET status = %s
                    WHERE order_id = %s
                    RETURNING order_id
                    """,
                    (normalized_status, str(order_id)),
                )
                updated = cur.fetchone()
                if updated is None:
                    raise HTTPException(status_code=404, detail="order not found")
                order_detail = fetch_order_detail(cur, str(order_id))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update order failed: {exc}") from exc

    return order_detail


@app.delete("/orders/{order_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_order(order_id: UUID):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT product_id, quantity
                    FROM order_items
                    WHERE order_id = %s
                    """,
                    (str(order_id),),
                )
                item_rows = cur.fetchall()
                if not item_rows:
                    cur.execute("SELECT 1 FROM orders WHERE order_id = %s", (str(order_id),))
                    if cur.fetchone() is None:
                        raise HTTPException(status_code=404, detail="order not found")

                for row in item_rows:
                    cur.execute(
                        """
                        UPDATE inventory
                        SET stock = stock + %s, updated_at = now()
                        WHERE product_id = %s
                        """,
                        (row["quantity"], str(row["product_id"])),
                    )

                cur.execute("DELETE FROM orders WHERE order_id = %s", (str(order_id),))
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="order not found")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete order failed: {exc}") from exc

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "order-service",
        "email_job_topic": EMAIL_JOB_TOPIC,
        "user_cdc_topic": USER_CDC_TOPIC,
        "cdc_consumer_group": CDC_CONSUMER_GROUP,
    }


@app.get("/kafka/health")
def kafka_health():
    try:
        producer = get_producer()
        producer._sender._client._maybe_refresh_metadata()
        return {"status": "ok", "service": "order-service", "kafka": "connected"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka check failed: {exc}")
