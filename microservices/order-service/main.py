import json
import os
import threading
import time

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError

app = FastAPI()

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
ORDER_CREATED_TOPIC = "order.created"

_producer = None
_producer_lock = threading.Lock()


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


@app.post("/order")
def create_order(order: dict):
    try:
        producer = get_producer()
        order_id = f"order-{int(time.time() * 1000)}"
        event = {
            "event_type": "order.created",
            "order_id": order_id,
            "user_id": order.get("user_id"),
            "items": order.get("items", []),
            "timestamp": time.time(),
            "status": "PENDING",
        }
        producer.send(
            ORDER_CREATED_TOPIC,
            key=order_id,
            value=event,
        ).get(timeout=10)

        return {
            "status": "ok",
            "order_id": order_id,
            "message": "Order accepted, event published to Kafka",
        }
    except KafkaError as exc:
        raise HTTPException(status_code=502, detail=f"Kafka publish failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Internal error: {exc}")


@app.get("/health")
def health():
    return {"status": "ok", "service": "order-service"}


@app.get("/kafka/health")
def kafka_health():
    try:
        producer = get_producer()
        producer._sender._client._maybe_refresh_metadata()
        return {"status": "ok", "service": "order-service", "kafka": "connected"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka check failed: {exc}")
