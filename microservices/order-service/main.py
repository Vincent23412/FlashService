import json
import os
import threading
import time

from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import psycopg2

app = FastAPI()

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
DB_URL = os.getenv("DB_URL")
EMAIL_JOB_TOPIC = os.getenv("EMAIL_JOB_TOPIC", "email-job")
USER_CDC_TOPIC = os.getenv("USER_CDC_TOPIC", "user-db-cdc")
CDC_CONSUMER_GROUP = os.getenv("CDC_CONSUMER_GROUP", "order-user-sync")

_producer = None
_producer_lock = threading.Lock()
_stop_event = threading.Event()
_consumer_thread = None


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
                    with psycopg2.connect(DB_URL) as conn:
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


@app.post("/order")
def create_order(order: dict):
    try:
        producer = get_producer()
        order_id = f"order-{int(time.time() * 1000)}"
        event = {
            "event_type": "email.job",
            "order_id": order_id,
            "user_id": order.get("user_id"),
            "items": order.get("items", []),
            "timestamp": time.time(),
            "status": "PENDING",
        }
        producer.send(
            EMAIL_JOB_TOPIC,
            key=order_id,
            value=event,
        ).get(timeout=10)

        return {
            "status": "ok",
            "order_id": order_id,
            "message": "Order accepted, email job published to Kafka",
        }
    except KafkaError as exc:
        raise HTTPException(status_code=502, detail=f"Kafka publish failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Internal error: {exc}")


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
