import json
import os
import threading
import time

from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer

app = FastAPI()

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
TOPIC = os.getenv("KAFKA_TOPIC", "email-job")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "failed-dlq")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "worker-service")

stop_event = threading.Event()
consumer_thread = None
producer = None
producer_lock = threading.Lock()


def get_producer():
    global producer
    with producer_lock:
        if producer is None:
            producer = KafkaProducer(
                bootstrap_servers=BROKERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
        return producer


def send_to_dlq(payload, error_message):
    event = {
        "payload": payload,
        "error": error_message,
        "failed_at": time.time(),
    }
    get_producer().send(DLQ_TOPIC, key=str(payload.get("order_id", "unknown")), value=event).get(timeout=10)


def format_email_job_log(payload):
    recipient = payload.get("recipient") or {}
    order = payload.get("order") or {}
    items = order.get("items") or []
    summary = order.get("summary") or {}

    item_lines = []
    for index, item in enumerate(items, start=1):
        item_lines.append(
            "  "
            f"{index}. {item.get('product_name', 'unknown-product')} "
            f"(product_id={item.get('product_id', 'unknown')}, "
            f"qty={item.get('quantity', 0)}, "
            f"unit_price={item.get('price_at_purchase', '0')}, "
            f"subtotal={item.get('subtotal', '0')})"
        )

    if not item_lines:
        item_lines.append("  (no items)")

    lines = [
        "[worker] Async email job received",
        f"  order_id={payload.get('order_id', 'unknown')}",
        f"  order_status={payload.get('status', 'unknown')}",
        f"  created_at={payload.get('created_at', 'unknown')}",
        "  recipient:",
        f"    user_id={recipient.get('user_id', 'unknown')}",
        f"    username={recipient.get('username', 'unknown')}",
        f"    email={recipient.get('email', 'unknown')}",
        "  order_items:",
        *item_lines,
        "  summary:",
        f"    item_count={summary.get('item_count', 0)}",
        f"    total_quantity={summary.get('total_quantity', 0)}",
        f"    total_amount={summary.get('total_amount', '0')}",
    ]
    return "\n".join(lines)


def consume_loop():
    while not stop_event.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BROKERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=300000,
            )

            print(f"[worker] Consumer joined group: {CONSUMER_GROUP}", flush=True)

            for msg in consumer:
                if stop_event.is_set():
                    break

                try:
                    payload = json.loads(msg.value)
                    print(format_email_job_log(payload), flush=True)
                except Exception as exc:
                    print(f"[worker] Processing error: {exc}", flush=True)
                    try:
                        decoded = json.loads(msg.value)
                    except Exception:
                        decoded = {"raw_value": msg.value}
                    send_to_dlq(decoded, str(exc))

            consumer.close()
        except Exception as exc:
            if stop_event.is_set():
                print("[worker] Shutdown signal received, exiting loop.", flush=True)
                break
            print(f"[worker] Consumer error: {exc}; retrying in 3s...", flush=True)
            time.sleep(3)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


@app.on_event("startup")
def start_consumer():
    global consumer_thread
    stop_event.clear()
    consumer_thread = threading.Thread(target=consume_loop, daemon=True)
    consumer_thread.start()


@app.on_event("shutdown")
def shutdown_consumer():
    global producer
    print("[worker] Shutdown requested, closing consumer...", flush=True)
    stop_event.set()
    if consumer_thread is not None:
        consumer_thread.join(timeout=10)
    with producer_lock:
        if producer is not None:
            producer.flush(timeout=10)
            producer.close(timeout=10)
            producer = None
    print("[worker] Consumer shutdown complete.", flush=True)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "worker-service",
        "topic": TOPIC,
        "consumer_group": CONSUMER_GROUP,
        "dlq_topic": DLQ_TOPIC,
    }
