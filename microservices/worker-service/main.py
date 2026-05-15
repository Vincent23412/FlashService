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
                    order_id = payload.get("order_id", "unknown")
                    user_id = payload.get("user_id", "unknown")
                    print(
                        f"[worker] Email job processed for user={user_id} order={order_id}",
                        flush=True,
                    )
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
