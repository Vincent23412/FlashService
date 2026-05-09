import json
import os
import threading
import time

from fastapi import FastAPI
from kafka import KafkaConsumer

app = FastAPI()

BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
TOPIC = os.getenv("KAFKA_TOPIC", "notification")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "notification-service")

stop_event = threading.Event()
consumer_thread = None


def consume_loop():
    while not stop_event.is_set():
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

            print(f"[notification] Consumer joined group: {CONSUMER_GROUP}", flush=True)

            for msg in consumer:
                if stop_event.is_set():
                    break
                try:
                    payload = json.loads(msg.value)
                    order_id = payload.get("order_id", "unknown")
                    user_id = payload.get("user_id", "unknown")
                    event_type = payload.get("event_type", "unknown")
                    print(
                        f"[Notification] To: {user_id}, Order: {order_id}, "
                        f"Event: {event_type}, Msg: Order confirmed (simulated).",
                        flush=True,
                    )
                except json.JSONDecodeError:
                    print(f"[notification] Non-JSON message: {msg.value}", flush=True)
                except Exception as exc:
                    print(f"[notification] Processing error: {exc}", flush=True)

            consumer.close()
        except Exception as exc:
            if stop_event.is_set():
                print("[notification] Shutdown signal received, exiting loop.", flush=True)
                break
            print(f"[notification] Consumer error: {exc}; retrying in 3s...", flush=True)
            time.sleep(3)
        finally:
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
    print("[notification] Shutdown requested, closing consumer...", flush=True)
    stop_event.set()
    if consumer_thread is not None:
        consumer_thread.join(timeout=10)
    print("[notification] Consumer shutdown complete.", flush=True)


@app.get("/health")
def health():
    return {"status": "ok", "service": "notification-service", "consumer_group": CONSUMER_GROUP}
