import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_REQUESTS_TOTAL = Counter(
    "kafka_requests_total",
    "Total number of Kafka messages produced",
    ["method", "endpoint", "status"],
)
KAFKA_REQUEST_ERRORS_TOTAL = Counter(
    "kafka_request_errors_total",
    "Total number of Kafka produce errors",
    ["method", "endpoint", "error_type"],
)
KAFKA_REQUEST_DURATION_SECONDS = Histogram(
    "kafka_request_duration_seconds",
    "Time spent producing a Kafka message",
    ["method", "endpoint"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def start_metrics_server(port):
    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.info("producer metrics server started on port %s", port)


def delivery_report(error, message):
    if error is not None:
        logging.error("delivery failed error=%s", error)
        return
    logging.info("delivered topic=%s partition=%s offset=%s", message.topic(), message.partition(), message.offset())


def build_event(event_type, timestamp, payload):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "payload": json.dumps(payload, sort_keys=True),
    }


def produce_event(producer, topic, serializer, event):
    event_type = event["event_type"]
    payload = json.loads(event["payload"])
    key = payload.get("product_id", event["event_id"])
    method = "produce"
    endpoint = topic
    try:
        with KAFKA_REQUEST_DURATION_SECONDS.labels(method=method, endpoint=endpoint).time():
            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=serializer(event, SerializationContext(topic, MessageField.VALUE)),
                callback=delivery_report,
            )
            producer.poll(0)
        KAFKA_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status="success").inc()
    except Exception as exc:
        KAFKA_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status="error").inc()
        KAFKA_REQUEST_ERRORS_TOTAL.labels(method=method, endpoint=endpoint, error_type="produce_error").inc()
        logging.error("failed to produce event event_type=%s error=%s", event_type, exc)
        raise


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("WAREHOUSE_TOPIC", "warehouse-events")
    metrics_port = int(os.getenv("METRICS_PORT", "8001"))

    start_metrics_server(metrics_port)

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    with open("schemas/warehouse_event.avsc", "r", encoding="utf-8") as schema_file:
        schema = schema_file.read()

    serializer = AvroSerializer(schema_registry_client, schema)
    producer = Producer({"bootstrap.servers": kafka_bootstrap})

    product_id = str(uuid.uuid4())
    zone_a = str(uuid.uuid4())
    zone_b = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    events = [
        build_event("PRODUCT_RECEIVED", started_at, {"product_id": product_id, "zone_id": zone_a, "quantity": 100}),
        build_event("PRODUCT_SHIPPED", started_at + timedelta(minutes=5), {"product_id": product_id, "zone_id": zone_a, "quantity": 20}),
        build_event("PRODUCT_RECEIVED", started_at + timedelta(minutes=2), {"product_id": product_id, "zone_id": zone_a, "quantity": 50}),
        build_event("PRODUCT_MOVED", started_at + timedelta(minutes=10), {"product_id": product_id, "from_zone_id": zone_a, "to_zone_id": zone_b, "quantity": 10}),
        build_event("PRODUCT_RESERVED", started_at + timedelta(minutes=12), {"product_id": product_id, "zone_id": zone_a, "quantity": 5}),
        build_event("PRODUCT_RELEASED", started_at + timedelta(minutes=13), {"product_id": product_id, "zone_id": zone_a, "quantity": 5}),
        build_event("INVENTORY_COUNTED", started_at + timedelta(minutes=14), {"product_id": product_id, "zone_id": zone_a, "counted_quantity": 70}),
        build_event("ORDER_CREATED", started_at + timedelta(minutes=15), {"order_id": order_id, "items": [{"product_id": product_id, "zone_id": zone_a, "quantity": 5}]}),
        build_event("ORDER_COMPLETED", started_at + timedelta(minutes=20), {"order_id": order_id, "items": [{"product_id": product_id, "zone_id": zone_a, "quantity": 5}]}),
        build_event("PRODUCT_RECEIVED", started_at + timedelta(minutes=25), {"product_id": product_id, "zone_id": zone_a, "quantity": -5}),
    ]
    events.append(events[0])

    for event in events:
        produce_event(producer, topic, serializer, event)
        time.sleep(1)

    producer.flush()
    logging.info(
        "wms producer finished product_id=%s zone_a=%s zone_b=%s order_id=%s",
        product_id, zone_a, zone_b, order_id,
    )

    while True:
        time.sleep(60)


if __name__ == "__main__":
    time.sleep(20)
    main()
