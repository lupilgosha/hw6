import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlparse

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import BatchStatement, SimpleStatement
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_consumer_instance = None


class ConsumerHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
        elif parsed.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        elif parsed.path == "/inventory":
            qs = parse_qs(parsed.query)
            product_id = qs.get("product_id", [None])[0]
            zone_id = qs.get("zone_id", [None])[0]
            if not product_id or not zone_id:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"product_id and zone_id are required"}')
                return
            try:
                row = _consumer_instance.session.execute(
                    "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone WHERE product_id = %s AND zone_id = %s",
                    (uuid.UUID(product_id), uuid.UUID(zone_id)),
                ).one()
                if row is None:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"error":"not found"}')
                    return
                body = json.dumps({
                    "product_id": product_id,
                    "zone_id": zone_id,
                    "available_quantity": row.available_quantity,
                    "reserved_quantity": row.reserved_quantity,
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(exc)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


KAFKA_REQUESTS_TOTAL = Counter(
    "kafka_requests_total",
    "Total number of Kafka messages processed",
    ["method", "endpoint", "status"],
)
KAFKA_REQUEST_ERRORS_TOTAL = Counter(
    "kafka_request_errors_total",
    "Total number of Kafka processing errors",
    ["method", "endpoint", "error_type"],
)
KAFKA_REQUEST_DURATION_SECONDS = Histogram(
    "kafka_request_duration_seconds",
    "Time spent processing a Kafka message",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


class EventValidationError(Exception):
    pass


class UnsupportedEventError(Exception):
    pass


class WarehouseConsumer:
    def __init__(self):
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        cassandra_hosts = os.getenv("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
        cassandra_port = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.topic = os.getenv("WAREHOUSE_TOPIC", "warehouse-events")
        self.dlq_topic = os.getenv("WAREHOUSE_DLQ_TOPIC", "warehouse-events-dlq")
        self.group_id = os.getenv("WAREHOUSE_CONSUMER_GROUP", "warehouse-state-consumer")

        self.consumer = Consumer(
            {
                "bootstrap.servers": kafka_bootstrap,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.consumer.subscribe([self.topic])
        self.dlq_producer = Producer({"bootstrap.servers": kafka_bootstrap})

        self.cluster = self._connect_to_cassandra(cassandra_hosts, cassandra_port)
        self.session = self.cluster.connect()
        self._init_db()

    def _connect_to_cassandra(self, hosts, port):
        while True:
            try:
                cluster = Cluster(contact_points=hosts, port=port)
                cluster.connect().shutdown()
                return cluster
            except NoHostAvailable:
                logging.info("waiting for cassandra to become available")
                time.sleep(5)

    def _init_db(self):
        self.session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS warehouse
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        self.session.set_keyspace("warehouse")

        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
                product_id uuid,
                zone_id uuid,
                available_quantity int,
                reserved_quantity int,
                last_event_timestamp timestamp,
                PRIMARY KEY ((product_id, zone_id))
            )
            """
        )
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory_by_product (
                product_id uuid,
                zone_id uuid,
                available_quantity int,
                reserved_quantity int,
                last_event_timestamp timestamp,
                PRIMARY KEY ((product_id), zone_id)
            )
            """
        )
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory_by_zone (
                zone_id uuid,
                product_id uuid,
                available_quantity int,
                reserved_quantity int,
                last_event_timestamp timestamp,
                PRIMARY KEY ((zone_id), product_id)
            )
            """
        )
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id uuid PRIMARY KEY,
                processed_at timestamp,
                event_type text,
                partition int,
                offset bigint
            )
            """
        )
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id uuid PRIMARY KEY,
                status text,
                last_event_timestamp timestamp
            )
            """
        )
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS event_history (
                product_id uuid,
                event_timestamp timestamp,
                event_id uuid,
                event_type text,
                payload text,
                PRIMARY KEY ((product_id), event_timestamp, event_id)
            ) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC)
            """
        )

    def _parse_uuid(self, value, field_name):
        try:
            return uuid.UUID(value)
        except Exception as error:
            raise EventValidationError(f"invalid {field_name}: {value}") from error

    def _parse_timestamp(self, value):
        if not value:
            raise EventValidationError("timestamp is required")
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as error:
            raise EventValidationError(f"invalid timestamp: {value}") from error
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)

    def _validate_positive_quantity(self, quantity, field_name="quantity"):
        if not isinstance(quantity, int) or quantity <= 0:
            raise EventValidationError(f"invalid {field_name}: {quantity} (must be positive integer)")

    def _is_event_processed(self, event_id):
        row = self.session.execute(
            "SELECT event_id FROM processed_events WHERE event_id = %s", (event_id,)
        ).one()
        return row is not None

    def _load_inventory_state(self, product_id, zone_id):
        row = self.session.execute(
            """
            SELECT available_quantity, reserved_quantity, last_event_timestamp
            FROM inventory_by_product_zone
            WHERE product_id = %s AND zone_id = %s
            """,
            (product_id, zone_id),
        ).one()
        if row is None:
            return {"available_quantity": 0, "reserved_quantity": 0, "last_event_timestamp": None}
        return {
            "available_quantity": row.available_quantity,
            "reserved_quantity": row.reserved_quantity,
            "last_event_timestamp": row.last_event_timestamp,
        }

    def _ensure_newer_event(self, current_timestamp, incoming_timestamp, product_id, zone_id):
        if current_timestamp is not None and incoming_timestamp <= current_timestamp:
            logging.info(
                "ignored stale event product_id=%s zone_id=%s incoming_timestamp=%s current_timestamp=%s",
                product_id,
                zone_id,
                incoming_timestamp.isoformat(),
                current_timestamp.isoformat(),
            )
            return False
        return True

    def _queue_inventory_snapshot(self, batch, product_id, zone_id, available_quantity, reserved_quantity, event_timestamp):
        batch.add(
            SimpleStatement(
                """
                UPDATE inventory_by_product_zone
                SET available_quantity = %s, reserved_quantity = %s, last_event_timestamp = %s
                WHERE product_id = %s AND zone_id = %s
                """
            ),
            (available_quantity, reserved_quantity, event_timestamp, product_id, zone_id),
        )
        batch.add(
            SimpleStatement(
                """
                UPDATE inventory_by_product
                SET available_quantity = %s, reserved_quantity = %s, last_event_timestamp = %s
                WHERE product_id = %s AND zone_id = %s
                """
            ),
            (available_quantity, reserved_quantity, event_timestamp, product_id, zone_id),
        )
        batch.add(
            SimpleStatement(
                """
                UPDATE inventory_by_zone
                SET available_quantity = %s, reserved_quantity = %s, last_event_timestamp = %s
                WHERE zone_id = %s AND product_id = %s
                """
            ),
            (available_quantity, reserved_quantity, event_timestamp, zone_id, product_id),
        )

    def _apply_inventory_delta(self, batch, product_id, zone_id, event_timestamp, available_delta=0, reserved_delta=0, absolute_available=None):
        current = self._load_inventory_state(product_id, zone_id)
        if not self._ensure_newer_event(current["last_event_timestamp"], event_timestamp, product_id, zone_id):
            return False

        available_quantity = current["available_quantity"]
        reserved_quantity = current["reserved_quantity"]

        if absolute_available is not None:
            available_quantity = absolute_available
        else:
            available_quantity += available_delta
        reserved_quantity += reserved_delta

        if available_quantity < 0:
            raise EventValidationError(
                f"negative available quantity for product_id={product_id} zone_id={zone_id}"
            )
        if reserved_quantity < 0:
            raise EventValidationError(
                f"negative reserved quantity for product_id={product_id} zone_id={zone_id}"
            )

        self._queue_inventory_snapshot(
            batch,
            product_id,
            zone_id,
            available_quantity,
            reserved_quantity,
            event_timestamp,
        )
        return True

    def _append_event_history(self, batch, event_type, event_timestamp, event_id, payload):
        product_ids = []
        if "product_id" in payload:
            product_ids.append(self._parse_uuid(payload["product_id"], "product_id"))
        for item in payload.get("items", []):
            product_ids.append(self._parse_uuid(item["product_id"], "product_id"))
        unique_product_ids = list(dict.fromkeys(product_ids))
        for product_id in unique_product_ids:
            batch.add(
                SimpleStatement(
                    """
                    INSERT INTO event_history (product_id, event_timestamp, event_id, event_type, payload)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                ),
                (product_id, event_timestamp, event_id, event_type, json.dumps(payload, sort_keys=True)),
            )

    def _build_batch(self, event, partition, offset):
        event_id = self._parse_uuid(event.get("event_id"), "event_id")
        event_type = event.get("event_type")
        payload = event.get("payload")
        event_timestamp = self._parse_timestamp(event.get("timestamp"))

        if not event_type:
            raise EventValidationError("event_type is required")
        if not isinstance(payload, dict):
            raise EventValidationError("payload must be an object")

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        mutated = False

        if event_type == "PRODUCT_RECEIVED":
            quantity = payload.get("quantity")
            self._validate_positive_quantity(quantity)
            mutated = self._apply_inventory_delta(
                batch,
                self._parse_uuid(payload.get("product_id"), "product_id"),
                self._parse_uuid(payload.get("zone_id"), "zone_id"),
                event_timestamp,
                available_delta=quantity,
            )
        elif event_type == "PRODUCT_SHIPPED":
            quantity = payload.get("quantity")
            self._validate_positive_quantity(quantity)
            mutated = self._apply_inventory_delta(
                batch,
                self._parse_uuid(payload.get("product_id"), "product_id"),
                self._parse_uuid(payload.get("zone_id"), "zone_id"),
                event_timestamp,
                available_delta=-quantity,
            )
        elif event_type == "PRODUCT_MOVED":
            quantity = payload.get("quantity")
            self._validate_positive_quantity(quantity)
            product_id = self._parse_uuid(payload.get("product_id"), "product_id")
            from_zone_id = self._parse_uuid(payload.get("from_zone_id"), "from_zone_id")
            to_zone_id = self._parse_uuid(payload.get("to_zone_id"), "to_zone_id")
            left = self._apply_inventory_delta(
                batch,
                product_id,
                from_zone_id,
                event_timestamp,
                available_delta=-quantity,
            )
            right = self._apply_inventory_delta(
                batch,
                product_id,
                to_zone_id,
                event_timestamp,
                available_delta=quantity,
            )
            mutated = left or right
        elif event_type == "PRODUCT_RESERVED":
            quantity = payload.get("quantity")
            self._validate_positive_quantity(quantity)
            mutated = self._apply_inventory_delta(
                batch,
                self._parse_uuid(payload.get("product_id"), "product_id"),
                self._parse_uuid(payload.get("zone_id"), "zone_id"),
                event_timestamp,
                available_delta=-quantity,
                reserved_delta=quantity,
            )
        elif event_type == "PRODUCT_RELEASED":
            quantity = payload.get("quantity")
            self._validate_positive_quantity(quantity)
            mutated = self._apply_inventory_delta(
                batch,
                self._parse_uuid(payload.get("product_id"), "product_id"),
                self._parse_uuid(payload.get("zone_id"), "zone_id"),
                event_timestamp,
                available_delta=quantity,
                reserved_delta=-quantity,
            )
        elif event_type == "INVENTORY_COUNTED":
            counted_quantity = payload.get("counted_quantity")
            self._validate_positive_quantity(counted_quantity, "counted_quantity")
            mutated = self._apply_inventory_delta(
                batch,
                self._parse_uuid(payload.get("product_id"), "product_id"),
                self._parse_uuid(payload.get("zone_id"), "zone_id"),
                event_timestamp,
                absolute_available=counted_quantity,
            )
        elif event_type == "ORDER_CREATED":
            order_id = self._parse_uuid(payload.get("order_id"), "order_id")
            items = payload.get("items")
            if not isinstance(items, list) or not items:
                raise EventValidationError("items must be a non-empty array")
            batch.add(
                SimpleStatement(
                    """
                    UPDATE orders
                    SET status = %s, last_event_timestamp = %s
                    WHERE order_id = %s
                    """
                ),
                ("CREATED", event_timestamp, order_id),
            )
            mutated = True
            for item in items:
                quantity = item.get("quantity")
                self._validate_positive_quantity(quantity)
                changed = self._apply_inventory_delta(
                    batch,
                    self._parse_uuid(item.get("product_id"), "product_id"),
                    self._parse_uuid(item.get("zone_id"), "zone_id"),
                    event_timestamp,
                    available_delta=-quantity,
                    reserved_delta=quantity,
                )
                mutated = mutated or changed
        elif event_type == "ORDER_COMPLETED":
            order_id = self._parse_uuid(payload.get("order_id"), "order_id")
            items = payload.get("items")
            if not isinstance(items, list) or not items:
                raise EventValidationError("items must be a non-empty array")
            batch.add(
                SimpleStatement(
                    """
                    UPDATE orders
                    SET status = %s, last_event_timestamp = %s
                    WHERE order_id = %s
                    """
                ),
                ("COMPLETED", event_timestamp, order_id),
            )
            mutated = True
            for item in items:
                quantity = item.get("quantity")
                self._validate_positive_quantity(quantity)
                changed = self._apply_inventory_delta(
                    batch,
                    self._parse_uuid(item.get("product_id"), "product_id"),
                    self._parse_uuid(item.get("zone_id"), "zone_id"),
                    event_timestamp,
                    reserved_delta=-quantity,
                )
                mutated = mutated or changed
        else:
            raise UnsupportedEventError(f"unsupported event_type: {event_type}")

        self._append_event_history(batch, event_type, event_timestamp, event_id, payload)
        batch.add(
            SimpleStatement(
                """
                INSERT INTO processed_events (event_id, processed_at, event_type, partition, offset)
                VALUES (%s, toTimestamp(now()), %s, %s, %s)
                """
            ),
            (event_id, event_type, partition, offset),
        )
        return event_id, event_type, batch, mutated

    def send_to_dlq(self, raw_event, error_reason, error_code, msg):
        payload = {
            "original_event": raw_event,
            "error_reason": error_reason,
            "error_code": error_code,
            "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "kafka_metadata": {
                "partition": msg.partition(),
                "offset": msg.offset(),
            },
        }
        self.dlq_producer.produce(self.dlq_topic, value=json.dumps(payload).encode("utf-8"))
        self.dlq_producer.flush()

    def run(self):
        global _consumer_instance
        _consumer_instance = self

        schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        metrics_port = int(os.getenv("METRICS_PORT", "8000"))

        server = HTTPServer(("0.0.0.0", metrics_port), ConsumerHTTPHandler)
        Thread(target=server.serve_forever, daemon=True).start()
        logging.info("http server started on port %s", metrics_port)

        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.serialization import SerializationContext, MessageField

        schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

        with open("schemas/warehouse_event.avsc", "r") as f:
            schema_str = f.read()

        avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

        endpoint = "warehouse-events"
        method = "consume"

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error("consumer error=%s", msg.error())
                    continue

                raw_event = None
                event_type = "UNKNOWN"
                try:
                    raw_event = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

                    if isinstance(raw_event.get("payload"), str):
                        raw_event["payload"] = json.loads(raw_event["payload"])

                    event_type = raw_event.get("event_type", "UNKNOWN")
                    event_id = self._parse_uuid(raw_event.get("event_id"), "event_id")

                    if self._is_event_processed(event_id):
                        self.consumer.commit(message=msg)
                        logging.info(
                            "duplicate event skipped event_id=%s event_type=%s offset=%s partition=%s",
                            event_id,
                            event_type,
                            msg.offset(),
                            msg.partition(),
                        )
                        KAFKA_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status="duplicate").inc()
                        continue

                    with KAFKA_REQUEST_DURATION_SECONDS.labels(method=method, endpoint=endpoint).time():
                        processed_event_id, event_type, batch, mutated = self._build_batch(
                            raw_event,
                            msg.partition(),
                            msg.offset(),
                        )
                        self.session.execute(batch)

                    self.consumer.commit(message=msg)
                    KAFKA_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status="success").inc()
                    logging.info(
                        "processed event_id=%s event_type=%s offset=%s partition=%s mutated=%s",
                        processed_event_id,
                        event_type,
                        msg.offset(),
                        msg.partition(),
                        mutated,
                    )
                except (EventValidationError, UnsupportedEventError, json.JSONDecodeError) as error:
                    logging.error(
                        "message sent to dlq offset=%s partition=%s reason=%s",
                        msg.offset(),
                        msg.partition(),
                        error,
                    )
                    KAFKA_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status="error").inc()
                    KAFKA_REQUEST_ERRORS_TOTAL.labels(method=method, endpoint=endpoint, error_type="validation_error").inc()
                    self.send_to_dlq(raw_event if raw_event is not None else str(msg.value()), str(error), "VALIDATION_ERROR", msg)
                    self.consumer.commit(message=msg)
                except Exception as error:
                    logging.exception("unexpected processing error offset=%s partition=%s", msg.offset(), msg.partition())
                    KAFKA_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status="error").inc()
                    KAFKA_REQUEST_ERRORS_TOTAL.labels(method=method, endpoint=endpoint, error_type="internal_error").inc()
                    self.send_to_dlq(raw_event if raw_event is not None else str(msg.value()), str(error), "INTERNAL_ERROR", msg)
                    self.consumer.commit(message=msg)
        finally:
            self.consumer.close()
            self.cluster.shutdown()


if __name__ == "__main__":
    WarehouseConsumer().run()
