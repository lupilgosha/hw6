"""
Integration tests: проверяют взаимодействие между сервисами.
Требуют запущенного docker-compose окружения.
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone

import pytest
from cassandra.cluster import Cluster
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CASSANDRA_HOST = os.getenv("CASSANDRA_CONTACT_POINTS", "localhost")
TOPIC = os.getenv("WAREHOUSE_TOPIC", "warehouse-events")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "warehouse_event.avsc")


@pytest.fixture(scope="module")
def cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect("warehouse")
    yield session
    cluster.shutdown()


@pytest.fixture(scope="module")
def avro_producer():
    src = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()
    ser = AvroSerializer(src, schema_str)
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    return p, ser


def send_event(producer, serializer, event_type, payload):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": json.dumps(payload, sort_keys=True),
    }
    key = payload.get("product_id", event["event_id"])
    producer.produce(
        topic=TOPIC,
        key=key.encode(),
        value=serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
    )
    producer.flush()
    return event


def poll_cassandra(session, product_id, zone_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = session.execute(
            "SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone "
            "WHERE product_id = %s AND zone_id = %s",
            (uuid.UUID(product_id), uuid.UUID(zone_id)),
        ).one()
        if row is not None:
            return row
        time.sleep(1)
    return None


def test_producer_to_kafka_schema_registry(avro_producer):
    """Producer -> Schema Registry -> Kafka: событие успешно сериализуется и доставляется."""
    p, ser = avro_producer
    product_id = str(uuid.uuid4())
    zone_id = str(uuid.uuid4())
    event = send_event(p, ser, "PRODUCT_RECEIVED", {
        "product_id": product_id, "zone_id": zone_id, "quantity": 10,
    })
    assert event["event_id"]
    assert event["event_type"] == "PRODUCT_RECEIVED"


def test_kafka_to_consumer_to_cassandra(cassandra_session, avro_producer):
    """Kafka -> Consumer -> Cassandra: событие обрабатывается и сохраняется в БД."""
    p, ser = avro_producer
    product_id = str(uuid.uuid4())
    zone_id = str(uuid.uuid4())

    send_event(p, ser, "PRODUCT_RECEIVED", {
        "product_id": product_id, "zone_id": zone_id, "quantity": 55,
    })

    row = poll_cassandra(cassandra_session, product_id, zone_id)
    assert row is not None, "record not found in Cassandra after timeout"
    assert row.available_quantity == 55


def test_invalid_event_routed_to_dlq(avro_producer):
    """Consumer -> DLQ: невалидное событие не блокирует consumer и уходит в DLQ."""
    p, ser = avro_producer

    dlq_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"dlq-check-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    dlq_consumer.subscribe(["warehouse-events-dlq"])
    # дождаться assignment партиций
    deadline_assign = time.time() + 10
    while time.time() < deadline_assign:
        dlq_consumer.poll(0.5)
        if dlq_consumer.assignment():
            break

    send_event(p, ser, "PRODUCT_RECEIVED", {
        "product_id": str(uuid.uuid4()),
        "zone_id": str(uuid.uuid4()),
        "quantity": -1,
    })

    found = False
    deadline = time.time() + 30
    while time.time() < deadline:
        msg = dlq_consumer.poll(1.0)
        if msg and not msg.error():
            data = json.loads(msg.value())
            if "error_reason" in data:
                found = True
                break
    dlq_consumer.close()
    assert found, "DLQ message not received within timeout"
