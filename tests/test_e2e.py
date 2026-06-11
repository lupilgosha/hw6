"""
E2E test: полный пользовательский сценарий склада.

Сценарий:
  1. Отправить PRODUCT_RECEIVED через Kafka (producer -> Schema Registry -> Kafka)
  2. Consumer обрабатывает событие и сохраняет в Cassandra
  3. Проверить остатки через HTTP API consumer (/inventory)
  4. Проверить состояние напрямую в Cassandra

Проверки:
  - HTTP статус /inventory == 200
  - Тело ответа содержит корректные поля и типы
  - Cassandra содержит запись с ожидаемым количеством
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone

import pytest
import requests
from cassandra.cluster import Cluster
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CASSANDRA_HOST = os.getenv("CASSANDRA_CONTACT_POINTS", "localhost")
CONSUMER_API_URL = os.getenv("CONSUMER_API_URL", "http://localhost:8000")
TOPIC = os.getenv("WAREHOUSE_TOPIC", "warehouse-events")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "warehouse_event.avsc")


@pytest.fixture(scope="module")
def cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect("warehouse")
    yield session
    cluster.shutdown()


def test_full_warehouse_scenario(cassandra_session):
    src = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()
    ser = AvroSerializer(src, schema_str)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    product_id = str(uuid.uuid4())
    zone_id = str(uuid.uuid4())
    quantity = 100

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "PRODUCT_RECEIVED",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": json.dumps({"product_id": product_id, "zone_id": zone_id, "quantity": quantity}, sort_keys=True),
    }
    producer.produce(
        topic=TOPIC,
        key=product_id.encode(),
        value=ser(event, SerializationContext(TOPIC, MessageField.VALUE)),
    )
    producer.flush()

    deadline = time.time() + 30
    response = None
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{CONSUMER_API_URL}/inventory",
                params={"product_id": product_id, "zone_id": zone_id},
                timeout=3,
            )
            if r.status_code == 200:
                response = r
                break
        except requests.RequestException:
            pass
        time.sleep(1)

    assert response is not None, "consumer /inventory did not return 200 within timeout"
    assert response.status_code == 200

    body = response.json()
    assert "product_id" in body
    assert "zone_id" in body
    assert "available_quantity" in body
    assert "reserved_quantity" in body
    assert isinstance(body["available_quantity"], int)
    assert isinstance(body["reserved_quantity"], int)
    assert body["product_id"] == product_id
    assert body["zone_id"] == zone_id
    assert body["available_quantity"] == quantity

    row = cassandra_session.execute(
        "SELECT available_quantity FROM inventory_by_product_zone WHERE product_id = %s AND zone_id = %s",
        (uuid.UUID(product_id), uuid.UUID(zone_id)),
    ).one()
    assert row is not None, "record not found in Cassandra"
    assert row.available_quantity == quantity
