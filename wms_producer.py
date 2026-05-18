import json
import logging
import os
import time
import uuid
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


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


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("WAREHOUSE_TOPIC", "warehouse-events")

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    with open("schemas/warehouse_event.avsc", "r", encoding="utf-8") as schema_file:
        schema = schema_file.read()

    serializer = AvroSerializer(schema_registry_client, schema)
    producer = Producer({"bootstrap.servers": kafka_bootstrap})

    product_id = str(uuid.uuid4())
    zone_a = str(uuid.uuid4())
    zone_b = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    events = [
        build_event(
            "PRODUCT_RECEIVED",
            started_at,
            {"product_id": product_id, "zone_id": zone_a, "quantity": 100},
        ),
        build_event(
            "PRODUCT_SHIPPED",
            started_at + timedelta(minutes=5),
            {"product_id": product_id, "zone_id": zone_a, "quantity": 20},
        ),
        build_event(
            "PRODUCT_RECEIVED",
            started_at + timedelta(minutes=2),
            {"product_id": product_id, "zone_id": zone_a, "quantity": 50},
        ),
        build_event(
            "PRODUCT_MOVED",
            started_at + timedelta(minutes=10),
            {"product_id": product_id, "from_zone_id": zone_a, "to_zone_id": zone_b, "quantity": 10},
        ),
        build_event(
            "PRODUCT_RESERVED",
            started_at + timedelta(minutes=12),
            {"product_id": product_id, "zone_id": zone_a, "quantity": 5},
        ),
        build_event(
            "PRODUCT_RELEASED",
            started_at + timedelta(minutes=13),
            {"product_id": product_id, "zone_id": zone_a, "quantity": 5},
        ),
        build_event(
            "INVENTORY_COUNTED",
            started_at + timedelta(minutes=14),
            {"product_id": product_id, "zone_id": zone_a, "counted_quantity": 70},
        ),
        build_event(
            "ORDER_CREATED",
            started_at + timedelta(minutes=15),
            {"order_id": order_id, "items": [{"product_id": product_id, "zone_id": zone_a, "quantity": 5}]},
        ),
        build_event(
            "ORDER_COMPLETED",
            started_at + timedelta(minutes=20),
            {"order_id": order_id, "items": [{"product_id": product_id, "zone_id": zone_a, "quantity": 5}]},
        ),
        build_event(
            "PRODUCT_RECEIVED",
            started_at + timedelta(minutes=25),
            {"product_id": product_id, "zone_id": zone_a, "quantity": -5},
        ),
    ]

    events.append(events[0])

    for event in events:
        payload = json.loads(event["payload"])
        key = payload.get("product_id", event["event_id"])
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=serializer(event, SerializationContext(topic, MessageField.VALUE)),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(1)

    producer.flush()
    logging.info("wms producer finished product_id=%s zone_a=%s zone_b=%s order_id=%s", product_id, zone_a, zone_b, order_id)


if __name__ == "__main__":
    time.sleep(20)
    main()
