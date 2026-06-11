import os
import sys
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer import EventValidationError, UnsupportedEventError, WarehouseConsumer


@pytest.fixture
def consumer():
    with patch("consumer.Cluster"), \
         patch("consumer.Consumer"), \
         patch("consumer.Producer"), \
         patch("consumer.HTTPServer"), \
         patch("consumer.Thread"):
        c = WarehouseConsumer.__new__(WarehouseConsumer)
        c.topic = "warehouse-events"
        c.dlq_topic = "warehouse-events-dlq"
        c.group_id = "warehouse-state-consumer"
        c.consumer = MagicMock()
        c.dlq_producer = MagicMock()
        c.cluster = MagicMock()
        c.session = MagicMock()
        c.session.execute.return_value = MagicMock(one=MagicMock(return_value=None))
        return c


def make_event(event_type, payload):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": payload,
    }


def test_product_received_valid(consumer):
    product_id = str(uuid.uuid4())
    zone_id = str(uuid.uuid4())
    event = make_event("PRODUCT_RECEIVED", {"product_id": product_id, "zone_id": zone_id, "quantity": 50})
    event_id, event_type, batch, mutated = consumer._build_batch(event, 0, 0)
    assert event_type == "PRODUCT_RECEIVED"
    assert mutated is True


def test_product_received_invalid_quantity(consumer):
    event = make_event("PRODUCT_RECEIVED", {"product_id": str(uuid.uuid4()), "zone_id": str(uuid.uuid4()), "quantity": -5})
    with pytest.raises(EventValidationError):
        consumer._build_batch(event, 0, 0)


def test_unsupported_event_type(consumer):
    event = make_event("UNKNOWN_EVENT", {"product_id": str(uuid.uuid4())})
    with pytest.raises(UnsupportedEventError):
        consumer._build_batch(event, 0, 0)


def test_stale_event_ignored(consumer):
    old_ts = datetime(2024, 1, 1, 12, 0, 0)
    stale_ts = datetime(2024, 1, 1, 11, 0, 0)
    result = consumer._ensure_newer_event(old_ts, stale_ts, uuid.uuid4(), uuid.uuid4())
    assert result is False


def test_duplicate_event_detected(consumer):
    event_id = uuid.uuid4()
    row = MagicMock()
    row.event_id = event_id
    consumer.session.execute.return_value = MagicMock(one=MagicMock(return_value=row))
    assert consumer._is_event_processed(event_id) is True


def test_new_event_not_duplicate(consumer):
    consumer.session.execute.return_value = MagicMock(one=MagicMock(return_value=None))
    assert consumer._is_event_processed(uuid.uuid4()) is False
