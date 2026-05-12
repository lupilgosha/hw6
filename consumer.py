import json
import logging
import uuid
from datetime import datetime
from confluent_kafka import Consumer, Producer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WarehouseConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'warehouse-state-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        self.consumer.subscribe(['warehouse-events'])
        
        self.dlq_producer = Producer({'bootstrap.servers': 'localhost:9092'})

        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect()
        self._init_db()

    def _init_db(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS warehouse 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        self.session.set_keyspace('warehouse')
        
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
                product_id uuid,
                zone_id uuid,
                available_quantity int,
                reserved_quantity int,
                last_updated_at timestamp,
                PRIMARY KEY ((product_id, zone_id))
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS inventory_by_product (
                product_id uuid,
                zone_id uuid,
                available_quantity int,
                reserved_quantity int,
                last_updated_at timestamp,
                PRIMARY KEY ((product_id), zone_id)
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS inventory_by_zone (
                zone_id uuid,
                product_id uuid,
                available_quantity int,
                reserved_quantity int,
                last_updated_at timestamp,
                PRIMARY KEY ((zone_id), product_id)
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id uuid,
                processed_at timestamp,
                PRIMARY KEY (event_id)
            );
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id uuid,
                status text,
                PRIMARY KEY (order_id)
            );
        """)

    def _is_event_processed(self, event_id):
        query = "SELECT event_id FROM processed_events WHERE event_id = %s"
        result = self.session.execute(query, (event_id,))
        return result.one() is not None

    def _get_inventory(self, product_id, zone_id):
        query = "SELECT available_quantity, reserved_quantity, last_updated_at FROM inventory_by_product_zone WHERE product_id = %s AND zone_id = %s"
        result = self.session.execute(query, (product_id, zone_id)).one()
        if result:
            return result.available_quantity, result.reserved_quantity, result.last_updated_at
        return 0, 0, None

    def _add_inventory_updates_to_batch(self, batch, product_id, zone_id, available_delta, reserved_delta, event_timestamp, absolute_available=None):
        curr_avail, curr_res, last_updated_at = self._get_inventory(product_id, zone_id)
        
        if last_updated_at and event_timestamp <= last_updated_at:
            logging.info(f"Skipping update for product {product_id} in zone {zone_id} due to older timestamp {event_timestamp} <= {last_updated_at}")
            return

        if absolute_available is not None:
            new_avail = absolute_available
            new_res = curr_res
        else:
            new_avail = curr_avail + available_delta
            new_res = curr_res + reserved_delta

        update_query = """
            UPDATE inventory_by_product_zone 
            SET available_quantity = %s, reserved_quantity = %s, last_updated_at = %s
            WHERE product_id = %s AND zone_id = %s
        """
        batch.add(SimpleStatement(update_query), (new_avail, new_res, event_timestamp, product_id, zone_id))

        update_query_prod = """
            UPDATE inventory_by_product 
            SET available_quantity = %s, reserved_quantity = %s, last_updated_at = %s
            WHERE product_id = %s AND zone_id = %s
        """
        batch.add(SimpleStatement(update_query_prod), (new_avail, new_res, event_timestamp, product_id, zone_id))

        update_query_zone = """
            UPDATE inventory_by_zone 
            SET available_quantity = %s, reserved_quantity = %s, last_updated_at = %s
            WHERE zone_id = %s AND product_id = %s
        """
        batch.add(SimpleStatement(update_query_zone), (new_avail, new_res, event_timestamp, zone_id, product_id))

    def process_event(self, event):
        event_id = uuid.UUID(event['event_id'])
        event_type = event['event_type']
        payload = event['payload']
        
        event_timestamp_str = event.get('timestamp', datetime.utcnow().isoformat())
        if event_timestamp_str.endswith('Z'):
            event_timestamp_str = event_timestamp_str[:-1] + '+00:00'
        event_timestamp = datetime.fromisoformat(event_timestamp_str)
        event_timestamp = event_timestamp.replace(tzinfo=None)

        if self._is_event_processed(event_id):
            logging.info(f"Event {event_id} already processed, skipping.")
            return

        if 'quantity' in payload and payload['quantity'] < 0:
            raise ValueError(f"Invalid quantity: {payload['quantity']} (must be positive)")
        if 'items' in payload:
            for item in payload['items']:
                if item.get('quantity', 0) < 0:
                    raise ValueError(f"Invalid quantity in items: {item['quantity']} (must be positive)")

        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

        if event_type == 'PRODUCT_RECEIVED':
            self._add_inventory_updates_to_batch(batch, uuid.UUID(payload['product_id']), uuid.UUID(payload['zone_id']), payload['quantity'], 0, event_timestamp)
        
        elif event_type == 'PRODUCT_SHIPPED':
            self._add_inventory_updates_to_batch(batch, uuid.UUID(payload['product_id']), uuid.UUID(payload['zone_id']), -payload['quantity'], 0, event_timestamp)
            
        elif event_type == 'PRODUCT_MOVED':
            self._add_inventory_updates_to_batch(batch, uuid.UUID(payload['product_id']), uuid.UUID(payload['from_zone_id']), -payload['quantity'], 0, event_timestamp)
            self._add_inventory_updates_to_batch(batch, uuid.UUID(payload['product_id']), uuid.UUID(payload['to_zone_id']), payload['quantity'], 0, event_timestamp)
            
        elif event_type == 'PRODUCT_RESERVED':
            self._add_inventory_updates_to_batch(batch, uuid.UUID(payload['product_id']), uuid.UUID(payload['zone_id']), -payload['quantity'], payload['quantity'], event_timestamp)
            
        elif event_type == 'PRODUCT_RELEASED':
            self._add_inventory_updates_to_batch(batch, uuid.UUID(payload['product_id']), uuid.UUID(payload['zone_id']), payload['quantity'], -payload['quantity'], event_timestamp)
            
        elif event_type == 'INVENTORY_COUNTED':
            self._add_inventory_updates_to_batch(batch, uuid.UUID(payload['product_id']), uuid.UUID(payload['zone_id']), 0, 0, event_timestamp, absolute_available=payload['counted_quantity'])
            
        elif event_type == 'ORDER_CREATED':
            batch.add(SimpleStatement("INSERT INTO orders (order_id, status) VALUES (%s, %s)"), (uuid.UUID(payload['order_id']), 'CREATED'))
            for item in payload['items']:
                self._add_inventory_updates_to_batch(batch, uuid.UUID(item['product_id']), uuid.UUID(item['zone_id']), -item['quantity'], item['quantity'], event_timestamp)
                
        elif event_type == 'ORDER_COMPLETED':
            batch.add(SimpleStatement("UPDATE orders SET status = %s WHERE order_id = %s"), ('COMPLETED', uuid.UUID(payload['order_id'])))
            for item in payload['items']:
                self._add_inventory_updates_to_batch(batch, uuid.UUID(item['product_id']), uuid.UUID(item['zone_id']), 0, -item['quantity'], event_timestamp)

        batch.add(SimpleStatement("INSERT INTO processed_events (event_id, processed_at) VALUES (%s, toTimestamp(now()))"), (event_id,))
        
        self.session.execute(batch)

    def send_to_dlq(self, event, error_reason, error_code, msg):
        dlq_msg = {
            "original_event": event,
            "error_reason": error_reason,
            "error_code": error_code,
            "failed_at": datetime.utcnow().isoformat() + "Z",
            "kafka_metadata": {
                "partition": msg.partition(),
                "offset": msg.offset()
            }
        }
        self.dlq_producer.produce(
            'warehouse-events-dlq',
            value=json.dumps(dlq_msg).encode('utf-8')
        )
        self.dlq_producer.poll(0)

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    self.process_event(event)
                    self.consumer.commit(msg)
                    logging.info(f"Processed event_id: {event['event_id']}, event_type: {event['event_type']}, offset: {msg.offset()}, partition: {msg.partition()}")
                except ValueError as e:
                    logging.error(f"Validation error processing message: {e}. Sending to DLQ.")
                    self.send_to_dlq(event, str(e), "VALIDATION_ERROR", msg)
                    self.consumer.commit(msg)
                except Exception as e:
                    logging.error(f"Error processing message: {e}. Sending to DLQ.")
                    self.send_to_dlq(event, str(e), "INTERNAL_ERROR", msg)
                    self.consumer.commit(msg)
        finally:
            self.consumer.close()
            self.dlq_producer.flush()
            self.cluster.shutdown()

if __name__ == '__main__':
    consumer = WarehouseConsumer()
    consumer.run()
