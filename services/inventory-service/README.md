# Inventory Service

**Role:** Mock inventory reservation consumer that simulates stock reservation for orders.

## Overview
The Inventory Service consumes `OrderCreated` events, simulates a stock reservation check against a mock database, and publishes either `InventoryReserved` or `InventoryFailed` events. It demonstrates event choreography in a distributed system.

## Responsibilities
- Consume OrderCreated events from `orders.created` topic
- Validate order contract and extract items
- Simulate inventory lookup and reservation (mock database)
- Publish InventoryReserved event on success
- Publish InventoryFailed event on failure or insufficient stock
- Track consumer lag and handle rebalances gracefully
- Log all state transitions with correlation_id

## Architecture

```
[orders.created topic]
    ↓ consume OrderCreated
[Inventory Service Consumer]
    ↓ validate & check mock stock
[Inventory Logic]
    ├─→ InventoryReserved → [inventory.reserve.result topic]
    └─→ InventoryFailed → [inventory.reserve.result topic]
```

## Implementation Checklist (Milestone 1)
- [ ] Kafka consumer setup with consumer group `inventory-service.v1`
- [ ] Mock inventory database (in-memory dict with product stock levels)
- [ ] Consume from `orders.created` topic
- [ ] Parse and validate OrderCreatedEventV1 contract
- [ ] Simulate stock check with configurable failure rate
- [ ] Publish InventoryReservedEventV1 on success
- [ ] Publish InventoryFailedEventV1 on failure
- [ ] Handle consumer group rebalancing
- [ ] Structured logging with event_type and order_id
- [ ] Graceful shutdown on SIGTERM
- [ ] Health check endpoint or status logging

## Environment Variables
```
APP_ENV=local|dev|prod
LOG_LEVEL=debug|info|warning|error
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=inventory-service-consumer-1
KAFKA_CONSUMER_GROUP=inventory-service.v1
KAFKA_SECURITY_PROTOCOL=plaintext
INVENTORY_FAIL_RATE=0.1  # Percentage of orders that fail reservation (0.0-1.0)
```

## Mock Inventory Database
Initial stock levels (in-memory):
```python
MOCK_INVENTORY = {
    "keyboard": 100,
    "mouse": 150,
    "monitor": 50,
    "headset": 80,
    "laptop-stand": 60,
    "webcam": 90,
}
```

When an order arrives, the service:
1. Checks if all items have sufficient stock
2. With `INVENTORY_FAIL_RATE` probability, returns FAILED regardless
3. Otherwise, decrements stock and returns RESERVED
4. On service restart, stock resets (learning mode only)

## Local Testing

### 1. Start Kafka and Order Service (if not running)
```bash
docker-compose -f infra/docker/docker-compose.yml up -d kafka
cd services/order-service
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 2. Create topics
```bash
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic orders.created \
  --partitions 1
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic inventory.reserve.result \
  --partitions 1
```

### 3. Run Inventory Service
```bash
cd services/inventory-service
pip install -r requirements.txt
export INVENTORY_FAIL_RATE=0.1
python main.py
```

### 4. Send an order (from another terminal)
```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "aditi-001",
    "customer_email": "aditi@example.com",
    "items": [{"product_id": "keyboard", "quantity": 1, "unit_price": 75.50}],
    "shipping_address": {"street": "123 Main St", "city": "SF", "state": "CA", "postal_code": "94105"}
  }'
```

### 5. Verify events
```bash
# Check inventory.reserve.result topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic inventory.reserve.result --from-beginning --max-messages 1
```

## Event Contracts

### Input: OrderCreatedEventV1
```json
{
  "event_id": "evt-550e8400-e29b-41d4-a716-446655440000",
  "event_type": "order_created",
  "occurred_at": "2026-08-31T10:30:00Z",
  "correlation_id": "corr-550e8400-e29b-41d4-a716-446655440001",
  "causation_id": "evt-550e8400-e29b-41d4-a716-446655440000",
  "idempotency_key": "idkey-550e8400-e29b-41d4-a716-446655440002",
  "order_id": "ord-550e8400-e29b-41d4-a716-446655440000",
  "payload": {
    "customer_id": "aditi-001",
    "items": [
      {"product_id": "keyboard", "quantity": 2, "unit_price": 75.50}
    ]
  }
}
```

### Output: InventoryReservedEventV1
```json
{
  "event_id": "evt-550e8400-e29b-41d4-a716-446655440100",
  "event_type": "inventory_reserved",
  "occurred_at": "2026-08-31T10:30:01Z",
  "correlation_id": "corr-550e8400-e29b-41d4-a716-446655440001",
  "causation_id": "evt-550e8400-e29b-41d4-a716-446655440000",
  "idempotency_key": "idkey-550e8400-e29b-41d4-a716-446655440002",
  "order_id": "ord-550e8400-e29b-41d4-a716-446655440000",
  "payload": {
    "reservation_id": "res-550e8400-e29b-41d4-a716-446655440200",
    "reserved_at": "2026-08-31T10:30:01Z"
  }
}
```

### Output: InventoryFailedEventV1
```json
{
  "event_id": "evt-550e8400-e29b-41d4-a716-446655440101",
  "event_type": "inventory_failed",
  "occurred_at": "2026-08-31T10:30:01Z",
  "correlation_id": "corr-550e8400-e29b-41d4-a716-446655440001",
  "causation_id": "evt-550e8400-e29b-41d4-a716-446655440000",
  "idempotency_key": "idkey-550e8400-e29b-41d4-a716-446655440002",
  "order_id": "ord-550e8400-e29b-41d4-a716-446655440000",
  "payload": {
    "reason": "insufficient_stock|mock_failure",
    "failed_at": "2026-08-31T10:30:01Z"
  }
}
```

## Dependencies
- confluent-kafka or kafka-python
- Pydantic 2.0+
- Python 3.11+

## Next Steps (Reliability Phase)
- Implement idempotency deduplication (track processed event_ids)
- Add dead-letter queue for malformed messages
- Implement exponential backoff for Kafka connect retries
- Add Prometheus metrics for reservation success rate
