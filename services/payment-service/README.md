# Payment Service

**Role:** Mock payment authorization consumer that simulates payment processing for orders.

## Overview
The Payment Service consumes `InventoryReserved` events, simulates a payment authorization check against a mock payment gateway, and publishes either `PaymentAuthorized` or `PaymentFailed` events. It demonstrates conditional downstream event production.

## Responsibilities
- Consume InventoryReserved events from `inventory.reserve.result` topic
- Validate event contract and extract order details
- Simulate payment authorization (mock gateway)
- Publish PaymentAuthorized event on success
- Publish PaymentFailed event on failure
- Handle payment decline scenarios (insufficient funds, declined card, etc.)
- Track consumer lag and handle rebalances gracefully
- Log all transactions with correlation_id and amount

## Architecture

```
[inventory.reserve.result topic]
    ↓ consume InventoryReserved
[Payment Service Consumer]
    ↓ validate & authorize payment
[Payment Logic]
    ├─→ PaymentAuthorized → [payment.authorize.result topic]
    └─→ PaymentFailed → [payment.authorize.result topic]
```

## Implementation Checklist (Milestone 1)
- [ ] Kafka consumer setup with consumer group `payment-service.v1`
- [ ] Only consume InventoryReserved events (filter on event_type)
- [ ] Extract order total from items in order payload
- [ ] Simulate payment authorization with configurable failure rate
- [ ] Publish PaymentAuthorizedEventV1 on success
- [ ] Publish PaymentFailedEventV1 on failure
- [ ] Graceful handling of malformed messages
- [ ] Structured logging with transaction details
- [ ] Consumer group rebalancing support
- [ ] Graceful shutdown on SIGTERM

## Environment Variables
```
APP_ENV=local|dev|prod
LOG_LEVEL=debug|info|warning|error
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=payment-service-consumer-1
KAFKA_CONSUMER_GROUP=payment-service.v1
KAFKA_SECURITY_PROTOCOL=plaintext
PAYMENT_FAIL_RATE=0.2  # Percentage of authorizations that fail (0.0-1.0)
```

## Mock Payment Gateway
Simulates a payment processor:
- Accepts payment amount and customer details
- With `PAYMENT_FAIL_RATE` probability, returns DECLINED
- Otherwise, returns AUTHORIZED with a mock transaction_id
- No actual card processing

Decline reasons (randomly selected when failing):
- "insufficient_funds"
- "card_declined"
- "authentication_failed"
- "processor_error"

## Local Testing

### 1. Start Kafka, Order Service, and Inventory Service (if not running)
```bash
# Terminal 1: Kafka
docker-compose -f infra/docker/docker-compose.yml up -d kafka

# Terminal 2: Order Service
cd services/order-service
uvicorn main:app --host 0.0.0.0 --port 8000

# Terminal 3: Inventory Service
cd services/inventory-service
export INVENTORY_FAIL_RATE=0.1
python main.py
```

### 2. Create topics
```bash
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic inventory.reserve.result \
  --partitions 1
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic payment.authorize.result \
  --partitions 1
```

### 3. Run Payment Service
```bash
cd services/payment-service
pip install -r requirements.txt
export PAYMENT_FAIL_RATE=0.2
python main.py
```

### 4. Send an order (from another terminal)
```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "rahul-001",
    "customer_email": "rahul@example.com",
    "items": [{"product_id": "monitor", "quantity": 1, "unit_price": 299.99}],
    "shipping_address": {"street": "456 Elm St", "city": "NYC", "state": "NY", "postal_code": "10001"}
  }'
```

### 5. Verify events flowing through pipeline
```bash
# Check inventory.reserve.result
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic inventory.reserve.result --from-beginning --max-messages 1

# Check payment.authorize.result
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic payment.authorize.result --from-beginning --max-messages 1
```

## Event Contracts

### Input: InventoryReservedEventV1
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
    "customer_id": "rahul-001",
    "total_amount": 299.99,
    "items": [
      {"product_id": "monitor", "quantity": 1, "unit_price": 299.99}
    ]
  }
}
```

### Output: PaymentAuthorizedEventV1
```json
{
  "event_id": "evt-550e8400-e29b-41d4-a716-446655440200",
  "event_type": "payment_authorized",
  "occurred_at": "2026-08-31T10:30:02Z",
  "correlation_id": "corr-550e8400-e29b-41d4-a716-446655440001",
  "causation_id": "evt-550e8400-e29b-41d4-a716-446655440100",
  "idempotency_key": "idkey-550e8400-e29b-41d4-a716-446655440002",
  "order_id": "ord-550e8400-e29b-41d4-a716-446655440000",
  "payload": {
    "transaction_id": "txn-550e8400-e29b-41d4-a716-446655440300",
    "amount": 299.99,
    "authorized_at": "2026-08-31T10:30:02Z"
  }
}
```

### Output: PaymentFailedEventV1
```json
{
  "event_id": "evt-550e8400-e29b-41d4-a716-446655440201",
  "event_type": "payment_failed",
  "occurred_at": "2026-08-31T10:30:02Z",
  "correlation_id": "corr-550e8400-e29b-41d4-a716-446655440001",
  "causation_id": "evt-550e8400-e29b-41d4-a716-446655440100",
  "idempotency_key": "idkey-550e8400-e29b-41d4-a716-446655440002",
  "order_id": "ord-550e8400-e29b-41d4-a716-446655440000",
  "payload": {
    "reason": "insufficient_funds|card_declined|authentication_failed",
    "amount": 299.99,
    "failed_at": "2026-08-31T10:30:02Z"
  }
}
```

## Dependencies
- confluent-kafka or kafka-python
- Pydantic 2.0+
- Python 3.11+

## Next Steps (Reliability Phase)
- Implement idempotency deduplication (track processed event_ids)
- Add dead-letter queue for authorization failures (distinct from service failures)
- Implement compensation flow (refund on payment reversal)
- Add Prometheus metrics for authorization success rate and amount tracking
