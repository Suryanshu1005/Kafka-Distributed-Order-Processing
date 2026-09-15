# Shipping Service

**Role:** Mock shipping creation consumer that simulates shipment generation for authorized orders.

## Overview
The Shipping Service consumes `PaymentAuthorized` events, simulates shipment creation with a mock shipping provider, and publishes `ShipmentCreated` events. It is the final producer in the happy path and feeds into terminal order completion.

## Responsibilities
- Consume PaymentAuthorized events from `payment.authorize.result` topic
- Validate event contract and extract shipping address
- Simulate shipment creation (mock carrier system)
- Generate fake tracking ID and estimated delivery date
- Publish ShipmentCreatedEventV1 with shipment details
- Handle shipping provider failures gracefully
- Log shipment transactions with order_id and tracking info
- Support consumer rebalancing and graceful shutdown

## Architecture

```
[payment.authorize.result topic]
    ↓ consume PaymentAuthorized
[Shipping Service Consumer]
    ↓ validate & create shipment
[Shipping Logic]
    └─→ ShipmentCreated → [shipping.create.result topic]
```

## Implementation Checklist (Milestone 1)
- [ ] Kafka consumer setup with consumer group `shipping-service.v1`
- [ ] Only consume PaymentAuthorized events (filter on event_type)
- [ ] Extract shipping address from order payload
- [ ] Simulate shipment creation with mock carrier
- [ ] Generate unique tracking_id
- [ ] Calculate estimated_delivery_date (5-7 business days mock)
- [ ] Publish ShipmentCreatedEventV1
- [ ] Structured logging with tracking and delivery info
- [ ] Consumer group rebalancing support
- [ ] Graceful shutdown on SIGTERM
- [ ] Handle malformed messages without crashing

## Environment Variables
```
APP_ENV=local|dev|prod
LOG_LEVEL=debug|info|warning|error
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=shipping-service-consumer-1
KAFKA_CONSUMER_GROUP=shipping-service.v1
KAFKA_SECURITY_PROTOCOL=plaintext
SHIPPING_FAIL_RATE=0.05  # Percentage of shipments that fail to create (0.0-1.0)
```

## Mock Shipping Provider
Simulates a carrier integration:
- Accepts shipping address and order details
- With `SHIPPING_FAIL_RATE` probability, returns FAILED
- Otherwise, generates a tracking ID and estimated delivery date
- No actual shipment label generation

Tracking ID format: `TRK-<8-digit-random>-<order_id_suffix>`

Estimated delivery: today + 5-7 business days (randomized)

## Local Testing

### 1. Start full pipeline (if not running)
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

# Terminal 4: Payment Service
cd services/payment-service
export PAYMENT_FAIL_RATE=0.2
python main.py
```

### 2. Create topics
```bash
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic payment.authorize.result \
  --partitions 1
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic shipping.create.result \
  --partitions 1
```

### 3. Run Shipping Service
```bash
cd services/shipping-service
pip install -r requirements.txt
export SHIPPING_FAIL_RATE=0.05
python main.py
```

### 4. Send an order (from another terminal)
```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "priya-001",
    "customer_email": "priya@example.com",
    "items": [{"product_id": "headset", "quantity": 1, "unit_price": 129.99}],
    "shipping_address": {
      "street": "789 Oak Ave",
      "city": "Seattle",
      "state": "WA",
      "postal_code": "98101"
    }
  }'
```

### 5. Verify end-to-end event flow
```bash
# Check shipping.create.result
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic shipping.create.result --from-beginning --max-messages 1
```

## Event Contracts

### Input: PaymentAuthorizedEventV1
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
    "amount": 129.99,
    "customer_id": "priya-001",
    "shipping_address": {
      "street": "789 Oak Ave",
      "city": "Seattle",
      "state": "WA",
      "postal_code": "98101"
    }
  }
}
```

### Output: ShipmentCreatedEventV1
```json
{
  "event_id": "evt-550e8400-e29b-41d4-a716-446655440400",
  "event_type": "shipment_created",
  "occurred_at": "2026-08-31T10:30:03Z",
  "correlation_id": "corr-550e8400-e29b-41d4-a716-446655440001",
  "causation_id": "evt-550e8400-e29b-41d4-a716-446655440200",
  "idempotency_key": "idkey-550e8400-e29b-41d4-a716-446655440002",
  "order_id": "ord-550e8400-e29b-41d4-a716-446655440000",
  "payload": {
    "shipment_id": "ship-550e8400-e29b-41d4-a716-446655440500",
    "tracking_id": "TRK-87654321-ord-000",
    "carrier": "FedEx",
    "estimated_delivery_date": "2026-09-07",
    "created_at": "2026-08-31T10:30:03Z"
  }
}
```

## Dependencies
- confluent-kafka or kafka-python
- Pydantic 2.0+
- Python 3.11+

## Next Steps (Terminal Events & Notifications Phase)
- Create notification consumer that listens to ShipmentCreated and publishes OrderCompleted
- Implement compensation flow if shipment fails
- Add Prometheus metrics for shipment creation success rate
- Track fulfillment SLA metrics
