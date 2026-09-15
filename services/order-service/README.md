# Order Service

**Role:** API gateway and order intake endpoint for the distributed order processing system.

## Overview
The Order Service is a FastAPI-based REST API that accepts customer order requests, validates them against the order contract, and publishes `OrderCreated` events to Kafka. It serves as the entry point for all orders into the system.

## Responsibilities
- Accept HTTP POST requests for new orders
- Validate order payloads against `OrderCreatedEventV1` contract
- Generate unique `order_id`, `correlation_id`, and `idempotency_key`
- Publish OrderCreated events to the `orders.created` topic
- Respond with order_id and initial status to caller
- Handle duplicate requests safely (idempotency)

## Architecture

```
[HTTP Client] 
    ↓ POST /orders
[Order Service FastAPI]
    ↓ validate & envelope
[Kafka Producer]
    ↓ publish OrderCreated
[orders.created topic]
```

## Implementation Checklist (Milestone 1)
- [ ] FastAPI app scaffold with uvicorn
- [ ] GET /health endpoint for Kubernetes probes
- [ ] POST /orders endpoint accepting OrderCreateRequest
- [ ] Contract validation using shared/contracts models
- [ ] Kafka producer initialization with bootstrap servers from env
- [ ] Publish OrderCreated events with proper envelope
- [ ] Structured logging with correlation_id
- [ ] Error handling for validation and publish failures
- [ ] Local testing with curl/pytest
- [ ] Docker build configuration

## Environment Variables
```
APP_ENV=local|dev|prod
LOG_LEVEL=debug|info|warning|error
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=order-service-producer-1
KAFKA_SECURITY_PROTOCOL=plaintext
```

## Local Testing

### 1. Start Kafka locally
```bash
docker-compose -f infra/docker/docker-compose.yml up -d kafka
```

### 2. Create topic
```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic orders.created \
  --partitions 1 \
  --replication-factor 1
```

### 3. Run Order Service
```bash
cd services/order-service
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 4. Test order creation
```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d @../../sample-data/orders_api_payloads.json | head -1
```

### 5. Verify Kafka message
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders.created --from-beginning --max-messages 1
```

## API Contract

### POST /orders
**Request:**
```json
{
  "customer_id": "aditi-001",
  "customer_email": "aditi@example.com",
  "items": [
    {
      "product_id": "keyboard",
      "quantity": 2,
      "unit_price": 75.50
    }
  ],
  "shipping_address": {
    "street": "123 Main St",
    "city": "San Francisco",
    "state": "CA",
    "postal_code": "94105"
  }
}
```

**Response (201 Created):**
```json
{
  "order_id": "ord-550e8400-e29b-41d4-a716-446655440000",
  "correlation_id": "corr-550e8400-e29b-41d4-a716-446655440001",
  "status": "created",
  "timestamp": "2026-08-31T10:30:00Z"
}
```

**Error Response (400 Bad Request):**
```json
{
  "detail": "Validation error",
  "errors": [
    {
      "loc": ["items"],
      "msg": "must have at least 1 item"
    }
  ]
}
```

## Dependencies
- FastAPI 0.104+
- Pydantic 2.0+
- confluent-kafka or kafka-python
- uvicorn

## Next Steps (Phase 2)
- Implement order state persistence (optional, for replay/audit)
- Add order lookup endpoint GET /orders/{order_id}
- Implement request deduplication cache
- Add authentication/authorization
