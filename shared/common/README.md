# Shared Common Utilities

**Purpose:** Centralized utilities and base classes used across all services.

## Contents

### Logging
Structured logging helper with standard fields for Kafka-based systems.

```python
from common.logging import setup_logger, get_logger

# In service startup
setup_logger(
    service_name="order-service",
    level="info",
    format="json"  # or "text"
)

# In service code
logger = get_logger(__name__)
logger.info("order_created", extra={
    "order_id": order_id,
    "correlation_id": correlation_id,
    "topic": "orders.created",
    "partition": 0,
    "offset": 12345
})
```

**Standard logging fields:**
- `service`: Service name (e.g., "order-service")
- `event_type`: Event type (e.g., "order_created")
- `order_id`: Order ID from event
- `correlation_id`: Correlation ID for tracing
- `topic`: Kafka topic name
- `partition`: Kafka partition number
- `offset`: Kafka offset

### Config
Environment-based configuration management.

```python
from common.config import get_config, ServiceConfig

# Loads from environment variables with type coercion
config = get_config()

print(config.app_env)  # "local|dev|prod"
print(config.log_level)  # "debug|info|warning|error"
print(config.kafka_bootstrap_servers)  # "localhost:9092"
print(config.kafka_security_protocol)  # "plaintext|ssl|sasl_ssl"
```

**Configuration values:**
- `APP_ENV`: Deployment environment
- `LOG_LEVEL`: Logging verbosity
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_CLIENT_ID`: Kafka client identifier
- `KAFKA_CONSUMER_GROUP`: Consumer group name (for consumers)
- `KAFKA_SECURITY_PROTOCOL`: Security protocol
- Service-specific: `INVENTORY_FAIL_RATE`, `PAYMENT_FAIL_RATE`, etc.

### Kafka Helpers
Utilities for producer and consumer initialization.

```python
from common.kafka import create_producer, create_consumer

# Producer initialization
producer = create_producer(config)
producer.send(topic="orders.created", value=event_dict)

# Consumer initialization
consumer = create_consumer(
    config=config,
    topics=["orders.created"],
)
for msg in consumer:
    process_event(msg.value())
    consumer.commit()
```

### Error Handling
Custom exceptions for the system.

```python
from common.errors import (
    ContractValidationError,
    KafkaConnectionError,
    ProcessingError
)

try:
    validate_event(event)
except ContractValidationError as e:
    logger.error("validation_failed", extra={"error": str(e)})
    # Send to DLQ or skip
```

## Usage in Services

### Minimal service template
```python
# services/my-service/main.py
from common.config import get_config
from common.logging import setup_logger, get_logger
from common.kafka import create_consumer

logger = get_logger(__name__)
config = get_config()

setup_logger(
    service_name=config.kafka_client_id,
    level=config.log_level
)

def main():
    consumer = create_consumer(config, topics=["orders.created"])
    
    try:
        for msg in consumer:
            event = msg.value()
            logger.info("event_received", extra={
                "event_type": event.get("event_type"),
                "order_id": event.get("order_id")
            })
            # Process event
            consumer.commit()
    except KeyboardInterrupt:
        logger.info("shutting_down")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
```

## Dependencies
- Python 3.11+
- confluent-kafka or kafka-python
- Pydantic 2.0+ (for config validation)

## Next Steps
- Add metrics/tracing helpers (OpenTelemetry integration)
- Add circuit breaker pattern for downstream calls
- Add deduplication store abstraction (Redis, in-memory, etc.)
- Add health check helpers for Kubernetes probes
