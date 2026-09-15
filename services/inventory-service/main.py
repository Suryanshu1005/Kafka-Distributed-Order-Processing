import json
import random
import signal
import sys
from datetime import datetime, timezone
from uuid import uuid4

from shared.common.config import get_config
from shared.common.errors import ContractValidationError, EventProcessingError
from shared.common.kafka import (
    KafkaConsumerError,
    KafkaProducerError,
    consume_events,
    create_consumer,
    create_producer,
    send_event,
)
from shared.common.logging import get_logger, log_with_context, setup_logger
from shared.contracts.models.events import (
    InventoryFailedEventV1,
    InventoryReservedEventV1,
    OrderCreatedEventV1,
)


MOCK_INVENTORY = {
    "1": 100,
    "2": 150,
    "3": 50,
    "4": 80,
    "5": 60,
    "6": 90,
}

config = get_config()
setup_logger("inventory-service", config.log_level)
logger = get_logger(__name__)

consumer = None
producer = None
running = True


def handle_shutdown(sig, frame):
    global running
    log_with_context(
        logger, "info", "shutdown signal received", service="inventory-service"
    )
    running = False


def get_or_create_consumer():
    global consumer
    if consumer is None:
        consumer = create_consumer(
            config, topics=["orders.created"], from_beginning=True, consumer_group="inventory-service.v1"
        )
    return consumer


def get_or_create_producer():
    global producer
    if producer is None:
        producer = create_producer(config)
    return producer


def should_fail_reservation() -> bool:
    return random.random() < config.inventory_fail_rate


def check_inventory(order_id: str, items: list) -> tuple[bool, str | None]:
    """Simulate inventory reservation check.

    Returns:
        (success: bool, reason: str | None)
    """
    if should_fail_reservation():
        return False, "mock_failure"

    for item in items:
        product_id = item.get("product_id")
        quantity = item.get("quantity", 0)
        available = MOCK_INVENTORY.get(product_id, 0)

        if available < quantity:
            return False, f"insufficient_stock for {product_id}"

    # Decrement stock
    for item in items:
        product_id = item.get("product_id")
        quantity = item.get("quantity", 0)
        MOCK_INVENTORY[product_id] = MOCK_INVENTORY.get(product_id, 0) - quantity

    return True, None


def build_inventory_result_event(
    order_created_event: dict, success: bool, reason: str | None
) -> InventoryReservedEventV1 | InventoryFailedEventV1:
    """Build a typed inventory result event."""
    status = "RESERVED" if success else "FAILED"

    payload = {
        "status": status,
        "reason": reason,
    }

    event_data = {
        "event_id": str(uuid4()),
        "event_type": "InventoryReserved" if success else "InventoryFailed",
        "occurred_at": datetime.now(timezone.utc),
        "correlation_id": order_created_event["correlation_id"],
        "causation_id": order_created_event["event_id"],
        "idempotency_key": order_created_event["idempotency_key"],
        "order_id": order_created_event["order_id"],
        "payload": payload,
    }

    if success:
        return InventoryReservedEventV1.model_validate(event_data)
    else:
        return InventoryFailedEventV1.model_validate(event_data)


def process_order_created_event(event_dict: dict) -> None:
    """Process a single OrderCreated event."""
    try:
        order_event = OrderCreatedEventV1.model_validate(event_dict)
    except Exception as exc:
        log_with_context(
            logger,
            "error",
            "failed to parse OrderCreated event",
            service="inventory-service",
            event_type="OrderCreated",
        )
        raise ContractValidationError(f"Invalid OrderCreated: {exc}") from exc

    order_id = order_event.order_id
    correlation_id = order_event.correlation_id
    items = order_event.payload.items

    success, reason = check_inventory(order_id, [item.model_dump() for item in items])

    result_event = build_inventory_result_event(event_dict, success, reason)

    try:
        send_event(
            get_or_create_producer(),
            topic="inventory.reserve.result",
            event=result_event.model_dump(mode="json"),
            key=order_id,
        )
    except KafkaProducerError as exc:
        log_with_context(
            logger,
            "error",
            "failed to publish inventory result",
            service="inventory-service",
            event_type=result_event.event_type,
            order_id=order_id,
            correlation_id=correlation_id,
            topic="inventory.reserve.result",
        )
        raise EventProcessingError(f"Failed to publish result: {exc}") from exc

    log_with_context(
        logger,
        "info",
        f"published {result_event.event_type}",
        service="inventory-service",
        event_type=result_event.event_type,
        order_id=order_id,
        correlation_id=correlation_id,
        topic="inventory.reserve.result",
    )


def main():
    global running

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    log_with_context(
        logger,
        "info",
        "inventory service started",
        service="inventory-service",
    )

    def callback(event: dict) -> None:
        process_order_created_event(event)

    try:
        while running:
            try:
                consume_events(
                    get_or_create_consumer(),
                    callback,
                    max_messages=None,
                )
            except KafkaConsumerError as exc:
                log_with_context(
                    logger,
                    "error",
                    "kafka consumer error",
                    service="inventory-service",
                )
                if running:
                    continue
            except ContractValidationError as exc:
                log_with_context(
                    logger,
                    "error",
                    "contract validation error",
                    service="inventory-service",
                )
                if running:
                    continue
            except EventProcessingError as exc:
                log_with_context(
                    logger,
                    "error",
                    "event processing error",
                    service="inventory-service",
                )
                if running:
                    continue
    except KeyboardInterrupt:
        pass
    finally:
        log_with_context(
            logger,
            "info",
            "inventory service stopped",
            service="inventory-service",
        )
        if consumer:
            consumer.close()
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
