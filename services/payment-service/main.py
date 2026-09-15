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
    PaymentAuthorizedEventV1,
    PaymentFailedEventV1,
)

config = get_config()
setup_logger("payment-service", config.log_level)
logger = get_logger(__name__)

consumer = None
producer = None
running = True

# In-memory cache of orders by order_id
# In production, this would be a persistent cache or saga store
order_cache = {}

DECLINE_REASONS = [
    "insufficient_funds",
    "card_declined",
    "authentication_failed",
    "processor_error",
]


def handle_shutdown(sig, frame):
    global running
    log_with_context(
        logger, "info", "shutdown signal received", service="payment-service"
    )
    running = False


def get_or_create_consumer():
    global consumer
    if consumer is None:
        # Consume from both topics: orders and inventory results
        consumer = create_consumer(
            config,
            topics=["orders.created", "inventory.reserve.result"],
            from_beginning=True,
            consumer_group="payment-service.v1",
        )
    return consumer


def get_or_create_producer():
    global producer
    if producer is None:
        producer = create_producer(config)
    return producer


def should_fail_payment() -> bool:
    return random.random() < config.payment_fail_rate


def cache_order_from_event(order_event: OrderCreatedEventV1) -> None:
    """Cache order data for later payment processing."""
    order_cache[order_event.order_id] = {
        "customer_name": order_event.payload.customer.name,
        "customer_email": order_event.payload.customer.email,
        "amount": order_event.payload.amount,
        "currency": order_event.payload.currency,
        "items": [item.model_dump() for item in order_event.payload.items],
    }


def authorize_payment(order_id: str, amount: float) -> tuple[bool, str | None]:
    """Simulate payment authorization.

    Returns:
        (success: bool, decline_reason: str | None)
    """
    if should_fail_payment():
        return False, random.choice(DECLINE_REASONS)
    return True, None


def build_payment_result_event(
    inventory_event: dict, success: bool, decline_reason: str | None
) -> PaymentAuthorizedEventV1 | PaymentFailedEventV1:
    """Build a typed payment result event."""
    status = "AUTHORIZED" if success else "DECLINED"

    payload = {
        "status": status,
        "auth_code": None,
        "reason": decline_reason,
    }

    event_data = {
        "event_id": str(uuid4()),
        "event_type": "PaymentAuthorized" if success else "PaymentFailed",
        "occurred_at": datetime.now(timezone.utc),
        "correlation_id": inventory_event["correlation_id"],
        "causation_id": inventory_event["event_id"],
        "idempotency_key": inventory_event["idempotency_key"],
        "order_id": inventory_event["order_id"],
        "payload": payload,
    }

    if success:
        return PaymentAuthorizedEventV1.model_validate(event_data)
    else:
        return PaymentFailedEventV1.model_validate(event_data)


def process_order_created_event(event_dict: dict) -> None:
    """Cache order data from OrderCreated event."""
    try:
        order_event = OrderCreatedEventV1.model_validate(event_dict)
        cache_order_from_event(order_event)
        log_with_context(
            logger,
            "info",
            "cached order data",
            service="payment-service",
            order_id=order_event.order_id,
            amount=order_event.payload.amount,
        )
    except Exception as exc:
        log_with_context(
            logger,
            "error",
            "failed to parse OrderCreated event",
            service="payment-service",
        )
        raise ContractValidationError(f"Invalid OrderCreated: {exc}") from exc


def process_inventory_result_event(event_dict: dict) -> None:
    """Process inventory result and authorize payment if inventory succeeded."""
    try:
        # Try to parse as either InventoryReserved or InventoryFailed
        event_type = event_dict.get("event_type")
        if event_type == "InventoryReserved":
            inventory_event = InventoryReservedEventV1.model_validate(event_dict)
        elif event_type == "InventoryFailed":
            inventory_event = InventoryFailedEventV1.model_validate(event_dict)
        else:
            raise ContractValidationError(
                f"Unknown event type: {event_type}. Expected InventoryReserved or InventoryFailed."
            )
    except Exception as exc:
        log_with_context(
            logger,
            "error",
            "failed to parse inventory result event",
            service="payment-service",
        )
        raise ContractValidationError(f"Invalid inventory result: {exc}") from exc

    order_id = inventory_event.order_id
    correlation_id = inventory_event.correlation_id

    # If inventory failed, emit payment failed immediately
    if inventory_event.payload.status == "FAILED":
        result_event = build_payment_result_event(
            event_dict, success=False, decline_reason="inventory_failed"
        )
    else:
        # Inventory succeeded, authorize payment
        order_data = order_cache.get(order_id)
        if not order_data:
            log_with_context(
                logger,
                "warning",
                "order not in cache, skipping payment",
                service="payment-service",
                order_id=order_id,
                correlation_id=correlation_id,
            )
            return

        amount = order_data["amount"]
        success, decline_reason = authorize_payment(order_id, amount)
        result_event = build_payment_result_event(event_dict, success, decline_reason)

    try:
        send_event(
            get_or_create_producer(),
            topic="payment.authorize.result",
            event=result_event.model_dump(mode="json"),
            key=order_id,
        )
    except KafkaProducerError as exc:
        log_with_context(
            logger,
            "error",
            "failed to publish payment result",
            service="payment-service",
            event_type=result_event.event_type,
            order_id=order_id,
            correlation_id=correlation_id,
            topic="payment.authorize.result",
        )
        raise EventProcessingError(f"Failed to publish result: {exc}") from exc

    log_with_context(
        logger,
        "info",
        f"published {result_event.event_type}",
        service="payment-service",
        event_type=result_event.event_type,
        order_id=order_id,
        correlation_id=correlation_id,
        topic="payment.authorize.result",
    )


def callback(event: dict) -> None:
    """Route event to appropriate handler based on event type."""
    event_type = event.get("event_type")

    if event_type == "OrderCreated":
        process_order_created_event(event)
    elif event_type in ("InventoryReserved", "InventoryFailed"):
        process_inventory_result_event(event)
    else:
        log_with_context(
            logger,
            "warning",
            f"unknown event type: {event_type}",
            service="payment-service",
        )


def main():
    global running

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    log_with_context(
        logger,
        "info",
        "payment service started",
        service="payment-service",
    )

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
                    service="payment-service",
                )
                if running:
                    continue
            except ContractValidationError as exc:
                log_with_context(
                    logger,
                    "error",
                    "contract validation error",
                    service="payment-service",
                )
                if running:
                    continue
            except EventProcessingError as exc:
                log_with_context(
                    logger,
                    "error",
                    "event processing error",
                    service="payment-service",
                )
                if running:
                    continue
    except KeyboardInterrupt:
        pass
    finally:
        log_with_context(
            logger,
            "info",
            "payment service stopped",
            service="payment-service",
        )
        if consumer:
            consumer.close()
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
