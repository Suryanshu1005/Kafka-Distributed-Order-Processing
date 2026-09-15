import random
import signal
from datetime import datetime, timedelta, timezone
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
    PaymentAuthorizedEventV1,
    PaymentFailedEventV1,
    ShipmentCreatedEventV1,
)

config = get_config()
setup_logger("shipping-service", config.log_level)
logger = get_logger(__name__)

consumer = None
producer = None
running = True


def handle_shutdown(sig, frame):
    global running
    log_with_context(
        logger, "info", "shutdown signal received", service="shipping-service"
    )
    running = False


def get_or_create_consumer():
    global consumer
    if consumer is None:
        consumer = create_consumer(
            config,
            topics=["payment.authorize.result"],
            from_beginning=True,
            consumer_group="shipping-service.v1",
        )
    return consumer


def get_or_create_producer():
    global producer
    if producer is None:
        producer = create_producer(config)
    return producer


def should_fail_shipment() -> bool:
    return random.random() < config.shipping_fail_rate


def generate_tracking_id(order_id: str) -> str:
    """Generate a mock tracking ID."""
    random_part = "".join([str(random.randint(0, 9)) for _ in range(8)])
    order_suffix = order_id.split("-")[-1][:6]
    return f"TRK-{random_part}-{order_suffix}"


def calculate_estimated_delivery() -> str:
    """Calculate estimated delivery date (5-7 business days from now)."""
    # For MVP, just add 5-7 days to current date (ignoring business days)
    days_to_add = random.randint(5, 7)
    delivery_date = datetime.now(timezone.utc) + timedelta(days=days_to_add)
    return delivery_date.isoformat()


def create_shipment(order_id: str) -> tuple[bool, str | None]:
    """Simulate shipment creation.

    Returns:
        (success: bool, tracking_id: str | None)
    """
    if should_fail_shipment():
        return False, None
    
    tracking_id = generate_tracking_id(order_id)
    return True, tracking_id


def build_shipment_created_event(
    payment_event: dict, tracking_id: str
) -> ShipmentCreatedEventV1:
    """Build a ShipmentCreatedEventV1."""
    shipment_id = f"SHP-{uuid4()}"
    estimated_delivery = calculate_estimated_delivery()

    payload = {
        "status": "CREATED",
        "shipment_id": shipment_id,
    }

    event_data = {
        "event_id": str(uuid4()),
        "event_type": "ShipmentCreated",
        "occurred_at": datetime.now(timezone.utc),
        "correlation_id": payment_event["correlation_id"],
        "causation_id": payment_event["event_id"],
        "idempotency_key": payment_event["idempotency_key"],
        "order_id": payment_event["order_id"],
        "payload": payload,
    }

    return ShipmentCreatedEventV1.model_validate(event_data)


def process_payment_authorized_event(event_dict: dict) -> None:
    """Process PaymentAuthorized event and create shipment."""
    try:
        payment_event = PaymentAuthorizedEventV1.model_validate(event_dict)
    except Exception as exc:
        log_with_context(
            logger,
            "error",
            "failed to parse PaymentAuthorized event",
            service="shipping-service",
        )
        raise ContractValidationError(f"Invalid PaymentAuthorized: {exc}") from exc

    order_id = payment_event.order_id
    correlation_id = payment_event.correlation_id

    success, tracking_id = create_shipment(order_id)

    if not success:
        log_with_context(
            logger,
            "warning",
            "shipment creation failed",
            service="shipping-service",
            order_id=order_id,
            correlation_id=correlation_id,
        )
        # In a full saga, this would trigger an OrderFailed event
        # For Phase 1 MVP, we just skip shipping for failed orders
        return

    shipment_event = build_shipment_created_event(event_dict, tracking_id)

    try:
        send_event(
            get_or_create_producer(),
            topic="shipping.create.result",
            event=shipment_event.model_dump(mode="json"),
            key=order_id,
        )
    except KafkaProducerError as exc:
        log_with_context(
            logger,
            "error",
            "failed to publish shipment event",
            service="shipping-service",
            event_type=shipment_event.event_type,
            order_id=order_id,
            correlation_id=correlation_id,
            topic="shipping.create.result",
        )
        raise EventProcessingError(f"Failed to publish shipment: {exc}") from exc

    log_with_context(
        logger,
        "info",
        "published ShipmentCreated",
        service="shipping-service",
        order_id=order_id,
        correlation_id=correlation_id,
        shipment_id=shipment_event.payload.shipment_id,
        topic="shipping.create.result",
    )


def callback(event: dict) -> None:
    """Route event based on event type."""
    event_type = event.get("event_type")

    if event_type == "PaymentAuthorized":
        process_payment_authorized_event(event)
    elif event_type == "PaymentFailed":
        log_with_context(
            logger,
            "info",
            "skipping failed payment, no shipment",
            service="shipping-service",
            order_id=event.get("order_id"),
        )
    else:
        log_with_context(
            logger,
            "warning",
            f"unknown event type: {event_type}",
            service="shipping-service",
        )


def main():
    global running

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    log_with_context(
        logger,
        "info",
        "shipping service started",
        service="shipping-service",
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
                    service="shipping-service",
                )
                if running:
                    continue
            except ContractValidationError as exc:
                log_with_context(
                    logger,
                    "error",
                    "contract validation error",
                    service="shipping-service",
                )
                if running:
                    continue
            except EventProcessingError as exc:
                log_with_context(
                    logger,
                    "error",
                    "event processing error",
                    service="shipping-service",
                )
                if running:
                    continue
    except KeyboardInterrupt:
        pass
    finally:
        log_with_context(
            logger,
            "info",
            "shipping service stopped",
            service="shipping-service",
        )
        if consumer:
            consumer.close()
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
