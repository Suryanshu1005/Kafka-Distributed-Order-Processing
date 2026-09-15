"""Custom exceptions for Kafka Distributed Order Processing system.

Defines domain-specific exceptions for different failure scenarios.
"""


class OrderProcessingError(Exception):
    """Base exception for order processing failures."""
    pass


class ContractValidationError(OrderProcessingError):
    """Raised when event fails contract validation.
    
    This indicates a schema or structure mismatch with EventEnvelopeV1.
    Events that fail validation should be sent to the DLQ.
    
    Example:
        raise ContractValidationError(
            f"Missing required field: order_id in {event}"
        )
    """
    pass


class KafkaConnectionError(OrderProcessingError):
    """Raised when Kafka connection fails.
    
    This indicates a network or broker connectivity issue.
    Services should retry after backoff.
    
    Example:
        raise KafkaConnectionError(
            f"Failed to connect to {bootstrap_servers}: connection refused"
        )
    """
    pass


class EventProcessingError(OrderProcessingError):
    """Raised when event processing fails.
    
    This indicates a business logic failure (e.g., inventory unavailable,
    payment declined, shipment creation failed).
    
    Example:
        raise EventProcessingError(
            f"Inventory reservation failed for order {order_id}: insufficient stock"
        )
    """
    pass


class DuplicateEventError(OrderProcessingError):
    """Raised when a duplicate event is detected.
    
    This indicates the same idempotency_key has been seen before.
    Services should skip processing but acknowledge the event.
    
    Example:
        raise DuplicateEventError(
            f"Event already processed: idempotency_key={key}"
        )
    """
    pass


class TimeoutError(OrderProcessingError):
    """Raised when an operation times out.
    
    This indicates a service took too long to respond.
    
    Example:
        raise TimeoutError(f"Payment service response timeout after {timeout}s")
    """
    pass


class DeadLetterQueueError(OrderProcessingError):
    """Raised when an event needs to be sent to DLQ.
    
    This is used by consumers to mark messages for dead-letter queue routing.
    
    Example:
        raise DeadLetterQueueError(
            f"Message could not be parsed, sending to DLQ: {error}"
        )
    """
    pass
