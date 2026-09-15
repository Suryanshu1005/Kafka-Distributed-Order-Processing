"""Shared common utilities for all services.

This module provides:
- Configuration management (config.py)
- Structured logging (logging.py)
- Kafka producer/consumer factories (kafka.py)
- Custom exceptions (errors.py)
"""

from .config import ServiceConfig, get_config
from .logging import setup_logger, get_logger, log_with_context
from .kafka import create_producer, create_consumer, send_event, consume_events
from .errors import (
    OrderProcessingError,
    ContractValidationError,
    KafkaConnectionError,
    EventProcessingError,
    DuplicateEventError,
    TimeoutError,
    DeadLetterQueueError,
)

__all__ = [
    # Config
    "ServiceConfig",
    "get_config",
    # Logging
    "setup_logger",
    "get_logger",
    "log_with_context",
    # Kafka
    "create_producer",
    "create_consumer",
    "send_event",
    "consume_events",
    # Errors
    "OrderProcessingError",
    "ContractValidationError",
    "KafkaConnectionError",
    "EventProcessingError",
    "DuplicateEventError",
    "TimeoutError",
    "DeadLetterQueueError",
]
