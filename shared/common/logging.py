"""Structured logging setup for Kafka services.

Provides consistent logging format with standard fields for tracing and debugging.
"""

import logging
import json
import sys
from typing import Optional


class JsonFormatter(logging.Formatter):
    """Custom formatter that outputs structured JSON logs.
    
    Includes standard fields for all Kafka service logs:
    - service: Service name
    - event_type: Type of event being logged
    - order_id: Order ID (if applicable)
    - correlation_id: For tracing across services
    - message: The log message
    - level: Log level (INFO, ERROR, etc.)
    - timestamp: ISO 8601 timestamp
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON.
        
        Args:
            record: logging.LogRecord to format
            
        Returns:
            JSON string with all log fields
        """
        log_obj = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add extra fields if provided
        if hasattr(record, "service"):
            log_obj["service"] = record.service
        if hasattr(record, "event_type"):
            log_obj["event_type"] = record.event_type
        if hasattr(record, "order_id"):
            log_obj["order_id"] = record.order_id
        if hasattr(record, "correlation_id"):
            log_obj["correlation_id"] = record.correlation_id
        if hasattr(record, "topic"):
            log_obj["topic"] = record.topic
        if hasattr(record, "partition"):
            log_obj["partition"] = record.partition
        if hasattr(record, "offset"):
            log_obj["offset"] = record.offset
        
        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_obj)


class TextFormatter(logging.Formatter):
    """Simple text formatter for local development.
    
    Format: [LEVEL] service - message
    Example: [INFO] order-service - order_created order_id=ORD-000001 correlation_id=...
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as readable text.
        
        Args:
            record: logging.LogRecord to format
            
        Returns:
            Formatted text string
        """
        service = getattr(record, "service", "unknown")
        extra = []
        
        if hasattr(record, "event_type"):
            extra.append(f"event_type={record.event_type}")
        if hasattr(record, "order_id"):
            extra.append(f"order_id={record.order_id}")
        if hasattr(record, "correlation_id"):
            extra.append(f"correlation_id={record.correlation_id}")
        if hasattr(record, "topic"):
            extra.append(f"topic={record.topic}")
        if hasattr(record, "partition"):
            extra.append(f"partition={record.partition}")
        if hasattr(record, "offset"):
            extra.append(f"offset={record.offset}")
        
        extra_str = " " + " ".join(extra) if extra else ""
        
        return f"[{record.levelname}] {service}{extra_str} - {record.getMessage()}"


def setup_logger(
    service_name: str,
    level: str = "info",
    format: str = "text",
):
    """Configure the root logger with structured logging.
    
    Args:
        service_name: Name of the service (for logging context)
        level: Logging level (debug, info, warning, error) - default: info
        format: Log format (text, json) - default: text
        
    Example:
        setup_logger(
            service_name="order-service",
            level="info",
            format="text"
        )
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler with appropriate formatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level.upper())
    
    if format.lower() == "json":
        formatter = JsonFormatter()
    else:
        formatter = TextFormatter()
    
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    
    # Store service name in root logger for easy access
    root_logger.service_name = service_name


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a module.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        logging.Logger instance
        
    Example:
        logger = get_logger(__name__)
        logger.info(
            "order_created",
            extra={
                "order_id": "ORD-000001",
                "correlation_id": "corr-123",
                "service": "order-service"
            }
        )
    """
    return logging.getLogger(name)


def log_with_context(
    logger: logging.Logger,
    level: str,
    message: str,
    service: Optional[str] = None,
    event_type: Optional[str] = None,
    order_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    topic: Optional[str] = None,
    partition: Optional[int] = None,
    offset: Optional[int] = None,
    **kwargs
):
    """Log a message with standard Kafka service context fields.
    
    Args:
        logger: logging.Logger instance
        level: Log level (debug, info, warning, error)
        message: Log message
        service: Service name
        event_type: Event type being logged
        order_id: Order ID (if applicable)
        correlation_id: Correlation ID for tracing
        topic: Kafka topic name
        partition: Kafka partition number
        offset: Kafka message offset
        **kwargs: Additional fields to include in the log
        
    Example:
        log_with_context(
            logger,
            "info",
            "order_created",
            service="order-service",
            order_id="ORD-000001",
            correlation_id="corr-123"
        )
    """
    extra = {}
    if service:
        extra["service"] = service
    if event_type:
        extra["event_type"] = event_type
    if order_id:
        extra["order_id"] = order_id
    if correlation_id:
        extra["correlation_id"] = correlation_id
    if topic:
        extra["topic"] = topic
    if partition is not None:
        extra["partition"] = partition
    if offset is not None:
        extra["offset"] = offset
    
    extra.update(kwargs)
    
    log_func = getattr(logger, level.lower(), logger.info)
    log_func(message, extra=extra)
