"""Configuration management for all services.

Loads configuration from environment variables with sensible defaults.
Uses pydantic for validation and type coercion.
"""

import os
from typing import Optional


class ServiceConfig:
    """Service configuration loaded from environment variables.
    
    Attributes:
        app_env: Deployment environment (local, dev, prod)
        log_level: Logging verbosity (debug, info, warning, error)
        kafka_bootstrap_servers: Kafka broker addresses (comma-separated)
        kafka_client_id: Kafka client identifier
        kafka_consumer_group: Consumer group name (for consumers only)
        kafka_security_protocol: Security protocol (plaintext, ssl, sasl_ssl)
    """
    
    def __init__(self):
        self.app_env = os.getenv("APP_ENV", "local")
        self.log_level = os.getenv("LOG_LEVEL", "info").upper()
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_client_id = os.getenv("KAFKA_CLIENT_ID", "unknown-service")
        self.kafka_consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "unknown-group")
        self.kafka_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        
        # Service-specific failure injection (for mock services)
        self.inventory_fail_rate = float(os.getenv("INVENTORY_FAIL_RATE", "0.1"))
        self.payment_fail_rate = float(os.getenv("PAYMENT_FAIL_RATE", "0.15"))
        self.shipping_fail_rate = float(os.getenv("SHIPPING_FAIL_RATE", "0.05"))
    
    def __repr__(self):
        return (
            f"ServiceConfig("
            f"app_env={self.app_env}, "
            f"log_level={self.log_level}, "
            f"kafka_bootstrap_servers={self.kafka_bootstrap_servers}, "
            f"kafka_client_id={self.kafka_client_id}, "
            f"kafka_consumer_group={self.kafka_consumer_group}"
            f")"
        )


def get_config() -> ServiceConfig:
    """Get or create the global service configuration.
    
    Returns:
        ServiceConfig instance
        
    Example:
        config = get_config()
        print(config.kafka_bootstrap_servers)  # "localhost:9092"
    """
    return ServiceConfig()
