"""Kafka producer and consumer helpers for services.

Provides factory functions for creating configured producer and consumer instances.
Handles connection details, serialization, and error handling.
"""

import json
from typing import Optional, Callable, Any


class KafkaProducerError(Exception):
    """Raised when Kafka producer fails to send a message."""
    pass


class KafkaConsumerError(Exception):
    """Raised when Kafka consumer fails to consume from a topic."""
    pass


def create_producer(config: Any):
    """Create and configure a Kafka producer.
    
    Args:
        config: ServiceConfig instance with kafka settings
        
    Returns:
        Configured Kafka producer instance
        
    Raises:
        ImportError: If kafka-python or confluent-kafka not installed
        KafkaProducerError: If producer initialization fails
        
    The producer is configured to:
    - Use JSON serialization for values
    - Enable compression (snappy)
    - Wait for broker acknowledgment (acks=all)
    - Retry up to 3 times on failure
    
    Example:
        producer = create_producer(config)
        producer.send("orders.created", value=event_dict)
        producer.flush()
    """
    try:
        # Try confluent-kafka first (preferred)
        from confluent_kafka import Producer as ConfluentProducer
        
        conf = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'client.id': config.kafka_client_id,
            'security.protocol': config.kafka_security_protocol.lower(),
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'compression.type': 'none',  # Disable compression for KRaft
        }
        
        return ConfluentProducer(conf)
    except ImportError:
        pass
    
    try:
        # Fallback to kafka-python
        from kafka import KafkaProducer
        
        producer = KafkaProducer(
            bootstrap_servers=config.kafka_bootstrap_servers,
            client_id=config.kafka_client_id,
            key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            compression_type=None,
            enable_idempotence=False,  # Disable idempotency for KRaft compatibility
        )
        return producer
    except ImportError as e:
        raise ImportError(
            "Neither confluent-kafka nor kafka-python is installed. "
            "Install with: pip install confluent-kafka or pip install kafka-python"
        ) from e


def create_consumer(
    config: Any,
    topics: list[str],
    from_beginning: bool = True,
    consumer_group: Optional[str] = None,
):
    """Create and configure a Kafka consumer.
    
    Args:
        config: ServiceConfig instance with kafka settings
        topics: List of topic names to consume from
        from_beginning: Start from earliest offset (default: True)
        consumer_group: Override the default consumer group name (optional)
        
    Returns:
        Configured Kafka consumer instance
        
    Raises:
        ImportError: If kafka-python or confluent-kafka not installed
        KafkaConsumerError: If consumer initialization fails
        
    The consumer is configured to:
    - Use auto.offset.reset=earliest to start from beginning
    - Enable auto-commit of offsets
    - Set session timeout for rebalancing
    - Use JSON deserialization for values
    
    Example:
        consumer = create_consumer(config, topics=['orders.created'], consumer_group='inventory-service.v1')
        for msg in consumer:
            event = msg.value()
            process_event(event)
            consumer.commit()
    """
    try:
        # Try confluent-kafka first (preferred)
        from confluent_kafka import Consumer as ConfluentConsumer
        
        group_id = consumer_group or config.kafka_consumer_group
        conf = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'group.id': group_id,
            'client.id': config.kafka_client_id,
            'security.protocol': config.kafka_security_protocol.lower(),
            'auto.offset.reset': 'earliest' if from_beginning else 'latest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
        }
        
        consumer = ConfluentConsumer(conf)
        consumer.subscribe(topics)
        return consumer
    except ImportError:
        pass
    
    try:
        # Fallback to kafka-python
        from kafka import KafkaConsumer
        
        group_id = consumer_group or config.kafka_consumer_group
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id=group_id,
            client_id=config.kafka_client_id,
            auto_offset_reset='earliest' if from_beginning else 'latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            session_timeout_ms=30000,
        )
        return consumer
    except ImportError as e:
        raise ImportError(
            "Neither confluent-kafka nor kafka-python is installed. "
            "Install with: pip install confluent-kafka or pip install kafka-python"
        ) from e


def send_event(
    producer: Any,
    topic: str,
    event: dict,
    key: Optional[str] = None,
) -> bool:
    """Send an event to a Kafka topic.
    
    Args:
        producer: Kafka producer instance
        topic: Topic name
        event: Event dict to send
        key: Message key for partitioning (optional)
        
    Returns:
        True if send succeeded, False otherwise
        
    Example:
        success = send_event(
            producer,
            topic="orders.created",
            event={"order_id": "ORD-001", ...},
            key="ORD-001"  # Use order_id as partition key
        )
    """
    try:
        # Check producer type and send accordingly
        if hasattr(producer, 'produce'):  # confluent-kafka
            producer.produce(
                topic=topic,
                key=key.encode() if key else None,
                value=json.dumps(event).encode() if isinstance(event, dict) else event,
            )
            producer.flush()
            return True
        else:  # kafka-python or other
            try:
                future = producer.send(
                    topic,
                    value=event,
                    key=key,
                )
                record_metadata = future.get(timeout=10)
                return bool(record_metadata)
            except Exception as e:
                import traceback
                print(f"[SEND_EVENT ERROR] Topic={topic}, Key={key}, Error={type(e).__name__}: {e}")
                print(f"[SEND_EVENT ERROR] Traceback: {traceback.format_exc()}")
                raise
    except Exception as e:
        raise KafkaProducerError(f"Failed to send event to {topic}: {e}") from e


def consume_events(
    consumer: Any,
    callback: Callable[[dict], None],
    max_messages: Optional[int] = None,
) -> int:
    """Consume events from Kafka and process with callback.
    
    Args:
        consumer: Kafka consumer instance
        callback: Function to call for each event
        max_messages: Maximum messages to consume (None for infinite)
        
    Returns:
        Number of messages processed
        
    Example:
        def process_event(event):
            print(f"Received: {event}")
        
        consumed = consume_events(consumer, process_event, max_messages=100)
        print(f"Processed {consumed} messages")
    """
    count = 0
    try:
        while True:
            if max_messages and count >= max_messages:
                break
            
            # kafka-python consumer - iterate directly
            try:
                msg = next(consumer)
            except StopIteration:
                break
            
            # Extract value - msg is a ConsumerRecord, msg.value is bytes
            if isinstance(msg.value, bytes):
                event = json.loads(msg.value.decode('utf-8'))
            else:
                event = msg.value
            
            callback(event)
            count += 1
    except StopIteration:
        pass
    except KeyboardInterrupt:
        pass
    
    return count
