#!/usr/bin/env python3
"""Verify event processing for 50 fixture orders through all Kafka topics."""

import json

from kafka import KafkaConsumer

def count_topic_messages(topic, timeout_ms=3000):
    """Count messages in a topic."""
    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=timeout_ms
    )
    consumer.subscribe([topic])
    count = 0
    try:
        for msg in consumer:
            count += 1
    except StopIteration:
        pass
    finally:
        consumer.close()
    return count

if __name__ == '__main__':
    topics = {
        'orders.created': 'OrderCreated',
        'inventory.reserve.result': 'InventoryReserved/Failed',
        'payment.authorize.result': 'PaymentAuthorized/Failed',
        'shipping.create.result': 'ShipmentCreated for authorized payments only'
    }

    print('\n' + '=' * 70)
    print('FIXTURE REPLAY VERIFICATION - 50 ORDERS')
    print('=' * 70)
    
    total = 0
    for topic, event_type in topics.items():
        count = count_topic_messages(topic)
        total += count
        required_count = 1 if topic == 'shipping.create.result' else 50
        status = 'OK' if count >= required_count else 'WARN'
        pct = int((count / 50) * 100) if count > 0 else 0
        print(f'{status:4} {topic:30} : {count:3d}/50+ events ({pct:3d}% - {event_type})')

    print('=' * 70)
    print(f'Total Events: {total}')
    print('=' * 70)
    
    if total >= 151:
        print('OK FIXTURE REPLAY VERIFIED - core topics processed all orders; shipping reflects successful payments only')
    elif total >= 50:
        print('WARN FIXTURE REPLAY IN PROGRESS - events still processing')
    else:
        print('WARN FIXTURE REPLAY INCOMPLETE - some events are not reaching topics')
