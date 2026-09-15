#!/usr/bin/env python3
"""Verify complete end-to-end event flow through all Kafka topics."""

import json

from kafka import KafkaConsumer

topics = [
    'orders.created',
    'inventory.reserve.result', 
    'payment.authorize.result',
    'shipping.create.result'
]

print('=' * 60)
print('END-TO-END EVENT FLOW VERIFICATION')
print('=' * 60)

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='e2e-verification',
    consumer_timeout_ms=5000
)

all_events = {}
for topic in topics:
    consumer.assign([])
    consumer.subscribe([topic])
    try:
        msg = next(iter(consumer))
        event = msg.value
        all_events[topic] = event
        
        order_id = event.get('order_id', 'N/A')
        event_type = event.get('event_type', 'N/A')
        status = event.get('payload', {}).get('status', 'N/A')
        correlation_id = event.get('correlation_id', 'N/A')
        
        print(f'\nOK {topic}')
        print(f'   Order ID: {order_id}')
        print(f'   Event Type: {event_type}')
        print(f'   Status: {status}')
        print(f'   Correlation ID: {correlation_id[:12]}...')
        
    except StopIteration:
        print(f'\nFAIL {topic} - No messages found')

# Verify correlation chain
print('\n' + '=' * 60)
print('CORRELATION CHAIN VERIFICATION')
print('=' * 60)

if len(all_events) == 4:
    correlations = [e.get('correlation_id') for e in all_events.values()]
    if len(set(correlations)) == 1:
        print('OK All events have SAME correlation_id')
        print(f'   Correlation ID: {correlations[0]}')
        print('\nOK END-TO-END TEST PASSED')
    else:
        print('FAIL Events have DIFFERENT correlation_ids!')
        for topic, event in all_events.items():
            print(f'   {topic}: {event.get("correlation_id")}')
else:
    print(f'FAIL Only {len(all_events)}/4 topics have events')
