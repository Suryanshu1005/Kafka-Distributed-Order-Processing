#!/usr/bin/env python3
"""
End-to-end integration test for Phase 1 MVP.
Tests the full order processing pipeline: Order → Inventory → Payment → Shipping
"""

import json
import time
from datetime import datetime, timezone
from uuid import uuid4

import requests
from kafka import KafkaConsumer, TopicPartition


def test_order_flow():
    """Send an order and verify it flows through all services."""

    # 1. Create an order via HTTP API
    print("📦 Step 1: Creating order via HTTP API...")
    order_request = {
        "customer_id": "test-customer-001",
        "customer_email": "test@example.com",
        "items": [
            {"product_id": "1", "quantity": 2, "unit_price": 10.0},
            {"product_id": "2", "quantity": 1, "unit_price": 20.0},
        ],
        "shipping_address": {
            "street": "123 Main St",
            "city": "San Francisco",
            "state": "CA",
            "postal_code": "94105",
        },
    }

    try:
        response = requests.post(
            "http://localhost:8000/orders",
            json=order_request,
            timeout=5,
        )
        if response.status_code != 201:
            print(f"❌ Order creation failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False

        order_response = response.json()
        order_id = order_response["order_id"]
        correlation_id = order_response["correlation_id"]
        print(f"✅ Order created: {order_id}")
        print(f"   Correlation ID: {correlation_id}")
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to Order Service at http://localhost:8000")
        print("   Make sure Order Service is running: python -m uvicorn main:app --port 8000 (in services/order-service)")
        return False
    except Exception as e:
        print(f"❌ Error creating order: {e}")
        return False

    # 2. Consume events from Kafka topics
    print("\n📨 Step 2: Waiting for events to flow through topics...")
    time.sleep(2)  # Wait for events to be processed

    topics = [
        "orders.created",
        "inventory.reserve.result",
        "payment.authorize.result",
        "shipping.create.result",
    ]

    events_by_topic = {}

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,
        )

        for topic in topics:
            consumer.assign([TopicPartition(topic, 0)])
            consumer.seek_to_end(TopicPartition(topic, 0))
            offset = consumer.position(TopicPartition(topic, 0))
            if offset > 0:
                consumer.seek(TopicPartition(topic, 0), max(0, offset - 10))

        events = {}
        for msg in consumer:
            event_data = msg.value
            topic = msg.topic
            if topic not in events_by_topic:
                events_by_topic[topic] = []
            events_by_topic[topic].append(event_data)

            if event_data.get("order_id") == order_id:
                print(f"✅ {topic}: {event_data.get('event_type')}")
                if event_data.get("correlation_id") != correlation_id:
                    print(
                        f"   ⚠️ Correlation ID mismatch: expected {correlation_id}, got {event_data.get('correlation_id')}"
                    )
                else:
                    print(f"   ✅ Correlation ID matches")

        consumer.close()

    except Exception as e:
        print(f"❌ Error consuming events: {e}")
        return False

    # 3. Verify event flow
    print("\n✅ Summary:")
    for topic in topics:
        count = len([e for e in events_by_topic.get(topic, []) if e.get("order_id") == order_id])
        print(f"   {topic}: {count} events")

    # Expected: 1 OrderCreated, 1 InventoryReserved/Failed, 1 PaymentAuthorized/Failed, 1 ShipmentCreated
    expected_topics = 4
    actual_topics = sum(1 for topic in topics if len([e for e in events_by_topic.get(topic, []) if e.get("order_id") == order_id]) > 0)

    if actual_topics == expected_topics:
        print(f"\n🎉 End-to-end test PASSED: All {expected_topics} services responded!")
        return True
    else:
        print(f"\n⚠️ End-to-end test INCOMPLETE: Only {actual_topics}/{expected_topics} services responded")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Phase 1 MVP: End-to-End Integration Test")
    print("=" * 60)
    print("\nPrerequisites:")
    print("1. Kafka broker running at localhost:9092")
    print("2. Order Service running at http://localhost:8000")
    print("3. Inventory, Payment, Shipping services running and consuming")
    print()

    success = test_order_flow()
    exit(0 if success else 1)
