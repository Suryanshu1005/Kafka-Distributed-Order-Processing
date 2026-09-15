from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from shared.contracts.models.events import OrderCreatedEventV1


def valid_order_event():
    return {
        "event_id": "evt-1",
        "event_type": "OrderCreated",
        "occurred_at": datetime.now(timezone.utc),
        "correlation_id": "corr-1",
        "causation_id": None,
        "idempotency_key": "OrderCreated:ORD-000001",
        "order_id": "ORD-000001",
        "payload": {
            "order_id": "ORD-000001",
            "customer": {
                "name": "Emily Johnson",
                "email": "emily@example.com",
            },
            "currency": "USD",
            "items": [
                {
                    "product_id": "1",
                    "quantity": 2,
                    "unit_price": 79.99,
                }
            ],
            "amount": 159.98,
            "created_at": datetime.now(timezone.utc),
        },
    }


def test_valid_order_created_event():
    event = OrderCreatedEventV1.model_validate(valid_order_event())

    assert event.event_type == "OrderCreated"
    assert event.payload.items[0].product_id == "1"


def test_order_created_rejects_extra_customer_fields():
    data = valid_order_event()
    data["payload"]["customer"]["id"] = 1

    with pytest.raises(ValidationError):
        OrderCreatedEventV1.model_validate(data)


def test_order_created_rejects_extra_item_fields():
    data = valid_order_event()
    data["payload"]["items"][0]["product_name"] = "Wireless Keyboard"

    with pytest.raises(ValidationError):
        OrderCreatedEventV1.model_validate(data)


def test_order_created_rejects_wrong_event_type():
    data = valid_order_event()
    data["event_type"] = "order.created"

    with pytest.raises(ValidationError):
        OrderCreatedEventV1.model_validate(data)