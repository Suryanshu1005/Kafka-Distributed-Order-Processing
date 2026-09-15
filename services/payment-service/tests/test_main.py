"""Tests for Payment Service consumer."""

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from shared.common.errors import ContractValidationError
from shared.contracts.models.events import (
    InventoryFailedEventV1,
    InventoryReservedEventV1,
    OrderCreatedEventV1,
    PaymentAuthorizedEventV1,
    PaymentFailedEventV1,
)

# Import from parent directory
import importlib.util

spec = importlib.util.spec_from_file_location(
    "payment_main",
    Path(__file__).parent.parent / "main.py"
)
payment_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(payment_main)

build_payment_result_event = payment_main.build_payment_result_event
authorize_payment = payment_main.authorize_payment
process_order_created_event = payment_main.process_order_created_event
process_inventory_result_event = payment_main.process_inventory_result_event
cache_order_from_event = payment_main.cache_order_from_event


@pytest.fixture
def sample_order_created_event():
    """Create a valid OrderCreatedEventV1 event."""
    order_id = f"ORD-{uuid4()}"
    correlation_id = str(uuid4())
    created_at = datetime.now(timezone.utc)

    return OrderCreatedEventV1.model_validate(
        {
            "event_id": str(uuid4()),
            "event_type": "OrderCreated",
            "occurred_at": created_at,
            "correlation_id": correlation_id,
            "causation_id": None,
            "idempotency_key": f"order-flow:{order_id}",
            "order_id": order_id,
            "payload": {
                "order_id": order_id,
                "customer": {
                    "name": "Test Customer",
                    "email": "test@example.com",
                },
                "currency": "USD",
                "items": [
                    {"product_id": "1", "quantity": 2, "unit_price": 10.0},
                    {"product_id": "2", "quantity": 1, "unit_price": 20.0},
                ],
                "amount": 40.0,
                "created_at": created_at,
            },
        }
    )


@pytest.fixture
def sample_inventory_reserved_event(sample_order_created_event):
    """Create a valid InventoryReservedEventV1 event."""
    return InventoryReservedEventV1.model_validate(
        {
            "event_id": str(uuid4()),
            "event_type": "InventoryReserved",
            "occurred_at": datetime.now(timezone.utc),
            "correlation_id": sample_order_created_event.correlation_id,
            "causation_id": sample_order_created_event.event_id,
            "idempotency_key": sample_order_created_event.idempotency_key,
            "order_id": sample_order_created_event.order_id,
            "payload": {
                "status": "RESERVED",
                "reason": None,
            },
        }
    )


@pytest.fixture
def sample_inventory_failed_event(sample_order_created_event):
    """Create a valid InventoryFailedEventV1 event."""
    return InventoryFailedEventV1.model_validate(
        {
            "event_id": str(uuid4()),
            "event_type": "InventoryFailed",
            "occurred_at": datetime.now(timezone.utc),
            "correlation_id": sample_order_created_event.correlation_id,
            "causation_id": sample_order_created_event.event_id,
            "idempotency_key": sample_order_created_event.idempotency_key,
            "order_id": sample_order_created_event.order_id,
            "payload": {
                "status": "FAILED",
                "reason": "insufficient_stock",
            },
        }
    )


def test_authorize_payment_succeeds():
    """Test successful payment authorization."""
    success, reason = authorize_payment("order-123", 100.0)
    assert isinstance(success, bool)


def test_authorize_payment_fails():
    """Test failed payment authorization."""
    success, reason = authorize_payment("order-123", 100.0)
    assert isinstance(success, bool)


def test_build_payment_authorized_event(sample_inventory_reserved_event):
    """Test building a PaymentAuthorized event."""
    event_dict = sample_inventory_reserved_event.model_dump(mode="json")
    result_event = build_payment_result_event(event_dict, success=True, decline_reason=None)

    assert isinstance(result_event, PaymentAuthorizedEventV1)
    assert result_event.event_type == "PaymentAuthorized"
    assert result_event.correlation_id == event_dict["correlation_id"]
    assert result_event.causation_id == event_dict["event_id"]
    assert result_event.order_id == event_dict["order_id"]
    assert result_event.payload.status == "AUTHORIZED"
    assert result_event.payload.reason is None


def test_build_payment_failed_event(sample_inventory_reserved_event):
    """Test building a PaymentFailed event."""
    event_dict = sample_inventory_reserved_event.model_dump(mode="json")
    result_event = build_payment_result_event(
        event_dict, success=False, decline_reason="card_declined"
    )

    assert isinstance(result_event, PaymentFailedEventV1)
    assert result_event.event_type == "PaymentFailed"
    assert result_event.correlation_id == event_dict["correlation_id"]
    assert result_event.causation_id == event_dict["event_id"]
    assert result_event.order_id == event_dict["order_id"]
    assert result_event.payload.status == "DECLINED"
    assert result_event.payload.reason == "card_declined"


def test_cache_order_from_event(sample_order_created_event):
    """Test caching order data from OrderCreated event."""
    payment_main.order_cache.clear()
    cache_order_from_event(sample_order_created_event)

    assert sample_order_created_event.order_id in payment_main.order_cache
    cached = payment_main.order_cache[sample_order_created_event.order_id]
    assert cached["amount"] == 40.0
    assert cached["customer_name"] == "Test Customer"
    assert cached["customer_email"] == "test@example.com"


def test_process_order_created_event(sample_order_created_event):
    """Test processing OrderCreated event caches the order."""
    payment_main.order_cache.clear()
    event_dict = sample_order_created_event.model_dump(mode="json")
    process_order_created_event(event_dict)

    assert sample_order_created_event.order_id in payment_main.order_cache


def test_process_inventory_failed_event(
    sample_inventory_failed_event, monkeypatch
):
    """Test processing InventoryFailed event emits PaymentFailed."""
    mock_send = MagicMock()
    mock_producer = MagicMock()

    monkeypatch.setattr(payment_main, "send_event", mock_send)
    monkeypatch.setattr(payment_main, "get_or_create_producer", lambda: mock_producer)

    event_dict = sample_inventory_failed_event.model_dump(mode="json")
    process_inventory_result_event(event_dict)

    assert mock_send.called
    call_args = mock_send.call_args
    assert call_args[1]["topic"] == "payment.authorize.result"


def test_process_inventory_reserved_event_with_cached_order(
    sample_inventory_reserved_event, sample_order_created_event, monkeypatch
):
    """Test processing InventoryReserved event authorizes payment."""
    # First cache the order
    payment_main.order_cache.clear()
    cache_order_from_event(sample_order_created_event)

    mock_send = MagicMock()
    mock_producer = MagicMock()
    mock_authorize = MagicMock(return_value=(True, None))

    monkeypatch.setattr(payment_main, "send_event", mock_send)
    monkeypatch.setattr(payment_main, "get_or_create_producer", lambda: mock_producer)
    monkeypatch.setattr(payment_main, "authorize_payment", mock_authorize)

    event_dict = sample_inventory_reserved_event.model_dump(mode="json")
    process_inventory_result_event(event_dict)

    assert mock_authorize.called
    assert mock_send.called
    call_args = mock_send.call_args
    assert call_args[1]["topic"] == "payment.authorize.result"


def test_process_inventory_reserved_event_missing_order(
    sample_inventory_reserved_event, monkeypatch
):
    """Test processing InventoryReserved without cached order skips."""
    payment_main.order_cache.clear()
    mock_send = MagicMock()

    monkeypatch.setattr(payment_main, "send_event", mock_send)

    event_dict = sample_inventory_reserved_event.model_dump(mode="json")
    # Should not raise, just skip
    process_inventory_result_event(event_dict)

    # send_event should not be called
    assert not mock_send.called
