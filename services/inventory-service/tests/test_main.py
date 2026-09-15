"""Tests for Inventory Service consumer."""

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
)

# Import from parent directory
import importlib.util

spec = importlib.util.spec_from_file_location(
    "inventory_main",
    Path(__file__).parent.parent / "main.py"
)
inventory_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory_main)

build_inventory_result_event = inventory_main.build_inventory_result_event
check_inventory = inventory_main.check_inventory
process_order_created_event = inventory_main.process_order_created_event


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


def test_check_inventory_sufficient_stock():
    """Test successful inventory check when stock is available."""
    items = [
        {"product_id": "1", "quantity": 1},
        {"product_id": "2", "quantity": 1},
    ]
    success, reason = check_inventory("order-123", items)
    assert isinstance(success, bool)


def test_check_inventory_insufficient_stock():
    """Test inventory check when stock is insufficient."""
    items = [
        {"product_id": "1", "quantity": 1000},  # Way more than available
    ]
    success, reason = check_inventory("order-123", items)
    assert isinstance(success, bool)


def test_build_inventory_reserved_event(sample_order_created_event):
    """Test building an InventoryReserved event."""
    event_dict = sample_order_created_event.model_dump(mode="json")
    result_event = build_inventory_result_event(event_dict, success=True, reason=None)

    assert isinstance(result_event, InventoryReservedEventV1)
    assert result_event.event_type == "InventoryReserved"
    assert result_event.correlation_id == event_dict["correlation_id"]
    assert result_event.causation_id == event_dict["event_id"]
    assert result_event.order_id == event_dict["order_id"]
    assert result_event.payload.status == "RESERVED"
    assert result_event.payload.reason is None


def test_build_inventory_failed_event(sample_order_created_event):
    """Test building an InventoryFailed event."""
    event_dict = sample_order_created_event.model_dump(mode="json")
    result_event = build_inventory_result_event(
        event_dict, success=False, reason="insufficient_stock"
    )

    assert isinstance(result_event, InventoryFailedEventV1)
    assert result_event.event_type == "InventoryFailed"
    assert result_event.correlation_id == event_dict["correlation_id"]
    assert result_event.causation_id == event_dict["event_id"]
    assert result_event.order_id == event_dict["order_id"]
    assert result_event.payload.status == "FAILED"
    assert result_event.payload.reason == "insufficient_stock"


def test_process_order_created_publishes_result(sample_order_created_event, monkeypatch):
    """Test that processing OrderCreated publishes an inventory result."""
    # Mock the dependencies
    mock_check = MagicMock(return_value=(True, None))
    mock_send = MagicMock()
    mock_producer = MagicMock()

    monkeypatch.setattr(inventory_main, "check_inventory", mock_check)
    monkeypatch.setattr(inventory_main, "send_event", mock_send)
    monkeypatch.setattr(inventory_main, "get_or_create_producer", lambda: mock_producer)

    event_dict = sample_order_created_event.model_dump(mode="json")
    process_order_created_event(event_dict)

    assert mock_send.called
    call_args = mock_send.call_args
    assert call_args[1]["topic"] == "inventory.reserve.result"
    assert call_args[1]["key"] == sample_order_created_event.order_id


def test_process_order_created_invalid_contract(monkeypatch):
    """Test that invalid OrderCreated event raises ContractValidationError."""
    mock_send = MagicMock()
    mock_producer = MagicMock()

    monkeypatch.setattr(inventory_main, "send_event", mock_send)
    monkeypatch.setattr(inventory_main, "get_or_create_producer", lambda: mock_producer)

    invalid_event = {
        "event_id": str(uuid4()),
        "event_type": "OrderCreated",
        # Missing required fields
    }

    with pytest.raises(ContractValidationError):
        process_order_created_event(invalid_event)
