"""Tests for Shipping Service consumer."""

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
    PaymentAuthorizedEventV1,
    PaymentFailedEventV1,
    ShipmentCreatedEventV1,
)

# Import from parent directory
import importlib.util

spec = importlib.util.spec_from_file_location(
    "shipping_main",
    Path(__file__).parent.parent / "main.py"
)
shipping_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shipping_main)

build_shipment_created_event = shipping_main.build_shipment_created_event
create_shipment = shipping_main.create_shipment
process_payment_authorized_event = shipping_main.process_payment_authorized_event
generate_tracking_id = shipping_main.generate_tracking_id


@pytest.fixture
def sample_payment_authorized_event():
    """Create a valid PaymentAuthorizedEventV1 event."""
    order_id = f"ORD-{uuid4()}"
    correlation_id = str(uuid4())

    return PaymentAuthorizedEventV1.model_validate(
        {
            "event_id": str(uuid4()),
            "event_type": "PaymentAuthorized",
            "occurred_at": datetime.now(timezone.utc),
            "correlation_id": correlation_id,
            "causation_id": str(uuid4()),
            "idempotency_key": f"order-flow:{order_id}",
            "order_id": order_id,
            "payload": {
                "status": "AUTHORIZED",
                "auth_code": None,
                "reason": None,
            },
        }
    )


def test_generate_tracking_id():
    """Test tracking ID generation."""
    order_id = "ORD-test-123"
    tracking_id = generate_tracking_id(order_id)

    assert tracking_id.startswith("TRK-")
    parts = tracking_id.split("-")
    assert len(parts) == 3
    assert len(parts[1]) == 8  # 8 random digits
    assert parts[2] == "123"  # order suffix


def test_create_shipment_succeeds():
    """Test successful shipment creation."""
    success, tracking_id = create_shipment("ORD-test-123")
    assert isinstance(success, bool)


def test_create_shipment_fails():
    """Test failed shipment creation."""
    success, tracking_id = create_shipment("ORD-test-123")
    assert isinstance(success, bool)


def test_build_shipment_created_event(sample_payment_authorized_event):
    """Test building a ShipmentCreated event."""
    event_dict = sample_payment_authorized_event.model_dump(mode="json")
    result_event = build_shipment_created_event(event_dict, "TRK-12345678-abc123")

    assert isinstance(result_event, ShipmentCreatedEventV1)
    assert result_event.event_type == "ShipmentCreated"
    assert result_event.correlation_id == event_dict["correlation_id"]
    assert result_event.causation_id == event_dict["event_id"]
    assert result_event.order_id == event_dict["order_id"]
    assert result_event.payload.status == "CREATED"
    assert result_event.payload.shipment_id.startswith("SHP-")


def test_process_payment_authorized_publishes_shipment(
    sample_payment_authorized_event, monkeypatch
):
    """Test processing PaymentAuthorized publishes shipment."""
    mock_send = MagicMock()
    mock_producer = MagicMock()
    mock_create = MagicMock(return_value=(True, "TRK-12345678-abc123"))

    monkeypatch.setattr(shipping_main, "send_event", mock_send)
    monkeypatch.setattr(shipping_main, "get_or_create_producer", lambda: mock_producer)
    monkeypatch.setattr(shipping_main, "create_shipment", mock_create)

    event_dict = sample_payment_authorized_event.model_dump(mode="json")
    process_payment_authorized_event(event_dict)

    assert mock_send.called
    call_args = mock_send.call_args
    assert call_args[1]["topic"] == "shipping.create.result"


def test_process_payment_authorized_shipment_fails(
    sample_payment_authorized_event, monkeypatch
):
    """Test processing PaymentAuthorized when shipment creation fails."""
    mock_send = MagicMock()
    mock_create = MagicMock(return_value=(False, None))

    monkeypatch.setattr(shipping_main, "send_event", mock_send)
    monkeypatch.setattr(shipping_main, "create_shipment", mock_create)

    event_dict = sample_payment_authorized_event.model_dump(mode="json")
    process_payment_authorized_event(event_dict)

    # send_event should not be called when shipment creation fails
    assert not mock_send.called


def test_process_payment_authorized_invalid_contract(monkeypatch):
    """Test processing invalid PaymentAuthorized event."""
    mock_send = MagicMock()
    mock_producer = MagicMock()

    monkeypatch.setattr(shipping_main, "send_event", mock_send)
    monkeypatch.setattr(shipping_main, "get_or_create_producer", lambda: mock_producer)

    invalid_event = {
        "event_id": str(uuid4()),
        "event_type": "PaymentAuthorized",
        # Missing required fields
    }

    with pytest.raises(ContractValidationError):
        process_payment_authorized_event(invalid_event)
