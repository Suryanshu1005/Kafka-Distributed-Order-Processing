import json
from pathlib import Path

from shared.contracts.models.events import (
    InventoryFailedEventV1,
    InventoryReservedEventV1,
    OrderCompletedEventV1,
    OrderCreatedEventV1,
    OrderFailedEventV1,
    PaymentAuthorizedEventV1,
    PaymentFailedEventV1,
    ShipmentCreatedEventV1,
)


EVENT_MODELS = {
    "OrderCreated": OrderCreatedEventV1,
    "InventoryReserved": InventoryReservedEventV1,
    "InventoryFailed": InventoryFailedEventV1,
    "PaymentAuthorized": PaymentAuthorizedEventV1,
    "PaymentFailed": PaymentFailedEventV1,
    "ShipmentCreated": ShipmentCreatedEventV1,
    "OrderCompleted": OrderCompletedEventV1,
    "OrderFailed": OrderFailedEventV1,
}


def test_generated_event_streams_match_contracts():
    root = Path(__file__).parents[1]
    sample_data = root / "sample-data"
    filenames = (
        "orders_created.jsonl",
        "inventory_results.jsonl",
        "payment_results.jsonl",
        "shipping_results.jsonl",
        "terminal_events.jsonl",
    )

    validated = 0
    for filename in filenames:
        with (sample_data / filename).open(encoding="utf-8") as stream:
            for line in stream:
                event = json.loads(line)
                EVENT_MODELS[event["event_type"]].model_validate(event)
                validated += 1

    assert validated > 0