#!/usr/bin/env python3
"""
Generate deterministic dummy API payloads and Kafka event streams
for Kafka Distributed Order Processing learning.

No external dependencies required.
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

PRODUCTS = [
    "keyboard",
    "mouse",
    "monitor",
    "headset",
    "laptop-stand",
    "webcam",
]

CUSTOMERS = [
    "Aditi",
    "Rahul",
    "Neha",
    "Arjun",
    "Priya",
    "Karan",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def pick_items(rng: random.Random):
    count = rng.randint(1, 3)
    items = []
    for _ in range(count):
        product = rng.choice(PRODUCTS)
        qty = rng.randint(1, 4)
        price = round(rng.uniform(15.0, 350.0), 2)
        items.append(
            {
                "product_id": product,
                "quantity": qty,
                "unit_price": price,
            }
        )
    return items


def total_amount(items):
    return round(sum(i["quantity"] * i["unit_price"] for i in items), 2)


def event_envelope(event_type: str, order_id: str, payload: dict, correlation_id: str):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": utc_now(),
        "correlation_id": correlation_id,
        "causation_id": None,
        "idempotency_key": f"{event_type}:{order_id}",
        "order_id": order_id,
        "payload": payload,
    }


def build_order(rng: random.Random, idx: int):
    order_id = f"ORD-{idx:06d}"
    items = pick_items(rng)
    customer_name = rng.choice(CUSTOMERS)
    amount = total_amount(items)
    return {
        "order_id": order_id,
        "customer": {
            "name": customer_name,
            "email": f"{customer_name.lower()}@example.test",
        },
        "currency": "USD",
        "items": items,
        "amount": amount,
        "created_at": utc_now(),
    }


def generate(order_count: int, fail_rate_payment: float, fail_rate_inventory: float, seed: int):
    rng = random.Random(seed)

    orders_api = []
    orders_created_events = []
    inventory_results_events = []
    payment_results_events = []
    shipping_results_events = []
    terminal_events = []

    for i in range(1, order_count + 1):
        order = build_order(rng, i)
        order_id = order["order_id"]
        correlation_id = str(uuid.uuid4())

        orders_api.append(order)

        created_evt = event_envelope("OrderCreated", order_id, order, correlation_id)
        orders_created_events.append(created_evt)

        inventory_failed = rng.random() < fail_rate_inventory
        if inventory_failed:
            inv_payload = {
                "status": "FAILED",
                "reason": "INSUFFICIENT_STOCK",
            }
            inventory_results_events.append(
                event_envelope("InventoryFailed", order_id, inv_payload, correlation_id)
            )
            terminal_events.append(
                event_envelope(
                    "OrderFailed",
                    order_id,
                    {"stage": "INVENTORY", "reason": "INSUFFICIENT_STOCK"},
                    correlation_id,
                )
            )
            continue

        inventory_results_events.append(
            event_envelope(
                "InventoryReserved",
                order_id,
                {"status": "RESERVED"},
                correlation_id,
            )
        )

        payment_failed = rng.random() < fail_rate_payment
        if payment_failed:
            payment_results_events.append(
                event_envelope(
                    "PaymentFailed",
                    order_id,
                    {"status": "DECLINED", "reason": "DUMMY_DECLINE"},
                    correlation_id,
                )
            )
            terminal_events.append(
                event_envelope(
                    "OrderFailed",
                    order_id,
                    {"stage": "PAYMENT", "reason": "DUMMY_DECLINE"},
                    correlation_id,
                )
            )
            continue

        payment_results_events.append(
            event_envelope(
                "PaymentAuthorized",
                order_id,
                {
                    "status": "AUTHORIZED",
                    "auth_code": f"AUTH-{rng.randint(100000, 999999)}",
                },
                correlation_id,
            )
        )

        shipping_results_events.append(
            event_envelope(
                "ShipmentCreated",
                order_id,
                {
                    "status": "CREATED",
                    "shipment_id": f"SHP-{rng.randint(100000, 999999)}",
                },
                correlation_id,
            )
        )

        terminal_events.append(
            event_envelope(
                "OrderCompleted",
                order_id,
                {"stage": "COMPLETED"},
                correlation_id,
            )
        )

    return {
        "orders_api": orders_api,
        "events": {
            "orders_created": orders_created_events,
            "inventory_results": inventory_results_events,
            "payment_results": payment_results_events,
            "shipping_results": shipping_results_events,
            "terminal": terminal_events,
        },
    }


def write_json(path: Path, value):
    with path.open("w", encoding="utf-8") as f:
        json.dump(value, f, indent=2)


def write_jsonl(path: Path, rows):
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")


def main():
    parser = argparse.ArgumentParser(description="Generate dummy data for APIs and Kafka events")
    parser.add_argument("--orders", type=int, default=100, help="Number of orders to generate")
    parser.add_argument("--payment-fail-rate", type=float, default=0.15, help="0.0 to 1.0")
    parser.add_argument("--inventory-fail-rate", type=float, default=0.1, help="0.0 to 1.0")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatable datasets")
    parser.add_argument(
        "--out-dir",
        type=str,
        default="sample-data",
        help="Output directory for generated files",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = generate(
        order_count=args.orders,
        fail_rate_payment=args.payment_fail_rate,
        fail_rate_inventory=args.inventory_fail_rate,
        seed=args.seed,
    )

    write_json(out_dir / "orders_api_payloads.json", data["orders_api"])
    write_jsonl(out_dir / "orders_created.jsonl", data["events"]["orders_created"])
    write_jsonl(out_dir / "inventory_results.jsonl", data["events"]["inventory_results"])
    write_jsonl(out_dir / "payment_results.jsonl", data["events"]["payment_results"])
    write_jsonl(out_dir / "shipping_results.jsonl", data["events"]["shipping_results"])
    write_jsonl(out_dir / "terminal_events.jsonl", data["events"]["terminal"])

    print(f"Generated dummy dataset in: {out_dir.resolve()}")
    print(f"Orders: {len(data['orders_api'])}")
    print(f"Orders created events: {len(data['events']['orders_created'])}")
    print(f"Inventory result events: {len(data['events']['inventory_results'])}")
    print(f"Payment result events: {len(data['events']['payment_results'])}")
    print(f"Shipping result events: {len(data['events']['shipping_results'])}")
    print(f"Terminal events: {len(data['events']['terminal'])}")


if __name__ == "__main__":
    main()
