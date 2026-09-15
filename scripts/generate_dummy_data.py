#!/usr/bin/env python3
"""
Generate deterministic dummy API payloads and Kafka event streams
for Kafka Distributed Order Processing learning.

This script creates a complete, realistic dataset for testing the order processing pipeline.
It integrates with DummyJSON API to fetch real product and user data:

1. API request payloads for testing the Order Service HTTP endpoint
2. Kafka event streams for each stage of the pipeline (inventory, payment, shipping)
3. Terminal events that represent order completion or failure

Features:
- Fetches real product data from https://dummyjson.com/products (30 items by default)
- Fetches real user data from https://dummyjson.com/users (30 items by default)
- Falls back to mock data if API is unavailable
- Dataset is deterministic (via random.seed) so you can generate the same data repeatedly
- Failure rates allow you to simulate realistic failure scenarios (e.g., 20% payment declines)

Usage:
    python scripts/generate_dummy_data.py --orders 500 --payment-fail-rate 0.2 --seed 42

Output files (in sample-data/):
    - orders_api_payloads.json: List of API request bodies for Order Service
    - orders_created.jsonl: OrderCreated events (one per line, ready for Kafka replay)
    - inventory_results.jsonl: InventoryReserved or InventoryFailed events
    - payment_results.jsonl: PaymentAuthorized or PaymentFailed events
    - shipping_results.jsonl: ShipmentCreated events (only if payment succeeds)
    - terminal_events.jsonl: OrderCompleted or OrderFailed events

Dependencies: None (uses only stdlib: json, random, uuid, datetime, pathlib, urllib)
"""

import argparse
import json
import random
import uuid
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# Default fallback mock data (used if DummyJSON API is unavailable)
MOCK_PRODUCTS = [
    {"id": 1, "title": "Wireless Keyboard", "price": 79.99},
    {"id": 2, "title": "USB Mouse", "price": 29.99},
    {"id": 3, "title": "4K Monitor", "price": 399.99},
    {"id": 4, "title": "Bluetooth Headset", "price": 149.99},
    {"id": 5, "title": "Laptop Stand", "price": 49.99},
    {"id": 6, "title": "Webcam HD", "price": 89.99},
]

MOCK_USERS = [
    {"id": 1, "firstName": "Emily", "lastName": "Johnson", "email": "emily.johnson@x.dummyjson.com"},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@x.dummyjson.com"},
    {"id": 3, "firstName": "Sophia", "lastName": "Brown", "email": "sophia.brown@x.dummyjson.com"},
    {"id": 4, "firstName": "James", "lastName": "Jones", "email": "james.jones@x.dummyjson.com"},
    {"id": 5, "firstName": "Olivia", "lastName": "Garcia", "email": "olivia.garcia@x.dummyjson.com"},
    {"id": 6, "firstName": "Robert", "lastName": "Miller", "email": "robert.miller@x.dummyjson.com"},
]


def fetch_from_api(url: str, description: str):
    """Fetch JSON data from a URL, with error handling and fallback.
    
    Args:
        url: The URL to fetch from
        description: Human-readable description for logging (e.g., "products" or "users")
        
    Returns:
        Parsed JSON response, or None if fetch fails
        
    This function handles network errors gracefully and logs status.
    """
    try:
        print(f"Fetching {description} from {url}...")
        # Create request with User-Agent to avoid potential blocking
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            print(f"✓ Successfully fetched {description}")
            return data
    except urllib.error.HTTPError as e:
        print(f"⚠ Failed to fetch {description}: HTTP {e.code} {e.reason}")
        print(f"  URL: {url}")
        return None
    except urllib.error.URLError as e:
        print(f"⚠ Failed to fetch {description}: {e.reason}")
        print(f"  URL: {url}")
        return None
    except TimeoutError as e:
        print(f"⚠ Failed to fetch {description}: Timeout after 5s")
        print(f"  URL: {url}")
        return None
    except Exception as e:
        print(f"⚠ Failed to fetch {description}: {type(e).__name__}: {e}")
        print(f"  URL: {url}")
        return None


def load_products(use_api: bool = True):
    """Load product data from DummyJSON API or use mock fallback.
    
    Args:
        use_api: Whether to attempt API fetch (default: True)
        
    Returns:
        List of product dicts with id, title, and price
    """
    if not use_api:
        print("Using mock products (API disabled)")
        return MOCK_PRODUCTS
    
    api_data = fetch_from_api(
        "https://dummyjson.com/products?limit=30",
        "products"
    )
    
    if api_data and "products" in api_data:
        products = []
        for p in api_data["products"]:
            products.append({
                "id": p.get("id"),
                "title": p.get("title", "Unknown Product"),
                "price": float(p.get("price", 0.0))
            })
        print(f"Loaded {len(products)} products from API")
        return products
    else:
        print(f"Using {len(MOCK_PRODUCTS)} mock products as fallback")
        return MOCK_PRODUCTS


def load_users(use_api: bool = True):
    """Load user data from DummyJSON API or use mock fallback.
    
    Args:
        use_api: Whether to attempt API fetch (default: True)
        
    Returns:
        List of user dicts with id, firstName, lastName, and email
    """
    if not use_api:
        print("Using mock users (API disabled)")
        return MOCK_USERS
    
    api_data = fetch_from_api(
        "https://dummyjson.com/users?limit=30",
        "users"
    )
    
    if api_data and "users" in api_data:
        users = []
        for u in api_data["users"]:
            users.append({
                "id": u.get("id"),
                "firstName": u.get("firstName", "User"),
                "lastName": u.get("lastName", "Unknown"),
                "email": u.get("email", f"user{u.get('id')}@example.com")
            })
        print(f"Loaded {len(users)} users from API")
        return users
    else:
        print(f"Using {len(MOCK_USERS)} mock users as fallback")
        return MOCK_USERS


def utc_now() -> str:
    """Return current time in ISO 8601 format (UTC).
    
    Used for all event timestamps to ensure consistency across the dataset.
    """
    return datetime.now(timezone.utc).isoformat()


def pick_items(rng: random.Random, products: list):
    """Generate random order line items (1-3 items per order) from available products.
    
    Args:
        rng: Seeded random.Random instance for deterministic generation
        products: List of product dicts with id, title, and price
        
    Returns:
        List of item dicts with product_id, product_name, quantity, and unit_price
        
    Logic:
        - Between 1-3 items per order (realistic shopping pattern)
        - Random product from loaded products catalog
        - Quantity 1-4 units
        - Price from actual product data
    """
    count = rng.randint(1, 3)
    items = []
    for _ in range(count):
        product = rng.choice(products)
        qty = rng.randint(1, 4)
        items.append(
            {
                "product_id": str(product["id"]),
                "quantity": qty,
                "unit_price": product["price"],
            }
        )
    return items


def total_amount(items):
    """Calculate total order amount by summing (quantity * unit_price) for all items.
    
    Args:
        items: List of item dicts with quantity and unit_price
        
    Returns:
        Total amount rounded to 2 decimal places
    """
    return round(sum(i["quantity"] * i["unit_price"] for i in items), 2)


def event_envelope(event_type: str, order_id: str, payload: dict, correlation_id: str):
    """Wrap a business payload in the EventEnvelopeV1 structure.
    
    This envelope conforms to shared/contracts/models/envelope.py EventEnvelopeV1.
    All Kafka events must use this structure to ensure consistency across services.
    
    Args:
        event_type: Type of event (e.g., "OrderCreated", "InventoryReserved", "PaymentAuthorized")
        order_id: The order_id this event relates to
        payload: The business-specific payload (order details, payment results, etc.)
        correlation_id: Correlation ID for tracing this order through the pipeline
        
    Returns:
        Dict representing a complete event with envelope metadata
        
    Fields:
        - event_id: Unique UUID for this specific event
        - event_type: Semantic event type for routing/filtering
        - occurred_at: ISO 8601 timestamp of event
        - correlation_id: Passed through unchanged across all events in same order flow
        - causation_id: ID of the event that caused this one (for tracing dependencies)
        - idempotency_key: Used by consumers for duplicate detection
        - order_id: Order this event relates to
        - payload: Business data specific to the event type
    """
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


def build_order(rng: random.Random, idx: int, products: list, users: list):
    """Generate a single mock order with random items and customer from loaded data.
    
    Args:
        rng: Seeded random.Random instance
        idx: Order number (1-indexed) for generating deterministic order_id
        products: List of product dicts from DummyJSON or mock
        users: List of user dicts from DummyJSON or mock
        
    Returns:
        Dict representing an order (suitable for API request or OrderCreated payload)
        
    Generated order has:
        - Sequential order_id: ORD-000001, ORD-000002, etc.
        - Random customer from loaded users list
        - 1-3 random line items with real product data
        - Calculated total amount
    """
    order_id = f"ORD-{idx:06d}"
    items = pick_items(rng, products)
    user = rng.choice(users)
    amount = total_amount(items)
    return {
        "order_id": order_id,
        "customer": {
            "name": f"{user['firstName']} {user['lastName']}",
            "email": user["email"],
        },
        "currency": "USD",
        "items": items,
        "amount": amount,
        "created_at": utc_now(),
    }


def generate(order_count: int, fail_rate_payment: float, fail_rate_inventory: float, seed: int, products: list, users: list):
    """Generate a complete dataset of orders and events for testing.
    
    This is the core data generation logic. It simulates the happy path and failure
    scenarios of the order processing pipeline:
    
    Happy Path: Order → Inventory Reserved → Payment Authorized → Shipment Created → Order Completed
    
    Failure Scenarios:
    - Inventory fails (e.g., out of stock): Order → Inventory Failed → Order Failed
    - Payment fails (e.g., card declined): Order → Inventory Reserved → Payment Failed → Order Failed
    
    Args:
        order_count: Number of orders to generate (e.g., 100)
        fail_rate_payment: Probability of payment failure [0.0, 1.0] (e.g., 0.2 = 20% fail)
        fail_rate_inventory: Probability of inventory failure [0.0, 1.0] (e.g., 0.1 = 10% fail)
        seed: Random seed for deterministic generation (same seed = same data)
        products: List of product dicts from DummyJSON or mock
        users: List of user dicts from DummyJSON or mock
        
    Returns:
        Dict with structure:
        {
            "orders_api": [...],  # Payloads for POST /orders
            "events": {
                "orders_created": [...],      # OrderCreated events
                "inventory_results": [...],   # InventoryReserved or InventoryFailed
                "payment_results": [...],     # PaymentAuthorized or PaymentFailed
                "shipping_results": [...],    # ShipmentCreated (only if payment succeeds)
                "terminal": [...]             # OrderCompleted or OrderFailed
            }
        }
    """
    # Initialize RNG with seed for reproducibility
    rng = random.Random(seed)

    # Initialize output collections for each event type/stage
    orders_api = []
    orders_created_events = []
    inventory_results_events = []
    payment_results_events = []
    shipping_results_events = []
    terminal_events = []

    # Generate orders one at a time, simulating each stage of the pipeline
    for i in range(1, order_count + 1):
        # Create the base order using loaded products and users data
        order = build_order(rng, i, products, users)
        order_id = order["order_id"]
        
        # Generate a new correlation_id for this order's entire flow
        # This ID will be passed through all events to enable tracing
        correlation_id = str(uuid.uuid4())

        # 1. Store API payload (used to test POST /orders endpoint)
        orders_api.append(order)

        # 2. Create OrderCreated event (entry point to Kafka pipeline)
        created_evt = event_envelope("OrderCreated", order_id, order, correlation_id)
        orders_created_events.append(created_evt)

        # 3. Simulate inventory stage: Check if reservation fails
        inventory_failed = rng.random() < fail_rate_inventory
        if inventory_failed:
            # Inventory reservation failed (e.g., out of stock)
            # Emit InventoryFailed event and terminal OrderFailed
            inv_payload = {
                "status": "FAILED",
                "reason": "INSUFFICIENT_STOCK",
            }
            inventory_results_events.append(
                event_envelope("InventoryFailed", order_id, inv_payload, correlation_id)
            )
            # Order terminates here
            terminal_events.append(
                event_envelope(
                    "OrderFailed",
                    order_id,
                    {"stage": "INVENTORY", "reason": "INSUFFICIENT_STOCK"},
                    correlation_id,
                )
            )
            # Skip to next order (no payment or shipping events)
            continue

        # Inventory succeeded: emit InventoryReserved event
        inventory_results_events.append(
            event_envelope(
                "InventoryReserved",
                order_id,
                {"status": "RESERVED"},
                correlation_id,
            )
        )

        # 4. Simulate payment stage: Check if authorization fails
        payment_failed = rng.random() < fail_rate_payment
        if payment_failed:
            # Payment authorization failed (e.g., card declined, insufficient funds)
            # Emit PaymentFailed event and terminal OrderFailed
            payment_results_events.append(
                event_envelope(
                    "PaymentFailed",
                    order_id,
                    {"status": "DECLINED", "reason": "DUMMY_DECLINE"},
                    correlation_id,
                )
            )
            # Order terminates here (inventory was reserved, but we'd need compensation)
            terminal_events.append(
                event_envelope(
                    "OrderFailed",
                    order_id,
                    {"stage": "PAYMENT", "reason": "DUMMY_DECLINE"},
                    correlation_id,
                )
            )
            # Skip to next order (no shipping events)
            continue

        # Payment succeeded: emit PaymentAuthorized event
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

        # 5. Simulate shipping stage: Create shipment (always succeeds in Phase 1)
        # Phase 2 will add failure scenarios and compensation
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

        # 6. Order completed successfully: emit OrderCompleted terminal event
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
    """Write a Python object as formatted JSON to a file.
    
    Args:
        path: Pathlib.Path object where to write JSON
        value: Python object to serialize (usually a list or dict)
        
    Used for: orders_api_payloads.json (list of order objects)
    """
    with path.open("w", encoding="utf-8") as f:
        json.dump(value, f, indent=2)


def write_jsonl(path: Path, rows):
    """Write a list of objects as JSONL (JSON Lines) format.
    
    JSONL format: One JSON object per line, no trailing comma.
    This format is ideal for streaming/piping into Kafka producers.
    
    Args:
        path: Pathlib.Path object where to write JSONL
        rows: List of dicts/objects to serialize
        
    Used for:
        - orders_created.jsonl
        - inventory_results.jsonl
        - payment_results.jsonl
        - shipping_results.jsonl
        - terminal_events.jsonl
    """
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")


def main():
    """Parse CLI arguments and orchestrate data generation.
    
    Command-line arguments:
        --orders: Number of orders to generate (default: 100)
        --payment-fail-rate: Probability of payment failure 0.0-1.0 (default: 0.15)
        --inventory-fail-rate: Probability of inventory failure 0.0-1.0 (default: 0.1)
        --seed: Random seed for reproducible generation (default: 42)
        --out-dir: Output directory for generated files (default: sample-data)
        --no-api: Disable API calls and use mock data only (for offline mode)
        
    Examples:
        # Generate 100 orders with 20% payment failures using DummyJSON APIs
        python generate_dummy_data.py --orders 100 --payment-fail-rate 0.2
        
        # Generate 500 orders with both failure scenarios, deterministic seed
        python generate_dummy_data.py --orders 500 --payment-fail-rate 0.2 --inventory-fail-rate 0.1 --seed 7
        
        # Use mock data (offline mode)
        python generate_dummy_data.py --orders 50 --no-api
        
        # Custom output directory
        python generate_dummy_data.py --orders 50 --out-dir /tmp/test-data
    """
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
    parser.add_argument(
        "--no-api",
        action="store_true",
        help="Disable API calls and use mock data only (offline mode)",
    )
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "="*60)
    print("Kafka Distributed Order Processing - Data Generator")
    print("="*60 + "\n")

    # Load products and users (from API if enabled, fallback to mock)
    print("Loading data sources...")
    products = load_products(use_api=not args.no_api)
    users = load_users(use_api=not args.no_api)
    print(f"Using {len(products)} products and {len(users)} users\n")

    # Generate the complete dataset
    print(f"Generating {args.orders} orders with:")
    print(f"  - Inventory fail rate: {args.inventory_fail_rate*100:.1f}%")
    print(f"  - Payment fail rate: {args.payment_fail_rate*100:.1f}%")
    print(f"  - Random seed: {args.seed}\n")
    
    data = generate(
        order_count=args.orders,
        fail_rate_payment=args.payment_fail_rate,
        fail_rate_inventory=args.inventory_fail_rate,
        seed=args.seed,
        products=products,
        users=users,
    )

    # Write output files
    print("Writing output files...")
    write_json(out_dir / "orders_api_payloads.json", data["orders_api"])
    write_jsonl(out_dir / "orders_created.jsonl", data["events"]["orders_created"])
    write_jsonl(out_dir / "inventory_results.jsonl", data["events"]["inventory_results"])
    write_jsonl(out_dir / "payment_results.jsonl", data["events"]["payment_results"])
    write_jsonl(out_dir / "shipping_results.jsonl", data["events"]["shipping_results"])
    write_jsonl(out_dir / "terminal_events.jsonl", data["events"]["terminal"])

    # Print summary statistics
    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    print(f"Output directory: {out_dir.resolve()}\n")
    print(f"Files generated:")
    print(f"  - orders_api_payloads.json: {len(data['orders_api'])} API payloads")
    print(f"  - orders_created.jsonl: {len(data['events']['orders_created'])} events")
    print(f"  - inventory_results.jsonl: {len(data['events']['inventory_results'])} events")
    print(f"  - payment_results.jsonl: {len(data['events']['payment_results'])} events")
    print(f"  - shipping_results.jsonl: {len(data['events']['shipping_results'])} events")
    print(f"  - terminal_events.jsonl: {len(data['events']['terminal'])} events\n")
    
    # Calculate success rates
    completed = sum(1 for e in data['events']['terminal'] if e['payload']['stage'] == 'COMPLETED')
    print(f"Order completion rate: {completed}/{args.orders} ({completed*100//args.orders}%)")
    print("="*60 + "\n")
    print("Ready for testing! See docs/SETUP.md for next steps.")


if __name__ == "__main__":
    main()
