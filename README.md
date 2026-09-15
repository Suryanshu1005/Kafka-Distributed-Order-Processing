# Kafka Distributed Order Processing

Event-driven order processing MVP built with FastAPI, Python services, and Apache Kafka. The project demonstrates a complete distributed flow from order intake through inventory reservation, payment authorization, and shipment creation while preserving `correlation_id`, `causation_id`, and `order_id` partition ordering across Kafka topics.

## Current Status

Phase 1 MVP is complete and has been verified locally on Windows with Kafka 4.3.1 in KRaft mode.

- 4 services implemented: order, inventory, payment, shipping
- 4 Kafka topics created and used by the flow
- 30 tests passing
- End-to-end order flow verified through all services
- 50 fixture orders replayed through the live system

## Architecture

```text
HTTP POST /orders
	|
	v
Order Service
	|
	| OrderCreated
	v
orders.created
	|
	+------------------> Payment Service caches order details
	|
	v
Inventory Service
	|
	| InventoryReserved or InventoryFailed
	v
inventory.reserve.result
	|
	v
Payment Service
	|
	| PaymentAuthorized or PaymentFailed
	v
payment.authorize.result
	|
	v
Shipping Service
	|
	| ShipmentCreated for authorized payments only
	v
shipping.create.result
```

All Kafka messages use `order_id` as the partition key. Every downstream event preserves the original `correlation_id` and sets `causation_id` to the predecessor event ID.

## Repository Layout

- `services/order-service` - FastAPI order intake API and `orders.created` producer.
- `services/inventory-service` - `orders.created` consumer and `inventory.reserve.result` producer.
- `services/payment-service` - dual-topic consumer for order cache plus inventory results, and `payment.authorize.result` producer.
- `services/shipping-service` - payment result consumer and `shipping.create.result` producer.
- `shared/contracts` - Pydantic V1 event contracts and contract tests.
- `shared/common` - shared configuration, Kafka, error, and logging helpers.
- `sample-data` - generated payloads and event stream fixtures.
- `tests` - integration and generated contract tests.
- `scripts` - dummy data generation utilities.
- `infra/docker` - local infrastructure assets.
- `deployments/k8s` - Kubernetes manifests for later deployment work.

## Prerequisites

- Windows PowerShell 5.1 or later
- Python 3.12+
- Java available on `PATH`
- Apache Kafka 4.3.1 extracted locally
- Git

This project was validated with Kafka at:

```powershell
C:\Users\sutiwari\Downloads\kafka_2.13-4.3.1\kafka_2.13-4.3.1
```

If Kafka is installed elsewhere, adjust `$env:KAFKA_HOME` in the commands below.

## Setup

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

For this workspace path, set `PYTHONPATH` before starting services:

```powershell
$env:PYTHONPATH="c:\Users\sutiwari\OneDrive - Amadeus Workplace\Desktop\Suryanshu\Personal\Projects\MCP\Kafka Distributed Order Processing"
```

## Start Kafka Locally

Set Kafka home:

```powershell
$env:KAFKA_HOME="C:\Users\sutiwari\Downloads\kafka_2.13-4.3.1\kafka_2.13-4.3.1"
cd $env:KAFKA_HOME
```

If the Kafka storage directory has not been formatted yet, run this once:

```powershell
$clusterId = .\bin\windows\kafka-storage.bat random-uuid
.\bin\windows\kafka-storage.bat format -t $clusterId -c .\config\server.properties
```

Start the broker:

```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Keep this terminal open while running the services.

## Create Topics

Open a new PowerShell terminal:

```powershell
$env:KAFKA_HOME="C:\Users\sutiwari\Downloads\kafka_2.13-4.3.1\kafka_2.13-4.3.1"
cd $env:KAFKA_HOME

$topics = @(
  "orders.created",
  "inventory.reserve.result",
  "payment.authorize.result",
  "shipping.create.result"
)

foreach ($topic in $topics) {
  .\bin\windows\kafka-topics.bat --create --if-not-exists --bootstrap-server localhost:9092 --topic $topic --partitions 1 --replication-factor 1
}

.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

Expected topics:

```text
inventory.reserve.result
orders.created
payment.authorize.result
shipping.create.result
```

## Run Services

Use four separate PowerShell terminals from the repository root. Set `PYTHONPATH` in each terminal before running the service.

Order Service:

```powershell
$env:PYTHONPATH=(Get-Location).Path
cd services\order-service
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Inventory Service:

```powershell
$env:PYTHONPATH="c:\Users\sutiwari\OneDrive - Amadeus Workplace\Desktop\Suryanshu\Personal\Projects\MCP\Kafka Distributed Order Processing"
cd services\inventory-service
python main.py
```

Payment Service:

```powershell
$env:PYTHONPATH="c:\Users\sutiwari\OneDrive - Amadeus Workplace\Desktop\Suryanshu\Personal\Projects\MCP\Kafka Distributed Order Processing"
cd services\payment-service
python main.py
```

Shipping Service:

```powershell
$env:PYTHONPATH="c:\Users\sutiwari\OneDrive - Amadeus Workplace\Desktop\Suryanshu\Personal\Projects\MCP\Kafka Distributed Order Processing"
cd services\shipping-service
python main.py
```

Consumer groups are configured per service:

- `inventory-service.v1`
- `payment-service.v1`
- `shipping-service.v1`

## Recreate the Flow

Send a test order:

```powershell
$body = '{"customer_id":"CUST-001","customer_email":"test@example.com","items":[{"product_id":"1","quantity":1,"unit_price":99.99}]}'
$response = Invoke-RestMethod -Uri "http://localhost:8000/orders" -Method POST -Body $body -ContentType "application/json"
$response
```

Expected response:

```json
{
  "order_id": "ORD-...",
  "correlation_id": "...",
  "status": "created",
  "timestamp": "..."
}
```

Verify the end-to-end event chain:

```powershell
cd "c:\Users\sutiwari\OneDrive - Amadeus Workplace\Desktop\Suryanshu\Personal\Projects\MCP\Kafka Distributed Order Processing"
$env:PYTHONPATH=(Get-Location).Path
python verify_e2e.py
```

The script checks the first event from each topic and confirms that all events share the same `correlation_id`.

## Replay Fixture Orders

Replay the 50-order sample fixture through the live Order Service:

```powershell
cd "c:\Users\sutiwari\OneDrive - Amadeus Workplace\Desktop\Suryanshu\Personal\Projects\MCP\Kafka Distributed Order Processing"
$env:PYTHONPATH=(Get-Location).Path
python replay_fixture.py
```

Verify topic counts afterward:

```powershell
python verify_fixture.py
```

Notes:

- `orders.created`, `inventory.reserve.result`, and `payment.authorize.result` should contain one event per submitted order.
- `shipping.create.result` contains `ShipmentCreated` only for orders that receive `PaymentAuthorized`.
- Orders with `PaymentFailed` are intentionally skipped by Shipping Service.

## Run Tests

From the repository root:

```powershell
$env:PYTHONPATH=(Get-Location).Path
pytest
```

Expected result for the current Phase 1 implementation:

```text
30 passed
```

## Useful Kafka Inspection Commands

Read events from a topic:

```powershell
$env:KAFKA_HOME="C:\Users\sutiwari\Downloads\kafka_2.13-4.3.1\kafka_2.13-4.3.1"
cd $env:KAFKA_HOME
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic orders.created --from-beginning --max-messages 5
```

Describe consumer groups:

```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group inventory-service.v1
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group payment-service.v1
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group shipping-service.v1
```

## Configuration

Common environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_CLIENT_ID` | `unknown-service` | Kafka client ID |
| `KAFKA_CONSUMER_GROUP` | `unknown-group` | Default consumer group, overridden by services |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Kafka security protocol |
| `INVENTORY_FAIL_RATE` | `0.0` | Random inventory failure injection |
| `PAYMENT_FAIL_RATE` | `0.0` | Random payment failure injection |
| `SHIPPING_FAIL_RATE` | `0.0` | Random shipping failure injection |
| `LOG_LEVEL` | `info` | Application logging level |

## Troubleshooting

Kafka connection refused:

- Confirm Kafka is running on `localhost:9092`.
- Confirm all four topics exist.
- Check that Java is installed and available on `PATH`.

`ModuleNotFoundError: No module named 'shared'`:

- Set `PYTHONPATH` to the repository root before starting each service.

`Libraries for snappy compression codec not found`:

- The shared Kafka producer disables compression for local development. If this appears, confirm the service is using the current `shared/common/kafka.py`.

Consumer rebalance loops or `NotCoordinatorError`:

- Ensure each service uses its own consumer group: `inventory-service.v1`, `payment-service.v1`, and `shipping-service.v1`.

Few or no shipment events after fixture replay:

- This can be expected if payment failures are enabled. Shipping only emits `ShipmentCreated` after `PaymentAuthorized`.

## GitHub Description

Kafka-based distributed order processing MVP with FastAPI microservices, strict Pydantic event contracts, correlation/causation tracing, fixture replay, and local KRaft Kafka verification.
