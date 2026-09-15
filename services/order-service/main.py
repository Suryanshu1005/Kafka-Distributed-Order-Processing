from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from shared.common.config import get_config
from shared.common.kafka import KafkaProducerError, create_producer, send_event
from shared.common.logging import get_logger, log_with_context, setup_logger
from shared.contracts.models.events import OrderCreatedEventV1


class OrderItemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: str = Field(min_length=1)
    quantity: int = Field(ge=1)
    unit_price: float = Field(gt=0)


class ShippingAddressRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    street: str = Field(min_length=1)
    city: str = Field(min_length=1)
    state: str = Field(min_length=1)
    postal_code: str = Field(min_length=1)


class OrderCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: str = Field(min_length=1)
    customer_email: str = Field(min_length=3)
    items: list[OrderItemRequest] = Field(min_length=1)
    shipping_address: ShippingAddressRequest | None = None


class OrderCreateResponse(BaseModel):
    order_id: str
    correlation_id: str
    status: str
    timestamp: datetime


config = get_config()
setup_logger("order-service", config.log_level)
logger = get_logger(__name__)
app = FastAPI(title="Order Service", version="1.0.0")
producer: Any | None = None


def get_or_create_producer():
    global producer
    if producer is None:
        producer = create_producer(config)
    return producer


def build_order_created_event(request: OrderCreateRequest) -> OrderCreatedEventV1:
    order_id = f"ORD-{uuid4()}"
    correlation_id = str(uuid4())
    created_at = datetime.now(timezone.utc)
    items = [item.model_dump() for item in request.items]
    amount = round(sum(item["quantity"] * item["unit_price"] for item in items), 2)

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
                    "name": request.customer_id,
                    "email": request.customer_email,
                },
                "currency": "USD",
                "items": items,
                "amount": amount,
                "created_at": created_at,
            },
        }
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "order-service"}


@app.post("/orders", response_model=OrderCreateResponse, status_code=status.HTTP_201_CREATED)
def create_order(request: OrderCreateRequest) -> OrderCreateResponse:
    event = build_order_created_event(request)

    try:
        send_event(
            get_or_create_producer(),
            topic="orders.created",
            event=event.model_dump(mode="json"),
            key=event.order_id,
        )
    except KafkaProducerError as exc:
        log_with_context(
            logger,
            "error",
            "failed to publish OrderCreated",
            service="order-service",
            event_type=event.event_type,
            order_id=event.order_id,
            correlation_id=event.correlation_id,
            topic="orders.created",
        )
        raise HTTPException(status_code=503, detail="Kafka is unavailable") from exc

    log_with_context(
        logger,
        "info",
        "published OrderCreated",
        service="order-service",
        event_type=event.event_type,
        order_id=event.order_id,
        correlation_id=event.correlation_id,
        topic="orders.created",
    )
    return OrderCreateResponse(
        order_id=event.order_id,
        correlation_id=event.correlation_id,
        status="created",
        timestamp=event.occurred_at,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)