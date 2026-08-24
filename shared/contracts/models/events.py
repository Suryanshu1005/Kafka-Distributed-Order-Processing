from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .envelope import EventEnvelopeV1


class CustomerV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    email: str = Field(min_length=3)


class OrderItemV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: str = Field(min_length=1)
    quantity: int = Field(ge=1)
    unit_price: float = Field(gt=0)


class OrderCreatedPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    customer: CustomerV1
    currency: str = Field(min_length=3, max_length=3)
    items: list[OrderItemV1] = Field(min_length=1)
    amount: float = Field(gt=0)
    created_at: datetime


class InventoryResultPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["RESERVED", "FAILED"]
    reason: str | None = None


class PaymentResultPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["AUTHORIZED", "DECLINED"]
    auth_code: str | None = None
    reason: str | None = None


class ShippingResultPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["CREATED"]
    shipment_id: str = Field(min_length=1)


class OrderTerminalPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage: Literal["INVENTORY", "PAYMENT", "COMPLETED"]
    reason: str | None = None


class OrderCreatedEventV1(EventEnvelopeV1):
    event_type: Literal["OrderCreated"]
    payload: OrderCreatedPayloadV1


class InventoryReservedEventV1(EventEnvelopeV1):
    event_type: Literal["InventoryReserved"]
    payload: InventoryResultPayloadV1


class InventoryFailedEventV1(EventEnvelopeV1):
    event_type: Literal["InventoryFailed"]
    payload: InventoryResultPayloadV1


class PaymentAuthorizedEventV1(EventEnvelopeV1):
    event_type: Literal["PaymentAuthorized"]
    payload: PaymentResultPayloadV1


class PaymentFailedEventV1(EventEnvelopeV1):
    event_type: Literal["PaymentFailed"]
    payload: PaymentResultPayloadV1


class ShipmentCreatedEventV1(EventEnvelopeV1):
    event_type: Literal["ShipmentCreated"]
    payload: ShippingResultPayloadV1


class OrderCompletedEventV1(EventEnvelopeV1):
    event_type: Literal["OrderCompleted"]
    payload: OrderTerminalPayloadV1


class OrderFailedEventV1(EventEnvelopeV1):
    event_type: Literal["OrderFailed"]
    payload: OrderTerminalPayloadV1
