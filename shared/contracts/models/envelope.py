from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventEnvelopeV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    occurred_at: datetime
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = None
    idempotency_key: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    payload: dict[str, Any]
