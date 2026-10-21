from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, HttpUrl

Currency = Literal["RUB", "USD", "EUR"]
PaymentStatus = Literal["pending", "succeeded", "failed"]

class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(min_length=1, max_length=2000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_json")
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
