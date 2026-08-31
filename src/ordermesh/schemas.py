from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator

from ordermesh.models import OrderStatus


class OrderItemCreate(BaseModel):
    sku: str = Field(pattern=r"^[A-Z0-9][A-Z0-9_-]{1,79}$")
    quantity: int = Field(ge=1, le=1000)


class OrderCreate(BaseModel):
    customer_email: EmailStr
    items: list[OrderItemCreate] = Field(min_length=1, max_length=50)
    simulate_transient_failures: int = Field(default=0, ge=0, le=10)

    @model_validator(mode="after")
    def unique_skus(self) -> "OrderCreate":
        skus = [item.sku for item in self.items]
        if len(skus) != len(set(skus)):
            raise ValueError("Each SKU may occur only once")
        return self


class OrderItemRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: str
    quantity: int


class OrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    customer_email: EmailStr
    status: OrderStatus
    correlation_id: str
    failure_reason: str | None
    items: list[OrderItemRead]
    created_at: datetime
    updated_at: datetime


class OrderList(BaseModel):
    items: list[OrderRead]
    total: int
    limit: int
    offset: int
