from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, selectinload

from ordermesh.broker import check_connection
from ordermesh.db import get_orders_db
from ordermesh.models import Order, OrderStatus
from ordermesh.schemas import OrderCreate, OrderList, OrderRead
from ordermesh.services import create_order, load_order

OrdersDb = Annotated[Session, Depends(get_orders_db)]
router = APIRouter()


@router.get("/health", tags=["service"])
def health(db: OrdersDb) -> dict[str, str]:
    database = "ok"
    rabbitmq = "ok"
    try:
        db.execute(text("SELECT 1"))
    except Exception:  # pragma: no cover
        database = "error"
    try:
        check_connection()
    except Exception:  # pragma: no cover
        rabbitmq = "error"
    if database != "ok" or rabbitmq != "ok":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"orders_database": database, "rabbitmq": rabbitmq},
        )
    return {"status": "ok", "orders_database": database, "rabbitmq": rabbitmq}


@router.post(
    "/api/v1/orders",
    response_model=OrderRead,
    status_code=status.HTTP_201_CREATED,
    tags=["orders"],
)
def create_order_endpoint(
    data: OrderCreate,
    request: Request,
    response: Response,
    db: OrdersDb,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=128)],
) -> Order:
    order, replayed = create_order(
        db,
        data,
        idempotency_key,
        str(request.state.correlation_id),
    )
    response.headers["Idempotent-Replayed"] = str(replayed).lower()
    if replayed:
        response.status_code = status.HTTP_200_OK
    return order


@router.get("/api/v1/orders/{order_id}", response_model=OrderRead, tags=["orders"])
def read_order(order_id: UUID, db: OrdersDb) -> Order:
    return load_order(db, order_id)


@router.get("/api/v1/orders", response_model=OrderList, tags=["orders"])
def list_orders(
    db: OrdersDb,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    order_status: Annotated[OrderStatus | None, Query(alias="status")] = None,
) -> OrderList:
    statement = select(Order).options(selectinload(Order.items))
    count_statement = select(func.count(Order.id))
    if order_status is not None:
        statement = statement.where(Order.status == order_status)
        count_statement = count_statement.where(Order.status == order_status)
    orders = list(
        db.scalars(statement.order_by(Order.created_at.desc()).offset(offset).limit(limit))
    )
    return OrderList(
        items=[OrderRead.model_validate(order) for order in orders],
        total=int(db.scalar(count_statement) or 0),
        limit=limit,
        offset=offset,
    )
