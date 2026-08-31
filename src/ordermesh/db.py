from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from ordermesh.config import get_settings


class OrdersBase(DeclarativeBase):
    pass


class InventoryBase(DeclarativeBase):
    pass


settings = get_settings()
orders_engine = create_engine(
    settings.orders_database_url, pool_pre_ping=True, pool_size=10, max_overflow=20
)
inventory_engine = create_engine(
    settings.inventory_database_url, pool_pre_ping=True, pool_size=10, max_overflow=20
)
OrdersSessionLocal = sessionmaker(bind=orders_engine, autoflush=False, expire_on_commit=False)
InventorySessionLocal = sessionmaker(bind=inventory_engine, autoflush=False, expire_on_commit=False)


def get_orders_db() -> Generator[Session, None, None]:
    with OrdersSessionLocal() as session:
        yield session
