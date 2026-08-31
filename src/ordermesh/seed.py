import logging

from sqlalchemy import select

from ordermesh.db import InventorySessionLocal
from ordermesh.models import InventoryItem

logger = logging.getLogger(__name__)

SEED_ITEMS = [
    ("BOOK-FASTAPI", "FastAPI in Production", 500),
    ("COURSE-ASYNC", "Async Python Course", 300),
    ("LICENSE-PRO", "Professional License", 1000),
]


def seed_database() -> None:
    with InventorySessionLocal.begin() as db:
        for sku, name, quantity in SEED_ITEMS:
            if db.scalar(select(InventoryItem).where(InventoryItem.sku == sku)) is None:
                db.add(InventoryItem(sku=sku, name=name, available_quantity=quantity))
    logger.info("Inventory seed completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seed_database()
