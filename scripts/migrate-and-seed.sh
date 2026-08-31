#!/bin/sh
set -eu

alembic -c alembic-orders.ini upgrade head
alembic -c alembic-inventory.ini upgrade head
python -m ordermesh.seed
