"""Проверить создание заказа и его обработку работающими workers."""

import json
import os
import time
import urllib.request
from uuid import uuid4

base = os.environ.get("ORDERMESH_SMOKE_URL", "http://localhost:8040")
key = os.environ.get("ORDERMESH_API_KEY", "local-demo-key-change-me")
headers = {
    "Authorization": "Bearer " + key,
    "Content-Type": "application/json",
    "Idempotency-Key": "smoke-" + str(uuid4()),
}
payload = json.dumps(
    {"customer_email": "smoke@example.com", "items": [{"sku": "BOOK-FASTAPI", "quantity": 1}]}
).encode()
request = urllib.request.Request(base + "/api/v1/orders", data=payload, headers=headers)
with urllib.request.urlopen(request, timeout=10) as response:
    assert response.status == 201
    order = json.load(response)
for _ in range(30):
    request = urllib.request.Request(base + "/api/v1/orders/" + order["id"], headers=headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        current = json.load(response)
    if current["status"] != "pending":
        break
    time.sleep(1)
assert current["status"] == "fulfilled", current["status"]
print("HTTP → Orders → RabbitMQ → Inventory → RabbitMQ → Orders: fulfilled")
