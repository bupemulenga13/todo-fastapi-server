import random
import datetime as dt
from typing import Any, Dict
from fastapi import FastAPI
from fastapi.testclient import TestClient
from dirty_equals import Contains, IsList, IsNow, IsPositiveFloat

app = FastAPI()

@app.post('/order')
async def create_order() -> Dict[str, Any]:
    return {
        "price": random.random() * 100,
        "products": ['milk', 'jucie', 'rice', 'noodles'],
        "created_at" : dt.datetime.now().isoformat(),
        "created_by": "Bupe Mulenga"
    }

# Test Function
def test_create_order_api() -> None:
    client = TestClient(app)
    response = client.post('/order')
    assert response.json() == {
        "price": IsPositiveFloat,
        "products": IsList(len >= 4) & Contains("rice"),
        "created_at": IsNow(iso_string=True),
        "created_by": "Bupe Mulenga"
    }
