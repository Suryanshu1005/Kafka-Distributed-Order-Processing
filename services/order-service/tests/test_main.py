import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parents[1]))
import main


class FakeProducer:
    pass


def order_payload():
    return {
        "customer_id": "customer-1",
        "customer_email": "customer@example.com",
        "items": [
            {
                "product_id": "1",
                "quantity": 2,
                "unit_price": 10.0,
            }
        ],
        "shipping_address": {
            "street": "1 Main Street",
            "city": "Austin",
            "state": "TX",
            "postal_code": "78701",
        },
    }


def test_health():
    response = TestClient(main.app).get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_create_order_publishes_typed_event(monkeypatch):
    published = {}

    def fake_send_event(producer, topic, event, key):
        published.update(producer=producer, topic=topic, event=event, key=key)
        return True

    monkeypatch.setattr(main, "send_event", fake_send_event)
    monkeypatch.setattr(main, "get_or_create_producer", lambda: FakeProducer())

    response = TestClient(main.app).post("/orders", json=order_payload())

    assert response.status_code == 201
    assert response.json()["status"] == "created"
    assert published["topic"] == "orders.created"
    assert published["key"] == response.json()["order_id"]
    assert published["event"]["event_type"] == "OrderCreated"
    assert published["event"]["payload"]["amount"] == 20.0


def test_create_order_returns_503_when_kafka_fails(monkeypatch):
    def failing_send_event(*args, **kwargs):
        raise main.KafkaProducerError("broker unavailable")

    monkeypatch.setattr(main, "send_event", failing_send_event)
    monkeypatch.setattr(main, "get_or_create_producer", lambda: FakeProducer())

    response = TestClient(main.app).post("/orders", json=order_payload())

    assert response.status_code == 503
    assert response.json() == {"detail": "Kafka is unavailable"}