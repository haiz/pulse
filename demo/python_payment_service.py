#!/usr/bin/env python3
"""
Python Payment Service — subscribes to order.created, publishes payment.completed
Uses HTTP gateway (works without PyO3 SDK installed).
Usage: python3 python_payment_service.py
"""
import json
import time
import urllib.request

GATEWAY = "http://127.0.0.1:8080"


def publish(topic: str, data: dict) -> dict:
    """Publish an event via HTTP gateway."""
    body = json.dumps({"topic": topic, "data": data}).encode()
    req = urllib.request.Request(
        f"{GATEWAY}/v1/publish",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main():
    print("[Python] Payment Service starting...")

    # Simulate processing orders and publishing payment events
    orders = [
        {"order_id": "ORD-PY-001", "amount": 99.99, "customer": "Python Customer"},
        {"order_id": "ORD-PY-002", "amount": 249.50, "customer": "Django Fan"},
        {"order_id": "ORD-PY-003", "amount": 15.00, "customer": "Flask User"},
    ]

    for order in orders:
        print(f"[Python] Processing payment for {order['order_id']}...")

        # Simulate payment processing
        time.sleep(0.3)

        result = publish("payment.completed", {
            "order_id": order["order_id"],
            "amount": order["amount"],
            "currency": "USD",
            "method": "credit_card",
            "transaction_id": f"TXN-PY-{order['order_id'][-3:]}",
            "processed_at": time.time(),
        })

        print(f"[Python] Payment published: msg_id={result['msg_id']} status={result['status']}")

    # Also publish a refund event
    print("[Python] Processing refund...")
    result = publish("payment.refunded", {
        "order_id": "ORD-PY-001",
        "amount": 99.99,
        "reason": "customer_request",
    })
    print(f"[Python] Refund published: msg_id={result['msg_id']} status={result['status']}")

    print("[Python] Payment Service done.")


if __name__ == "__main__":
    main()
