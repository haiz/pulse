# pulse-py — Python SDK for Pulse

Native Python bindings via PyO3. Communicates directly over Pulse's binary TCP protocol for maximum performance.

## Install

```bash
cd sdks/python
pip install maturin
maturin develop
```

## Usage

```python
from pulse_py import Pulse

# Connect
client = Pulse.connect("127.0.0.1:4222", "my-service", "default")

# Publish
msg_id = client.publish("order.created", {"id": 42, "total": 99.99})
print(f"Published: {msg_id}")

# Publish with headers
msg_id = client.publish("audit.log", {"action": "payment"}, 
    headers={"trace_id": "abc123"})

# Subscribe
client.subscribe("order.*")
client.subscribe("payment.>", group="payment-workers")

# Properties
print(client.broker_id)
```

## API

| Method | Description |
|--------|-------------|
| `Pulse.connect(addr, service_id, namespace, api_key="")` | Connect to broker |
| `client.publish(topic, data, headers=None)` | Publish event, returns msg_id |
| `client.subscribe(topic, group=None)` | Subscribe to topic pattern |
| `client.broker_id` | Broker ID from CONNACK |

## Alternative: HTTP Gateway

If you don't need native performance, use the HTTP gateway with `requests` or `urllib`:

```python
import requests

requests.post("http://localhost:8080/v1/publish", json={
    "topic": "order.created",
    "data": {"id": 42}
})
```
