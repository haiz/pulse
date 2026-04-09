"""
Integration tests for pulse_py.

These tests require a running Pulse broker at 127.0.0.1:4222.
Skip with: pytest -k "not integration"
"""

import pytest


def test_import():
    """Verify the module can be imported."""
    from pulse_py import Pulse, Event
    assert Pulse is not None
    assert Event is not None


@pytest.mark.integration
def test_connect_and_publish():
    """Connect to broker and publish an event."""
    from pulse_py import Pulse

    client = Pulse.connect("127.0.0.1:4222", "test-py", "default")
    msg_id = client.publish("test.python", {"hello": "world"})
    assert msg_id is not None
    assert len(msg_id) > 0


@pytest.mark.integration
def test_publish_with_headers():
    """Publish with custom headers."""
    from pulse_py import Pulse

    client = Pulse.connect("127.0.0.1:4222", "test-py", "default")
    msg_id = client.publish(
        "test.python",
        {"data": 42},
        headers={"trace_id": "abc123"},
    )
    assert msg_id is not None


@pytest.mark.integration
def test_subscribe():
    """Subscribe to a topic pattern."""
    from pulse_py import Pulse

    client = Pulse.connect("127.0.0.1:4222", "test-py", "default")
    client.subscribe("test.*")


@pytest.mark.integration
def test_broker_id():
    """Check broker_id property."""
    from pulse_py import Pulse

    client = Pulse.connect("127.0.0.1:4222", "test-py", "default")
    assert client.broker_id is not None


def test_invalid_address():
    """Invalid address should raise ValueError."""
    from pulse_py import Pulse

    with pytest.raises(ValueError):
        Pulse.connect("not-an-address", "svc", "ns")
