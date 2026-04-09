"""
Pulse Python SDK — high-performance event broker client.

Usage:
    from pulse_py import Pulse

    client = Pulse.connect("127.0.0.1:4222", "my-service", "default")
    client.publish("order.created", {"id": 42, "amount": 1500})
"""

from .pulse_py import Pulse, Event

__all__ = ["Pulse", "Event"]
__version__ = "0.1.0"
