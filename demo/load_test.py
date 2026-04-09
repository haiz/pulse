#!/usr/bin/env python3
"""
Pulse Load Test — concurrent multi-language stress test.

Simulates a realistic e-commerce system under load:
  - 50 concurrent order publishers (HTTP)
  - 20 concurrent payment processors (HTTP)
  - 10 concurrent notification senders (HTTP)
  - Burst traffic patterns (spike → steady → spike)
  - Measures throughput, latency percentiles, error rate

Usage: python3 load_test.py
"""
import json
import time
import urllib.request
import urllib.error
import threading
import statistics
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

GATEWAY = "http://127.0.0.1:8080"
RESULTS = defaultdict(list)  # topic -> [latency_ms]
ERRORS = defaultdict(int)    # topic -> error_count
LOCK = threading.Lock()


def publish(topic: str, data: dict) -> float:
    """Publish and return latency in ms."""
    body = json.dumps({"topic": topic, "data": data}).encode()
    req = urllib.request.Request(
        f"{GATEWAY}/v1/publish",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            elapsed = (time.monotonic() - start) * 1000
            if result.get("status") == "stored":
                with LOCK:
                    RESULTS[topic].append(elapsed)
                return elapsed
            else:
                with LOCK:
                    ERRORS[topic] += 1
                return -1
    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        with LOCK:
            ERRORS[topic] += 1
        return -1


def batch_publish(events: list) -> float:
    """Batch publish and return latency in ms."""
    body = json.dumps({"events": events}).encode()
    req = urllib.request.Request(
        f"{GATEWAY}/v1/publish/batch",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            elapsed = (time.monotonic() - start) * 1000
            stored = sum(1 for r in result.get("results", []) if r.get("status") == "stored")
            with LOCK:
                for _ in range(stored):
                    RESULTS["batch"].append(elapsed / max(len(events), 1))
                ERRORS["batch"] += len(events) - stored
            return elapsed
    except Exception:
        with LOCK:
            ERRORS["batch"] += len(events)
        return -1


def order_worker(worker_id: int, count: int):
    """Simulate an order service publishing order events."""
    for i in range(count):
        publish("order.created", {
            "order_id": f"ORD-{worker_id:03d}-{i:04d}",
            "customer": f"customer_{worker_id}_{i}",
            "total": round(10 + (i * 7.77) % 500, 2),
            "items": i % 5 + 1,
            "region": ["VN", "US", "EU", "JP", "KR"][i % 5],
        })


def payment_worker(worker_id: int, count: int):
    """Simulate a payment service."""
    for i in range(count):
        publish("payment.completed", {
            "order_id": f"ORD-PAY-{worker_id:03d}-{i:04d}",
            "amount": round(10 + (i * 13.37) % 1000, 2),
            "method": ["credit_card", "paypal", "bank_transfer"][i % 3],
            "currency": "USD",
        })


def notification_worker(worker_id: int, count: int):
    """Simulate a notification service."""
    for i in range(count):
        channel = ["email", "sms", "push"][i % 3]
        publish(f"notification.{channel}", {
            "to": f"user_{worker_id}_{i}@example.com",
            "subject": f"Order update {i}",
            "priority": ["low", "normal", "high"][i % 3],
        })


def inventory_batch_worker(worker_id: int, batches: int, batch_size: int):
    """Simulate batch inventory updates."""
    for b in range(batches):
        events = [
            {
                "topic": "inventory.updated",
                "data": {
                    "sku": f"SKU-{worker_id:02d}-{b:03d}-{j:02d}",
                    "delta": -(j % 5 + 1),
                    "warehouse": f"WH-{worker_id % 3}",
                },
            }
            for j in range(batch_size)
        ]
        batch_publish(events)


def analytics_worker(count: int):
    """Simulate analytics events (high volume, fire-and-forget)."""
    for i in range(count):
        publish("analytics.pageview", {
            "url": f"/product/{i % 100}",
            "session": f"sess_{i:06d}",
            "ts": time.time(),
        })


def print_stats(phase: str, duration: float):
    """Print latency statistics."""
    all_latencies = []
    for latencies in RESULTS.values():
        all_latencies.extend(latencies)

    total_errors = sum(ERRORS.values())
    total_events = len(all_latencies) + total_errors

    if not all_latencies:
        print(f"  No successful events in {phase}")
        return

    all_latencies.sort()
    p50 = all_latencies[len(all_latencies) // 2]
    p95 = all_latencies[int(len(all_latencies) * 0.95)]
    p99 = all_latencies[int(len(all_latencies) * 0.99)]
    throughput = len(all_latencies) / duration if duration > 0 else 0

    print(f"  Events:     {len(all_latencies)} ok / {total_errors} errors ({total_events} total)")
    print(f"  Throughput: {throughput:,.0f} events/sec")
    print(f"  Latency P50: {p50:.1f}ms  P95: {p95:.1f}ms  P99: {p99:.1f}ms")
    print(f"  Avg: {statistics.mean(all_latencies):.1f}ms  Min: {min(all_latencies):.1f}ms  Max: {max(all_latencies):.1f}ms")

    if total_errors > 0:
        print(f"  Error rate: {total_errors / total_events * 100:.2f}%")

    # Per-topic breakdown
    print(f"\n  Per-topic breakdown:")
    for topic in sorted(RESULTS.keys()):
        lats = RESULTS[topic]
        errs = ERRORS.get(topic, 0)
        if lats:
            tp50 = sorted(lats)[len(lats) // 2]
            print(f"    {topic:30s}  {len(lats):5d} ok  {errs:3d} err  P50={tp50:.1f}ms")


def main():
    print()
    print("╔═══════════════════════════════════════════════════════════╗")
    print("║          Pulse Load Test — Concurrent Stress Test        ║")
    print("╚═══════════════════════════════════════════════════════════╝")
    print()

    # Health check
    try:
        with urllib.request.urlopen(f"{GATEWAY}/v1/health", timeout=2) as r:
            if r.status != 200:
                raise Exception("unhealthy")
    except Exception:
        print("ERROR: Gateway not running on", GATEWAY)
        print("Start the demo first: cargo run -p pulse-demo")
        sys.exit(1)

    print("Gateway healthy. Starting load test...\n")

    # ─── Phase 1: Warm-up ───
    print("━━━ Phase 1: Warm-up (10 events, sequential) ━━━")
    RESULTS.clear()
    ERRORS.clear()
    start = time.monotonic()
    for i in range(10):
        publish("warmup.test", {"i": i})
    dur = time.monotonic() - start
    print_stats("warm-up", dur)
    print()

    # ─── Phase 2: Sustained concurrent load ───
    print("━━━ Phase 2: Sustained load (50 order + 20 payment + 10 notif workers) ━━━")
    RESULTS.clear()
    ERRORS.clear()

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=100) as pool:
        futures = []

        # 50 order publishers, 20 events each = 1000 order events
        for w in range(50):
            futures.append(pool.submit(order_worker, w, 20))

        # 20 payment processors, 25 events each = 500 payment events
        for w in range(20):
            futures.append(pool.submit(payment_worker, w, 25))

        # 10 notification senders, 30 events each = 300 notification events
        for w in range(10):
            futures.append(pool.submit(notification_worker, w, 30))

        # Wait for all
        for f in as_completed(futures):
            f.result()

    dur = time.monotonic() - start
    print(f"  Duration: {dur:.2f}s")
    print_stats("sustained", dur)
    print()

    # ─── Phase 3: Burst spike ───
    print("━━━ Phase 3: Burst spike (100 workers × 10 events = 1000 events) ━━━")
    RESULTS.clear()
    ERRORS.clear()

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=100) as pool:
        futures = []
        for w in range(100):
            futures.append(pool.submit(analytics_worker, 10))
        for f in as_completed(futures):
            f.result()

    dur = time.monotonic() - start
    print(f"  Duration: {dur:.2f}s")
    print_stats("burst", dur)
    print()

    # ─── Phase 4: Batch throughput ───
    print("━━━ Phase 4: Batch publish (10 workers × 20 batches × 10 events = 2000) ━━━")
    RESULTS.clear()
    ERRORS.clear()

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = []
        for w in range(10):
            futures.append(pool.submit(inventory_batch_worker, w, 20, 10))
        for f in as_completed(futures):
            f.result()

    dur = time.monotonic() - start
    print(f"  Duration: {dur:.2f}s")
    print_stats("batch", dur)
    print()

    # ─── Phase 5: Mixed realistic traffic ───
    print("━━━ Phase 5: Mixed realistic traffic (all types concurrent) ━━━")
    RESULTS.clear()
    ERRORS.clear()

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=80) as pool:
        futures = []
        # Orders (30 workers × 10)
        for w in range(30):
            futures.append(pool.submit(order_worker, w, 10))
        # Payments (15 workers × 10)
        for w in range(15):
            futures.append(pool.submit(payment_worker, w, 10))
        # Notifications (10 workers × 10)
        for w in range(10):
            futures.append(pool.submit(notification_worker, w, 10))
        # Analytics (20 workers × 10)
        for w in range(20):
            futures.append(pool.submit(analytics_worker, 10))
        # Batch inventory (5 workers × 5 batches × 10)
        for w in range(5):
            futures.append(pool.submit(inventory_batch_worker, w, 5, 10))

        for f in as_completed(futures):
            f.result()

    dur = time.monotonic() - start
    print(f"  Duration: {dur:.2f}s")
    print_stats("mixed", dur)
    print()

    # ─── Summary ───
    print("╔═══════════════════════════════════════════════════════════╗")
    print("║                Load Test Complete                        ║")
    print("╚═══════════════════════════════════════════════════════════╝")
    print()


if __name__ == "__main__":
    main()
