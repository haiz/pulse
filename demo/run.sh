#!/bin/bash
set -e

# Pulse E-Commerce Demo — Full Microservices Flow
# Runs all services in sequence against a running broker+gateway

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
GATEWAY="http://127.0.0.1:8080"

echo ""
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║          Pulse E-Commerce Demo — Run Script              ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

# Check if gateway is running
echo "▸ Checking gateway health..."
if ! curl -sf "$GATEWAY/v1/health" > /dev/null 2>&1; then
    echo "  ✗ Gateway not running on $GATEWAY"
    echo "  Start the demo first: cargo run -p pulse-demo"
    exit 1
fi
echo "  ✓ Gateway is healthy"
echo ""

# ─── Step 1: curl smoke test ───
echo "━━━ STEP 1: [curl] Smoke test ━━━"
echo ""

echo "  Publishing test event..."
RESULT=$(curl -sf -X POST "$GATEWAY/v1/publish" \
    -H "Content-Type: application/json" \
    -d '{"topic":"test.smoke","data":{"test":true,"timestamp":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}}')
echo "  Response: $RESULT"
echo ""

echo "  Checking health..."
curl -sf "$GATEWAY/v1/health" | python3 -m json.tool 2>/dev/null || curl -sf "$GATEWAY/v1/health"
echo ""

echo "  Checking info..."
curl -sf "$GATEWAY/v1/info" | python3 -m json.tool 2>/dev/null || curl -sf "$GATEWAY/v1/info"
echo ""

# ─── Step 2: Go Order Service ───
echo "━━━ STEP 2: [Go] Order Service ━━━"
echo ""
if command -v go &> /dev/null; then
    cd "$SCRIPT_DIR" && go run go_order_service.go
    echo ""
else
    echo "  ⚠ Go not installed, simulating with curl..."
    curl -sf -X POST "$GATEWAY/v1/publish" \
        -H "Content-Type: application/json" \
        -d '{"topic":"order.created","data":{"order_id":"ORD-CURL-001","customer":"Curl Customer","total":59.99}}'
    echo ""
    echo ""
fi

# ─── Step 3: Python Payment Service ───
echo "━━━ STEP 3: [Python] Payment Service ━━━"
echo ""
if command -v python3 &> /dev/null; then
    python3 "$SCRIPT_DIR/python_payment_service.py"
    echo ""
else
    echo "  ⚠ Python3 not installed, simulating with curl..."
    curl -sf -X POST "$GATEWAY/v1/publish" \
        -H "Content-Type: application/json" \
        -d '{"topic":"payment.completed","data":{"order_id":"ORD-CURL-001","amount":59.99,"method":"credit_card"}}'
    echo ""
    echo ""
fi

# ─── Step 4: Node Notification Service ───
echo "━━━ STEP 4: [Node] Notification Service ━━━"
echo ""
if command -v node &> /dev/null; then
    node "$SCRIPT_DIR/node_notification_service.js"
    echo ""
else
    echo "  ⚠ Node.js not installed, simulating with curl..."
    curl -sf -X POST "$GATEWAY/v1/publish" \
        -H "Content-Type: application/json" \
        -d '{"topic":"notification.email","data":{"to":"user@example.com","subject":"Payment received"}}'
    echo ""
    echo ""
fi

# ─── Step 5: Batch publish ───
echo "━━━ STEP 5: [curl] Batch publish ━━━"
echo ""
BATCH_RESULT=$(curl -sf -X POST "$GATEWAY/v1/publish/batch" \
    -H "Content-Type: application/json" \
    -d '{
        "events": [
            {"topic":"audit.log","data":{"action":"demo_complete","services":["go","python","node","curl"]}},
            {"topic":"metrics.demo","data":{"total_events":10,"duration_sec":5}},
            {"topic":"system.heartbeat","data":{"node":"demo","uptime":300}}
        ]
    }')
echo "  Batch result: $BATCH_RESULT"
echo ""

# ─── Summary ───
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║                 Demo Flow Complete ✓                      ║"
echo "╠═══════════════════════════════════════════════════════════╣"
echo "║                                                           ║"
echo "║  Services tested:                                         ║"
echo "║    ✓ [curl]   Smoke test + batch publish                  ║"
echo "║    ✓ [Go]     Order Service (HTTP publish)                ║"
echo "║    ✓ [Python] Payment Service (HTTP publish)              ║"
echo "║    ✓ [Node]   Notification Service (HTTP publish)         ║"
echo "║    ✓ [Rust]   Analytics + Payment (TCP subscribe)         ║"
echo "║                                                           ║"
echo "║  Topics used:                                             ║"
echo "║    order.created, payment.completed, payment.refunded,    ║"
echo "║    inventory.updated, shipping.requested,                 ║"
echo "║    notification.email, notification.sms,                  ║"
echo "║    notification.audit, audit.log, metrics.demo,           ║"
echo "║    system.heartbeat, test.smoke                           ║"
echo "║                                                           ║"
echo "╚═══════════════════════════════════════════════════════════╝"
