#!/usr/bin/env node
/**
 * Node.js Notification Service — subscribes to payment.* via WebSocket gateway
 * Usage: node node_notification_service.js
 *
 * If WebSocket is not available, falls back to polling HTTP health endpoint
 * and publishing notifications via HTTP.
 */

const GATEWAY = "http://127.0.0.1:8080";

async function publishNotification(channel, message) {
  const body = JSON.stringify({
    topic: `notification.${channel}`,
    data: {
      channel,
      message,
      sent_at: new Date().toISOString(),
    },
  });

  const resp = await fetch(`${GATEWAY}/v1/publish`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });

  const result = await resp.json();
  console.log(`[Node] Notification sent via ${channel}: msg_id=${result.msg_id}`);
  return result;
}

async function main() {
  console.log("[Node] Notification Service starting...");

  // Check gateway health
  try {
    const health = await fetch(`${GATEWAY}/v1/health`);
    const data = await health.json();
    console.log(`[Node] Gateway health: ${data.status}`);
  } catch (e) {
    console.error("[Node] Gateway not reachable:", e.message);
    process.exit(1);
  }

  // Simulate receiving payment events and sending notifications
  const payments = [
    { order_id: "ORD-001", amount: 89.98, customer_email: "hai@example.com" },
    { order_id: "ORD-002", amount: 149.99, customer_email: "user@example.com" },
  ];

  for (const payment of payments) {
    console.log(`[Node] Processing notification for order ${payment.order_id}...`);

    // Send email notification
    await publishNotification("email", {
      to: payment.customer_email,
      subject: `Payment received for order ${payment.order_id}`,
      body: `Your payment of $${payment.amount} has been processed.`,
    });

    // Send SMS for large orders
    if (payment.amount > 100) {
      await publishNotification("sms", {
        to: "+84123456789",
        message: `Large order ${payment.order_id}: $${payment.amount} processed`,
      });
    }

    // Small delay between notifications
    await new Promise((r) => setTimeout(r, 300));
  }

  // Publish a summary event
  await publishNotification("audit", {
    type: "batch_complete",
    notifications_sent: payments.length * 2,
    timestamp: new Date().toISOString(),
  });

  console.log("[Node] Notification Service done.");
}

main().catch(console.error);
