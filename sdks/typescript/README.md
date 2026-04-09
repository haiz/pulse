# pulse-client — TypeScript/Node.js SDK for Pulse

HTTP + WebSocket client. No native dependencies — works everywhere Node.js runs.

## Install

```bash
cd sdks/typescript
npm install
npm run build
```

## Usage

```typescript
import { Pulse } from 'pulse-client';

const client = new Pulse('http://localhost:8080');

// Publish (HTTP)
const result = await client.publish('order.created', { id: 42, total: 99.99 });
console.log(result.msgId);

// Batch publish
await client.publishBatch([
  { topic: 'a', data: { x: 1 } },
  { topic: 'b', data: { x: 2 } },
]);

// Subscribe (WebSocket, auto-reconnect)
client.subscribe('order.*', (event) => {
  console.log(event.topic, event.data);
  event.ack();
});

// With consumer group
client.subscribe('payment.*', handler, { group: 'payment-workers' });

// Health check
const healthy = await client.health();

// Cleanup
client.close();
```

## API

| Method | Description |
|--------|-------------|
| `new Pulse(url, options?)` | Create client |
| `client.publish(topic, data, options?)` | Publish event via HTTP |
| `client.publishBatch(events)` | Batch publish via HTTP |
| `client.subscribe(topic, handler, options?)` | Subscribe via WebSocket |
| `client.unsubscribe(subId)` | Remove subscription |
| `client.health()` | Health check |
| `client.close()` | Disconnect |

## Options

```typescript
const client = new Pulse({
  url: 'http://localhost:8080',
  apiKey: 'psk_live_abc',
  autoReconnect: true,
  maxReconnectAttempts: 10,
});
```
