/**
 * Pulse TypeScript/Node.js SDK
 *
 * Uses the HTTP/WebSocket gateway for communication.
 * No native dependencies — works everywhere Node.js runs.
 *
 * @example
 * ```typescript
 * import { Pulse } from 'pulse-client';
 *
 * const client = new Pulse('http://localhost:8080');
 * await client.publish('order.created', { id: 42, amount: 1500 });
 *
 * client.subscribe('order.*', (event) => {
 *   console.log(event.topic, event.data);
 *   event.ack();
 * });
 * ```
 */

import WebSocket from 'ws';

// ─── Types ───

export interface PulseOptions {
  /** Gateway HTTP URL (default: http://localhost:8080) */
  url?: string;
  /** API key for authentication */
  apiKey?: string;
  /** Auto-reconnect WebSocket on disconnect (default: true) */
  autoReconnect?: boolean;
  /** Max reconnect attempts (default: 10) */
  maxReconnectAttempts?: number;
}

export interface PublishOptions {
  /** Custom headers */
  headers?: Record<string, string>;
}

export interface SubscribeOptions {
  /** Consumer group name */
  group?: string;
  /** Content filter expression */
  filter?: string;
}

export interface PulseEvent {
  msgId: string;
  topic: string;
  data: any;
  headers: Record<string, string>;
  attempt: number;
  /** Acknowledge the event (success) */
  ack: () => void;
  /** Reject the event (trigger retry) */
  nack: () => void;
}

export interface PublishResult {
  msgId: string;
  status: string;
}

export type EventHandler = (event: PulseEvent) => void | Promise<void>;

// ─── Client ───

export class Pulse {
  private url: string;
  private apiKey: string;
  private ws: WebSocket | null = null;
  private handlers: Map<string, EventHandler> = new Map();
  private autoReconnect: boolean;
  private maxReconnectAttempts: number;
  private subIdCounter = 0;

  constructor(options: PulseOptions | string = {}) {
    if (typeof options === 'string') {
      options = { url: options };
    }
    this.url = options.url || 'http://localhost:8080';
    this.apiKey = options.apiKey || '';
    this.autoReconnect = options.autoReconnect ?? true;
    this.maxReconnectAttempts = options.maxReconnectAttempts ?? 10;
  }

  // ─── REST (Publish) ───

  /**
   * Publish an event to a topic.
   *
   * @example
   * ```typescript
   * const result = await client.publish('order.created', { id: 42 });
   * console.log(result.msgId);
   * ```
   */
  async publish(
    topic: string,
    data: any,
    options?: PublishOptions,
  ): Promise<PublishResult> {
    const body = {
      topic,
      data,
      headers: options?.headers || {},
    };

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }

    const response = await fetch(`${this.url}/v1/publish`, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const err = await response.json().catch(() => ({ error: response.statusText }));
      throw new Error(`Publish failed (${response.status}): ${err.error || response.statusText}`);
    }

    const result = await response.json();
    return { msgId: result.msg_id, status: result.status };
  }

  /**
   * Publish multiple events atomically.
   */
  async publishBatch(
    events: Array<{ topic: string; data: any; headers?: Record<string, string> }>,
  ): Promise<PublishResult[]> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }

    const response = await fetch(`${this.url}/v1/publish/batch`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ events }),
    });

    if (!response.ok) {
      throw new Error(`Batch publish failed: ${response.statusText}`);
    }

    const result = await response.json();
    return result.results.map((r: any) => ({
      msgId: r.msg_id,
      status: r.status,
    }));
  }

  // ─── WebSocket (Subscribe) ───

  /**
   * Subscribe to a topic pattern and handle events.
   *
   * @example
   * ```typescript
   * client.subscribe('order.*', (event) => {
   *   console.log(event.topic, event.data);
   *   event.ack();
   * });
   * ```
   */
  subscribe(topic: string, handler: EventHandler, options?: SubscribeOptions): string {
    const subId = `sub-${++this.subIdCounter}`;
    this.handlers.set(subId, handler);

    this.ensureWsConnected();

    this.sendWs({
      type: 'sub',
      topic,
      sub_id: subId,
      group: options?.group,
      filter: options?.filter,
    });

    return subId;
  }

  /**
   * Unsubscribe from a subscription.
   */
  unsubscribe(subId: string): void {
    this.handlers.delete(subId);
    this.sendWs({ type: 'unsub', sub_id: subId });
  }

  /**
   * Close the client connection.
   */
  close(): void {
    this.autoReconnect = false;
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  // ─── Health ───

  /**
   * Check if the gateway is healthy.
   */
  async health(): Promise<boolean> {
    try {
      const response = await fetch(`${this.url}/v1/health`);
      return response.ok;
    } catch {
      return false;
    }
  }

  // ─── Internal ───

  private ensureWsConnected(): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }

    const wsUrl = this.url.replace(/^http/, 'ws') + '/v1/subscribe';
    const fullUrl = this.apiKey ? `${wsUrl}?token=${this.apiKey}` : wsUrl;

    this.ws = new WebSocket(fullUrl);

    this.ws.on('message', (raw: Buffer) => {
      try {
        const msg = JSON.parse(raw.toString());
        this.handleWsMessage(msg);
      } catch {
        // ignore malformed messages
      }
    });

    this.ws.on('close', () => {
      if (this.autoReconnect && this.handlers.size > 0) {
        setTimeout(() => this.ensureWsConnected(), 1000);
      }
    });

    this.ws.on('error', () => {
      // error handling — reconnect will be triggered by 'close' event
    });
  }

  private handleWsMessage(msg: any): void {
    if (msg.type === 'event') {
      // Deliver to all handlers (topic matching is done server-side)
      for (const [subId, handler] of this.handlers) {
        const event: PulseEvent = {
          msgId: msg.msg_id,
          topic: msg.topic,
          data: msg.data,
          headers: msg.headers || {},
          attempt: msg.attempt || 1,
          ack: () => this.sendWs({ type: 'ack', msg_id: msg.msg_id }),
          nack: () => {
            // NACK is implicit — just don't ACK and the broker will retry
          },
        };
        Promise.resolve(handler(event)).catch(console.error);
      }
    }
  }

  private sendWs(msg: any): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }
}

export default Pulse;
