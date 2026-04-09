import { Pulse } from '../src/index';

describe('Pulse TypeScript SDK', () => {
  test('can create client', () => {
    const client = new Pulse('http://localhost:8080');
    expect(client).toBeDefined();
  });

  test('can create client with options', () => {
    const client = new Pulse({
      url: 'http://broker:8080',
      apiKey: 'psk_live_abc',
      autoReconnect: false,
    });
    expect(client).toBeDefined();
  });

  test('health check returns false when broker is down', async () => {
    const client = new Pulse('http://localhost:19999');
    const healthy = await client.health();
    expect(healthy).toBe(false);
  });
});
