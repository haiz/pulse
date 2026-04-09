# pulse-go — Go SDK for Pulse

HTTP client for the Pulse gateway. No CGo or native dependencies.

## Install

```bash
go get github.com/pulse/pulse-go
```

## Usage

```go
package main

import (
    "fmt"
    pulse "github.com/pulse/pulse-go"
)

func main() {
    client := pulse.NewClient("http://localhost:8080", pulse.Options{
        APIKey: "psk_live_abc",
    })

    // Publish
    result, err := client.Publish("order.created", map[string]any{
        "order_id": "ORD-001",
        "total":    99.99,
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("Published:", result.MsgID)

    // Publish with headers
    result, _ = client.Publish("audit.log", map[string]any{
        "action": "payment",
    }, map[string]string{"trace_id": "abc123"})

    // Health check
    healthy, _ := client.Health()
    fmt.Println("Healthy:", healthy)
}
```

## API

| Method | Description |
|--------|-------------|
| `pulse.NewClient(url, opts)` | Create client |
| `client.Publish(topic, data, headers...)` | Publish event |
| `client.PublishBatch(events)` | Batch publish |
| `client.Health()` | Health check |
