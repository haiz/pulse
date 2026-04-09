// Go Order Service — publishes order.created events via HTTP gateway
// Usage: go run go_order_service.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func main() {
	fmt.Println("[Go] Order Service starting...")
	gateway := "http://127.0.0.1:8080"

	orders := []map[string]any{
		{
			"topic": "order.created",
			"data": map[string]any{
				"order_id": "ORD-GO-001",
				"customer": "Go Customer",
				"total":    149.99,
				"items":    []string{"Go in Action", "Concurrency in Go"},
			},
		},
		{
			"topic": "order.created",
			"data": map[string]any{
				"order_id": "ORD-GO-002",
				"customer": "Another Customer",
				"total":    29.99,
				"items":    []string{"The Go Programming Language"},
			},
		},
	}

	for i, order := range orders {
		body, _ := json.Marshal(order)
		resp, err := http.Post(gateway+"/v1/publish", "application/json", bytes.NewReader(body))
		if err != nil {
			fmt.Printf("[Go] Error publishing order %d: %v\n", i+1, err)
			continue
		}

		var result map[string]any
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		fmt.Printf("[Go] Published order %d: msg_id=%s status=%s\n", i+1, result["msg_id"], result["status"])

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("[Go] Order Service done.")
}
