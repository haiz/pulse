package pulse

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	client := NewClient("http://localhost:8080", Options{
		APIKey: "test-key",
	})

	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.url != "http://localhost:8080" {
		t.Errorf("unexpected url: %s", client.url)
	}
	if client.apiKey != "test-key" {
		t.Errorf("unexpected api key: %s", client.apiKey)
	}
}

func TestHealthWhenBrokerDown(t *testing.T) {
	client := NewClient("http://localhost:19999", Options{})
	healthy, err := client.Health()
	if err == nil && healthy {
		t.Error("expected health check to fail when broker is down")
	}
}
