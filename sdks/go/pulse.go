// Package pulse provides a Go client for the Pulse event broker.
//
// Uses the HTTP/WebSocket gateway — no CGo or native dependencies required.
//
// Usage:
//
//	client := pulse.NewClient("http://localhost:8080", pulse.Options{
//	    APIKey: "psk_live_abc123",
//	})
//
//	// Publish
//	result, err := client.Publish("order.created", map[string]any{"id": 42})
//
//	// Subscribe
//	client.Subscribe("order.*", func(event *pulse.Event) {
//	    fmt.Println(event.Topic, event.Data)
//	    event.Ack()
//	})
package pulse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

// Options configures the Pulse client.
type Options struct {
	// API key for authentication.
	APIKey string
	// HTTP client to use (defaults to http.DefaultClient).
	HTTPClient *http.Client
}

// Client is a Pulse event broker client.
type Client struct {
	url        string
	apiKey     string
	httpClient *http.Client
	subCounter atomic.Int64
	mu         sync.Mutex
}

// Event is a received event from the broker.
type Event struct {
	MsgID   string         `json:"msg_id"`
	Topic   string         `json:"topic"`
	Data    any            `json:"data"`
	Headers map[string]string `json:"headers"`
	Attempt int            `json:"attempt"`
}

// PublishResult is the result of a publish operation.
type PublishResult struct {
	MsgID  string `json:"msg_id"`
	Status string `json:"status"`
}

// EventHandler is a callback for received events.
type EventHandler func(event *Event)

// NewClient creates a new Pulse client.
func NewClient(gatewayURL string, opts Options) *Client {
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		url:        gatewayURL,
		apiKey:     opts.APIKey,
		httpClient: httpClient,
	}
}

// Publish sends an event to a topic.
func (c *Client) Publish(topic string, data any, headers ...map[string]string) (*PublishResult, error) {
	h := map[string]string{}
	if len(headers) > 0 && headers[0] != nil {
		h = headers[0]
	}

	body := map[string]any{
		"topic":   topic,
		"data":    data,
		"headers": h,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	req, err := http.NewRequest("POST", c.url+"/v1/publish", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("request error: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("publish failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("publish failed (%d): %s", resp.StatusCode, string(respBody))
	}

	var result PublishResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
}

// PublishBatch sends multiple events atomically.
func (c *Client) PublishBatch(events []struct {
	Topic   string         `json:"topic"`
	Data    any            `json:"data"`
	Headers map[string]string `json:"headers,omitempty"`
}) ([]PublishResult, error) {
	body := map[string]any{
		"events": events,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	req, err := http.NewRequest("POST", c.url+"/v1/publish/batch", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Results []PublishResult `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Results, nil
}

// Health checks if the gateway is healthy.
func (c *Client) Health() (bool, error) {
	resp, err := c.httpClient.Get(c.url + "/v1/health")
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}
