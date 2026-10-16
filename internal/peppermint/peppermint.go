// Package peppermint is a thin REST client for the Peppermint ticket desk.
// It calls the UNAUTHENTICATED public create endpoint
// (POST /api/v1/ticket/public/create) over the internal container network —
// never via a public Caddy route.
package peppermint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/thomasteoh/event-triage/internal/triage"
)

// Client posts normalized events to Peppermint as tickets.
type Client struct {
	baseURL string
	key     string // optional X-API-Key header
	user    string // createdBy label
	http    *http.Client
}

// New creates a Client for the given Peppermint base URL. The key is optional
// (the public create endpoint needs no auth; the key is only sent when set).
func New(baseURL, key, user string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		key:     key,
		user:    user,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

// Ready reports whether the Peppermint endpoint is reachable. It does a
// lightweight GET and treats any response as ready (the public create endpoint
// is unauthenticated, so a 200/404/4xx all mean the server is up).
func (c *Client) Ready() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return true
}
// CreateTicket posts an event to the public create endpoint and returns the
// ticket ID on success. It fails closed: any non-2xx or empty response is an
// error so the caller can retry or alert rather than silently dropping.
func (c *Client) CreateTicket(ctx context.Context, e *triage.Event) (string, error) {
	payload := map[string]any{
		"name":     e.From,
		"company":  e.Company,
		"detail":   e.Detail,
		"title":    e.Title,
		"priority": e.TicketPriority(),
		"email":    e.Email,
		"engineer": e.Engineer,
		"type":     e.TicketType(),
		"createdBy": c.user,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal ticket: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/ticket/public/create", bytes.NewReader(b))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.key != "" {
		req.Header.Set("X-API-Key", c.key)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("post ticket: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("peppermint create returned %d", resp.StatusCode)
	}
	var out struct {
		ID      string `json:"id"`
		Success bool   `json:"success"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode ticket response: %w", err)
	}
	if !out.Success {
		return "", fmt.Errorf("peppermint create returned success=false")
	}
	return out.ID, nil
}
