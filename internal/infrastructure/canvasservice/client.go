// Package canvasservice is a narrow HTTP client that data-service
// uses to push query results into canvas-service alongside the durable
// Kafka fan-out. Closes §19.1 flow 1G.
package canvasservice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Client posts query-result rows into canvas-service.
type Client struct {
	baseURL    string
	httpClient *http.Client
	// ServiceToken is sent as Bearer so canvas-service's auth middleware
	// treats data-service as an authenticated caller.
	ServiceToken string
	// ServiceUserID is the pseudo-user-id data-service acts as.
	ServiceUserID string
}

// NewClient builds a Client. An empty baseURL disables the push path.
func NewClient(baseURL string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// QueryResultPayload mirrors the canvas-service receiver shape.
type QueryResultPayload struct {
	QueryID    string           `json:"query_id,omitempty"`
	Rows       []map[string]any `json:"rows"`
	Columns    []string         `json:"columns"`
	RowCount   int              `json:"row_count"`
	DurationMS int64            `json:"duration_ms,omitempty"`
	Status     string           `json:"status,omitempty"`
	ExecutedAt time.Time        `json:"executed_at,omitempty"`
	Error      string           `json:"error,omitempty"`
}

// ApplyQueryResult pushes the result rows to the target canvas node.
func (c *Client) ApplyQueryResult(ctx context.Context, canvasID, nodeID uuid.UUID, payload QueryResultPayload) error {
	if c == nil || c.baseURL == "" {
		return nil
	}
	url := fmt.Sprintf("%s/api/v1/canvases/%s/nodes/%s/query-result", c.baseURL, canvasID, nodeID)
	return c.post(ctx, url, payload)
}

// ApplyQueryResultByNode pushes the result rows without knowing the
// owning canvas id — canvas-service resolves the canvas from the node
// record.
func (c *Client) ApplyQueryResultByNode(ctx context.Context, nodeID uuid.UUID, payload QueryResultPayload) error {
	if c == nil || c.baseURL == "" {
		return nil
	}
	url := fmt.Sprintf("%s/api/v1/canvas-nodes/%s/query-result", c.baseURL, nodeID)
	return c.post(ctx, url, payload)
}

func (c *Client) post(ctx context.Context, url string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("canvasservice: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("canvasservice: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.ServiceToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.ServiceToken)
	}
	if c.ServiceUserID != "" {
		req.Header.Set("X-User-ID", c.ServiceUserID)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[canvasservice.data] POST %s failed: %v", url, err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
		log.Printf("[canvasservice.data] POST %s returned %d: %s", url, resp.StatusCode, string(respBody))
		return fmt.Errorf("canvasservice: %s returned %d", url, resp.StatusCode)
	}
	return nil
}
