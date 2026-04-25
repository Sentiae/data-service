// Package canvasservice is a gRPC client that data-service uses to push
// query results into canvas-service alongside the durable Kafka fan-out.
// Closes §19.1 flow 1G.
//
// Platform rule: service↔service = gRPC. The legacy HTTP client was
// removed in Foxtrot.
package canvasservice

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	canvasv1 "github.com/sentiae/canvas-service/gen/proto/canvas/v1"
)

// Client pushes query-result rows into canvas-service via
// CanvasService.UpdateDashboardNodeResults.
type Client struct {
	conn    *grpc.ClientConn
	client  canvasv1.CanvasServiceClient
	timeout time.Duration

	// ServiceToken / ServiceUserID travel as gRPC metadata so canvas's
	// interceptor can attribute writes to data-service.
	ServiceToken  string
	ServiceUserID string
}

// NewClient dials canvas-service's gRPC listener. An empty grpcAddr
// disables the push path.
func NewClient(grpcAddr string, timeout time.Duration) *Client {
	if grpcAddr == "" {
		return &Client{}
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	conn, err := grpc.NewClient(grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return &Client{timeout: timeout}
	}
	return &Client{
		conn:    conn,
		client:  canvasv1.NewCanvasServiceClient(conn),
		timeout: timeout,
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

func (c *Client) outCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	md := metadata.MD{}
	if c.ServiceToken != "" {
		md.Set("authorization", "Bearer "+c.ServiceToken)
	}
	if c.ServiceUserID != "" {
		md.Set("x-user-id", c.ServiceUserID)
	}
	if len(md) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return context.WithTimeout(ctx, c.timeout)
}

func (c *Client) buildReq(canvasID, nodeID uuid.UUID, payload QueryResultPayload) (*canvasv1.UpdateDashboardNodeResultsRequest, error) {
	rows := make([]*canvasv1.DashboardQueryRow, 0, len(payload.Rows))
	for _, r := range payload.Rows {
		rowBytes, err := json.Marshal(r)
		if err != nil {
			return nil, fmt.Errorf("canvasservice: marshal row: %w", err)
		}
		rows = append(rows, &canvasv1.DashboardQueryRow{RowJson: rowBytes})
	}
	executedAt := ""
	if !payload.ExecutedAt.IsZero() {
		executedAt = payload.ExecutedAt.UTC().Format(time.RFC3339Nano)
	}
	return &canvasv1.UpdateDashboardNodeResultsRequest{
		CanvasId:   canvasID.String(),
		NodeId:     nodeID.String(),
		QueryId:    payload.QueryID,
		Rows:       rows,
		Columns:    payload.Columns,
		RowCount:   int32(payload.RowCount),
		DurationMs: payload.DurationMS,
		Status:     payload.Status,
		ExecutedAt: executedAt,
		Error:      payload.Error,
	}, nil
}

// ApplyQueryResult pushes the result rows to the target canvas node.
func (c *Client) ApplyQueryResult(ctx context.Context, canvasID, nodeID uuid.UUID, payload QueryResultPayload) error {
	if c == nil || c.client == nil {
		return nil
	}
	req, err := c.buildReq(canvasID, nodeID, payload)
	if err != nil {
		return err
	}
	out, cancel := c.outCtx(ctx)
	defer cancel()
	if _, err := c.client.UpdateDashboardNodeResults(out, req); err != nil {
		return fmt.Errorf("canvasservice: UpdateDashboardNodeResults: %w", err)
	}
	return nil
}

// ApplyQueryResultByNode pushes the result rows without knowing the
// owning canvas id — canvas-service resolves the canvas from the node
// record. An empty canvas_id on the gRPC request signals this lookup.
func (c *Client) ApplyQueryResultByNode(ctx context.Context, nodeID uuid.UUID, payload QueryResultPayload) error {
	if c == nil || c.client == nil {
		return nil
	}
	req, err := c.buildReq(uuid.Nil, nodeID, payload)
	if err != nil {
		return err
	}
	req.CanvasId = ""
	out, cancel := c.outCtx(ctx)
	defer cancel()
	if _, err := c.client.UpdateDashboardNodeResults(out, req); err != nil {
		return fmt.Errorf("canvasservice: UpdateDashboardNodeResults(by-node): %w", err)
	}
	return nil
}

// Close releases the underlying gRPC channel.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
