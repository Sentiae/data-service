package grpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deep_bridge.go provides an in-process HTTP-handler bridge so the gRPC
// server can dispatch synthetic requests through the existing chi router
// for paths whose business logic is too heavy to duplicate in gRPC
// handlers right now (SyncSchema, ExecuteDataQuery, NaturalLanguageQuery,
// RotateDashboardEmbedToken, GetDashboardByEmbedToken).
//
// Residual debt: extract the business logic into a shared service layer
// so both HTTP and gRPC handlers can call it directly. Tracked in the
// migration notes.

// dispatchHTTP synthesizes an http.Request, runs it through the
// configured chi router, and returns (statusCode, body). When the
// HTTPHandler is unset the bridge returns FailedPrecondition so callers
// see a clear error in tests rather than a nil-deref.
func (s baseServer) dispatchHTTP(method, path string, headers map[string]string, body any) (int, []byte, error) {
	if s.deps.HTTPHandler == nil {
		return 0, nil, status.Error(codes.FailedPrecondition, "data-service grpc bridge: HTTPHandler not wired")
	}
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return 0, nil, status.Errorf(codes.Internal, "marshal bridge body: %v", err)
		}
		reader = bytes.NewReader(buf)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	rr := httptest.NewRecorder()
	s.deps.HTTPHandler.ServeHTTP(rr, req)
	return rr.Code, rr.Body.Bytes(), nil
}

// envelope mirrors the {"success":..,"data":..,"error":..} shape the
// data-service HTTP handlers wrap responses in via response.go.
type envelope struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
	Error   *envelopeError  `json:"error"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// decodeEnvelope unwraps the standard response envelope.
func decodeEnvelope(body []byte) (json.RawMessage, error) {
	if len(body) == 0 {
		return nil, nil
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		// Some endpoints return raw JSON without the envelope.
		return body, nil
	}
	if env.Error != nil && env.Error.Message != "" {
		return nil, fmt.Errorf("%s: %s", env.Error.Code, env.Error.Message)
	}
	if env.Data != nil {
		return env.Data, nil
	}
	return body, nil
}

// statusFromHTTP maps an HTTP status code onto the closest gRPC code,
// preserving the upstream message when present.
func statusFromHTTP(code int, body []byte) error {
	if code >= 200 && code < 300 {
		return nil
	}
	msg := ""
	var env envelope
	if json.Unmarshal(body, &env) == nil && env.Error != nil {
		msg = env.Error.Message
	}
	if msg == "" {
		msg = string(body)
	}
	switch code {
	case http.StatusBadRequest:
		return status.Errorf(codes.InvalidArgument, "%s", msg)
	case http.StatusUnauthorized:
		return status.Errorf(codes.Unauthenticated, "%s", msg)
	case http.StatusForbidden:
		return status.Errorf(codes.PermissionDenied, "%s", msg)
	case http.StatusNotFound:
		return status.Errorf(codes.NotFound, "%s", msg)
	case http.StatusConflict:
		return status.Errorf(codes.AlreadyExists, "%s", msg)
	default:
		return status.Errorf(codes.Internal, "http %d: %s", code, msg)
	}
}

// baseServer carries the Deps reference shared across topical handlers.
type baseServer struct {
	deps Deps
}
