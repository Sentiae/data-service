package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// TestAdapter_PropagatesOrgContextHeader (§12.1, C9) verifies the REST
// adapter stamps the X-Sentiae-Org-Id header on outbound calls once
// the planner has called SetOrgContext.
func TestAdapter_PropagatesOrgContextHeader(t *testing.T) {
	orgID := uuid.New()

	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get(OrgHeaderName)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":1},{"id":2}]`))
	}))
	defer server.Close()

	a := NewAdapter()
	a.SetOrgContext(orgID)

	query := server.URL + " :: GET /any"
	res, err := a.Execute(context.Background(), query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if got != orgID.String() {
		t.Fatalf("expected org header %q, got %q", orgID.String(), got)
	}
}

// TestAdapter_NoOrgHeaderWhenUnset ensures the header is omitted when
// no org has been set, so adapters don't stamp an empty value.
func TestAdapter_NoOrgHeaderWhenUnset(t *testing.T) {
	var saw bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, saw = r.Header[OrgHeaderName]
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	a := NewAdapter()

	query := server.URL + " :: GET /any"
	if _, err := a.Execute(context.Background(), query); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if saw {
		t.Fatalf("did not expect %s header when org is unset", OrgHeaderName)
	}
}
