package snowflake

import (
	"testing"

	"github.com/google/uuid"
)

// TestSetRLSContext_RoundTrips confirms the snapshot returns what was
// stamped — the core contract the planner relies on before calling
// Execute.
func TestSetRLSContext_RoundTrips(t *testing.T) {
	a := NewAdapter()
	org := uuid.New()
	user := uuid.New()
	a.SetRLSContext(org, user)
	gotOrg, gotUser := a.RLSSnapshot()
	if gotOrg != org {
		t.Fatalf("org = %s, want %s", gotOrg, org)
	}
	if gotUser != user {
		t.Fatalf("user = %s, want %s", gotUser, user)
	}
}

// TestSetRLSContext_Zeroable confirms clearing the context (e.g. in a
// test harness reset) works.
func TestSetRLSContext_Zeroable(t *testing.T) {
	a := NewAdapter()
	a.SetRLSContext(uuid.New(), uuid.New())
	a.SetRLSContext(uuid.Nil, uuid.Nil)
	org, user := a.RLSSnapshot()
	if org != uuid.Nil || user != uuid.Nil {
		t.Fatalf("expected zeros, got (%s, %s)", org, user)
	}
}
