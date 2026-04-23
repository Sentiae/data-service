package bigquery

import (
	"testing"

	"github.com/google/uuid"
)

// TestRLSLabels_EmptyWithoutContext returns nil when no context has
// been stamped.
func TestRLSLabels_EmptyWithoutContext(t *testing.T) {
	a := NewAdapter()
	if labels := a.RLSLabels(); labels != nil {
		t.Fatalf("labels = %v, want nil", labels)
	}
}

// TestRLSLabels_PopulatedFromContext confirms the label map follows
// the BigQuery label naming rules (lowercase) and surfaces both ids.
func TestRLSLabels_PopulatedFromContext(t *testing.T) {
	a := NewAdapter()
	org := uuid.New()
	user := uuid.New()
	a.SetRLSContext(org, user)
	labels := a.RLSLabels()
	if got := labels["app_current_org_id"]; got != org.String() {
		t.Fatalf("org label = %q, want %q", got, org.String())
	}
	if got := labels["app_current_user_id"]; got != user.String() {
		t.Fatalf("user label = %q, want %q", got, user.String())
	}
}
