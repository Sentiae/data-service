package clickhouse

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestRLSStatements_EmptyWithoutContext returns nil when no context
// has been stamped.
func TestRLSStatements_EmptyWithoutContext(t *testing.T) {
	a := NewAdapter()
	if stmts := a.RLSStatements(); stmts != nil {
		t.Fatalf("statements = %v, want nil", stmts)
	}
}

// TestRLSStatements_OrgAndUser confirms both SET statements are issued
// in order when both ids are stamped.
func TestRLSStatements_OrgAndUser(t *testing.T) {
	a := NewAdapter()
	org := uuid.New()
	user := uuid.New()
	a.SetRLSContext(org, user)
	stmts := a.RLSStatements()
	if len(stmts) != 2 {
		t.Fatalf("want 2 SET statements, got %d: %v", len(stmts), stmts)
	}
	if !strings.Contains(stmts[0], org.String()) || !strings.Contains(stmts[0], "SQL_app_current_org_id") {
		t.Fatalf("org stmt = %q", stmts[0])
	}
	if !strings.Contains(stmts[1], user.String()) || !strings.Contains(stmts[1], "SQL_app_current_user_id") {
		t.Fatalf("user stmt = %q", stmts[1])
	}
}
