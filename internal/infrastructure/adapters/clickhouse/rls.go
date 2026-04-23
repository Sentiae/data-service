// Package clickhouse — §12.1 RLS surface.
//
// adapter.go runs the SET statements via stampRLS on every Query. This
// file exposes the snapshot + expected SET statements so tests can
// verify the per-adapter session setup without touching a live
// ClickHouse cluster.
package clickhouse

import (
	"fmt"

	"github.com/google/uuid"
)

// RLSSnapshot returns the current (org, user) the adapter would stamp
// on a session. Exposed for tests only.
func (a *Adapter) RLSSnapshot() (uuid.UUID, uuid.UUID) {
	return a.rlsSnapshot()
}

// RLSStatements returns the SET statements the adapter would issue on
// the next query's session, in order. Empty if neither org nor user is
// populated. Keeps the statement generation testable without an HTTP
// round-trip.
func (a *Adapter) RLSStatements() []string {
	orgID, userID := a.rlsSnapshot()
	if orgID == uuid.Nil && userID == uuid.Nil {
		return nil
	}
	out := make([]string, 0, 2)
	if orgID != uuid.Nil {
		out = append(out, fmt.Sprintf("SET SQL_app_current_org_id = '%s'", orgID.String()))
	}
	if userID != uuid.Nil {
		out = append(out, fmt.Sprintf("SET SQL_app_current_user_id = '%s'", userID.String()))
	}
	return out
}
