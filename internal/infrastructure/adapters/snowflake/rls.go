// Package snowflake — §12.1 RLS surface.
//
// The RLS wire-up lives in adapter.go (SetRLSContext + applyRLS). This
// file exists so operators can grep for "rls.go" under each warehouse
// adapter and find the consistent entry point; it re-exports the
// applyRLS seam under a stable name so tests + future refactors have
// one place to look.
package snowflake

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

// ApplyRLS is the exported alias for applyRLS so callers outside the
// package can write verification tests against the RLS path without
// reaching into unexported methods. Internal call sites should keep
// using a.applyRLS for terseness.
func (a *Adapter) ApplyRLS(ctx context.Context, conn *sql.Conn) error {
	return a.applyRLS(ctx, conn)
}

// RLSSnapshot returns the current (org, user) the adapter would stamp
// on a session. Exposed for tests — production code should never read
// these directly; the adapter stamps them via applyRLS on every query.
func (a *Adapter) RLSSnapshot() (uuid.UUID, uuid.UUID) {
	return a.rlsSnapshot()
}
