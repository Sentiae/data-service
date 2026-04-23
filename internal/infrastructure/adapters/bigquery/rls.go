// Package bigquery — §12.1 RLS surface.
//
// The BigQuery adapter stamps org/user on every query via job labels.
// See adapter.go runQuery for the hot path. This file exposes the
// per-adapter state so tests can verify the labels would be populated
// correctly without issuing a real job.
package bigquery

import "github.com/google/uuid"

// RLSSnapshot returns the current (org, user) the adapter would attach
// to a job. Exposed for tests only.
func (a *Adapter) RLSSnapshot() (uuid.UUID, uuid.UUID) {
	return a.rlsSnapshot()
}

// RLSLabels returns the label map the adapter would attach to the next
// BigQuery job. Matches the keys runQuery uses at dispatch time.
func (a *Adapter) RLSLabels() map[string]string {
	orgID, userID := a.rlsSnapshot()
	if orgID == uuid.Nil && userID == uuid.Nil {
		return nil
	}
	labels := map[string]string{}
	if orgID != uuid.Nil {
		labels["app_current_org_id"] = orgID.String()
	}
	if userID != uuid.Nil {
		labels["app_current_user_id"] = userID.String()
	}
	return labels
}
