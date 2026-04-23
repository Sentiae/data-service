// Package permission — PlatformKitAdapter bridges the service-local
// Checker (which knows data-source + column-level rules) to the
// platform-kit middleware.PermissionChecker contract, so data-service
// can mount the shared RequirePermission HTTP middleware on routes that
// gate a data_source by id (e.g. queries, dashboards, annotations).
package permission

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// PlatformKitAdapter implements platform-kit middleware.PermissionChecker
// by reusing Checker.CanReadSource for "can_read" and falling back to
// default-deny for any unknown permission. This keeps the gRPC client and
// caching logic single-sourced in Checker.
type PlatformKitAdapter struct {
	inner *Checker
}

// NewPlatformKitAdapter wraps a Checker for HTTP-middleware consumption.
func NewPlatformKitAdapter(c *Checker) *PlatformKitAdapter {
	return &PlatformKitAdapter{inner: c}
}

// CheckPermission satisfies platform-kit middleware.PermissionChecker.
// subjectID must be a UUID string; resourceID must be a UUID string when
// resourceType is "data_source".
func (a *PlatformKitAdapter) CheckPermission(ctx context.Context, subjectID, permission, resourceType, resourceID string) (bool, error) {
	if a == nil || a.inner == nil {
		return true, nil
	}
	userID, err := uuid.Parse(subjectID)
	if err != nil {
		return false, fmt.Errorf("permission adapter: parse subject %q: %w", subjectID, err)
	}
	switch resourceType {
	case "data_source":
		rid, err := uuid.Parse(resourceID)
		if err != nil {
			return false, fmt.Errorf("permission adapter: parse resource %q: %w", resourceID, err)
		}
		if permission != "can_read" && permission != "read" {
			// data-service currently only exposes read-path gating; writes
			// are gated by the write-approval flow.
			return false, nil
		}
		return a.inner.CanReadSource(ctx, userID, rid), nil
	default:
		// Unknown resource types pass through — callers can add explicit
		// handling here as new resources are gated.
		return true, nil
	}
}
