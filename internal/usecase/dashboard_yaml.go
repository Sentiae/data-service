// Package usecase hosts the dashboard-as-code YAML parser (§12.5).
// The parser is intentionally minimal: enough to validate a YAML
// submission, produce a structured Dashboard value, and hand back
// something the HTTP handler can persist + materialise into a live
// DashboardConfig row.
package usecase

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Dashboard is the parsed representation of a dashboard-as-code YAML
// document. Fields map 1:1 to the YAML top-level keys so operators can
// author by example without reading the schema doc.
type Dashboard struct {
	Slug        string            `yaml:"slug" json:"slug"`
	Name        string            `yaml:"name" json:"name"`
	Description string            `yaml:"description,omitempty" json:"description,omitempty"`
	Tags        []string          `yaml:"tags,omitempty" json:"tags,omitempty"`
	Variables   map[string]string `yaml:"variables,omitempty" json:"variables,omitempty"`
	Queries     []DashboardQuery  `yaml:"queries" json:"queries"`
	Panels      []DashboardPanel  `yaml:"panels" json:"panels"`
}

// DashboardQuery describes one data-source query the dashboard
// depends on. Engine + DataSource together locate the data source;
// SQL is the raw query text with ${variable} substitutions.
type DashboardQuery struct {
	ID         string `yaml:"id" json:"id"`
	Engine     string `yaml:"engine" json:"engine"`
	DataSource string `yaml:"data_source,omitempty" json:"data_source,omitempty"`
	SQL        string `yaml:"sql" json:"sql"`
}

// DashboardPanel describes a single visual panel. QueryID references
// one of the Queries by id; ChartType is a portal-side render hint.
type DashboardPanel struct {
	ID        string         `yaml:"id" json:"id"`
	Title     string         `yaml:"title" json:"title"`
	QueryID   string         `yaml:"query_id" json:"query_id"`
	ChartType string         `yaml:"chart_type,omitempty" json:"chart_type,omitempty"`
	Layout    map[string]any `yaml:"layout,omitempty" json:"layout,omitempty"`
	Options   map[string]any `yaml:"options,omitempty" json:"options,omitempty"`
}

// ErrInvalidDashboardYAML signals a structural problem — empty doc,
// missing required field, duplicate ids, or a panel referencing an
// unknown query id. Handlers return 400 on this; bubble errors
// wrapped with %w so callers can unwrap the specific failure reason.
var ErrInvalidDashboardYAML = errors.New("invalid dashboard yaml")

// ParseDashboardYAML parses the provided YAML bytes into a Dashboard
// and validates structural invariants:
//
//   - slug and name are required
//   - every query has a non-empty id and sql
//   - query ids are unique within the file
//   - every panel has an id and references a query by id
//   - panel ids are unique within the file
func ParseDashboardYAML(input []byte) (*Dashboard, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("%w: empty document", ErrInvalidDashboardYAML)
	}
	var d Dashboard
	if err := yaml.Unmarshal(input, &d); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDashboardYAML, err)
	}
	d.Slug = strings.TrimSpace(d.Slug)
	d.Name = strings.TrimSpace(d.Name)
	if d.Slug == "" {
		return nil, fmt.Errorf("%w: slug is required", ErrInvalidDashboardYAML)
	}
	if d.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidDashboardYAML)
	}
	if len(d.Queries) == 0 {
		return nil, fmt.Errorf("%w: at least one query is required", ErrInvalidDashboardYAML)
	}
	if len(d.Panels) == 0 {
		return nil, fmt.Errorf("%w: at least one panel is required", ErrInvalidDashboardYAML)
	}

	queryIDs := make(map[string]struct{}, len(d.Queries))
	for i, q := range d.Queries {
		id := strings.TrimSpace(q.ID)
		if id == "" {
			return nil, fmt.Errorf("%w: queries[%d] is missing id", ErrInvalidDashboardYAML, i)
		}
		if strings.TrimSpace(q.SQL) == "" {
			return nil, fmt.Errorf("%w: queries[%d] (%q) is missing sql", ErrInvalidDashboardYAML, i, id)
		}
		if _, dup := queryIDs[id]; dup {
			return nil, fmt.Errorf("%w: duplicate query id %q", ErrInvalidDashboardYAML, id)
		}
		queryIDs[id] = struct{}{}
	}

	panelIDs := make(map[string]struct{}, len(d.Panels))
	for i, p := range d.Panels {
		id := strings.TrimSpace(p.ID)
		if id == "" {
			return nil, fmt.Errorf("%w: panels[%d] is missing id", ErrInvalidDashboardYAML, i)
		}
		if _, dup := panelIDs[id]; dup {
			return nil, fmt.Errorf("%w: duplicate panel id %q", ErrInvalidDashboardYAML, id)
		}
		panelIDs[id] = struct{}{}
		if _, ok := queryIDs[strings.TrimSpace(p.QueryID)]; !ok {
			return nil, fmt.Errorf("%w: panels[%d] (%q) references unknown query id %q",
				ErrInvalidDashboardYAML, i, id, p.QueryID)
		}
	}
	return &d, nil
}

// ChecksumYAML returns the SHA-256 checksum (hex) of the YAML bytes.
// Used by the handler to de-duplicate no-op re-submissions.
func ChecksumYAML(input []byte) string {
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:])
}
