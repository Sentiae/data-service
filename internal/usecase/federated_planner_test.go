package usecase

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

type stubAdapter struct {
	name   string
	result *FederatedQueryResult
}

func (s *stubAdapter) Name() string { return s.name }
func (s *stubAdapter) Execute(_ context.Context, _ string) (*FederatedQueryResult, error) {
	// Return a fresh copy per call to avoid cross-test mutation.
	rows := make([]map[string]any, len(s.result.Rows))
	for i, r := range s.result.Rows {
		row := make(map[string]any, len(r))
		for k, v := range r {
			row[k] = v
		}
		rows[i] = row
	}
	cols := append([]string(nil), s.result.Columns...)
	return &FederatedQueryResult{Columns: cols, Rows: rows}, nil
}

type denySecretColumn struct{}

func (denySecretColumn) CanAccessColumn(_ context.Context, _ uuid.UUID, _, _, column string) bool {
	return column != "secret"
}

func TestFederatedPlanner_ColumnFilter_StripsRestrictedColumns(t *testing.T) {
	planner := NewFederatedPlanner()
	planner.SetLogger(silentLogger())
	planner.RegisterAdapter("work", &stubAdapter{
		name: "work",
		result: &FederatedQueryResult{
			Columns: []string{"public", "secret"},
			Rows: []map[string]any{
				{"public": "visible", "secret": "hidden"},
				{"public": "also-visible", "secret": "also-hidden"},
			},
		},
	})
	planner.SetPermissionChecker(denySecretColumn{})

	req := &FederatedQueryRequest{
		OrganizationID: uuid.New(),
		UserID:         uuid.New(),
		SubQueries: []SubQuery{
			{Source: "work", Query: "select * from things"},
		},
	}

	got, err := planner.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(got.Columns) != 1 || got.Columns[0] != "public" {
		t.Fatalf("expected only [public] column, got %v", got.Columns)
	}

	for i, row := range got.Rows {
		if _, ok := row["secret"]; ok {
			t.Fatalf("row %d still contains secret column", i)
		}
		if _, ok := row["public"]; !ok {
			t.Fatalf("row %d missing public column", i)
		}
	}
}

func TestFederatedPlanner_ColumnFilter_JoinKeyFiltered(t *testing.T) {
	// When the join key is one of the restricted columns, the join step
	// should fail to match anything (rather than silently leaking values).
	planner := NewFederatedPlanner()
	planner.SetLogger(silentLogger())
	planner.RegisterAdapter("a", &stubAdapter{
		name: "a",
		result: &FederatedQueryResult{
			Columns: []string{"id", "secret"},
			Rows: []map[string]any{
				{"id": "1", "secret": "x"},
			},
		},
	})
	planner.RegisterAdapter("b", &stubAdapter{
		name: "b",
		result: &FederatedQueryResult{
			Columns: []string{"id", "public"},
			Rows: []map[string]any{
				{"id": "1", "public": "p"},
			},
		},
	})
	planner.SetPermissionChecker(denySecretColumn{})

	req := &FederatedQueryRequest{
		OrganizationID: uuid.New(),
		UserID:         uuid.New(),
		JoinKey:        "id",
		SubQueries: []SubQuery{
			{Source: "a", Query: "select * from a"},
			{Source: "b", Query: "select * from b"},
		},
	}

	got, err := planner.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, row := range got.Rows {
		for k := range row {
			if k == "a.secret" || k == "b.secret" {
				t.Fatalf("secret column leaked in joined result: %v", row)
			}
		}
	}
}
