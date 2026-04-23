package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// fakeNLDelegate returns a canned SQL per source so Plan can be
// exercised without a live foundry endpoint. It captures the prompt
// and source name so tests can verify the planner threaded both.
type fakeNLDelegate struct {
	called map[string]string // source.Name → prompt
	reply  string
	err    error
}

func (f *fakeNLDelegate) NLToSQL(_ context.Context, prompt string, src DataSourceRef) (string, error) {
	if f.called == nil {
		f.called = map[string]string{}
	}
	f.called[src.Name] = prompt
	if f.err != nil {
		return "", f.err
	}
	return f.reply, nil
}

// TestPlan_RejectsBothNLAndSQL enforces the mutual-exclusion rule.
func TestPlan_RejectsBothNLAndSQL(t *testing.T) {
	fp := NewFederatedPlanner()
	_, err := fp.Plan(context.Background(), PlanRequest{NLQuery: "x", SQL: "SELECT 1"})
	if err == nil {
		t.Fatalf("want mutual exclusion error, got nil")
	}
}

// TestPlan_RejectsBothEmpty enforces at-least-one.
func TestPlan_RejectsBothEmpty(t *testing.T) {
	fp := NewFederatedPlanner()
	_, err := fp.Plan(context.Background(), PlanRequest{})
	if err == nil {
		t.Fatalf("want empty-input error, got nil")
	}
}

// TestPlanNL_FansOutAcrossSources confirms the NL path calls the
// delegate once per supplied source and emits one sub-query per source.
func TestPlanNL_FansOutAcrossSources(t *testing.T) {
	delegate := &fakeNLDelegate{reply: "SELECT 1"}
	fp := NewFederatedPlanner()
	fp.SetNLDelegate(delegate)

	req := PlanRequest{
		OrganizationID: uuid.New(),
		UserID:         uuid.New(),
		NLQuery:        "Total sales this quarter",
		Sources: []DataSourceRef{
			{Name: "snowflake_sales", Engine: "snowflake", DSN: "snowflake://x", Tables: []string{"orders"}},
			{Name: "bigquery_web", Engine: "bigquery", DSN: "bigquery://y", Tables: []string{"pageviews"}},
		},
	}
	out, err := fp.Plan(context.Background(), req)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(out.SubQueries) != 2 {
		t.Fatalf("expected 2 sub-queries, got %d", len(out.SubQueries))
	}
	if len(delegate.called) != 2 {
		t.Fatalf("delegate should have been called twice, got %d", len(delegate.called))
	}
	for _, sq := range out.SubQueries {
		if !strings.Contains(sq.Query, "::") {
			t.Fatalf("expected DSL form `<dsn> :: <sql>`, got %q", sq.Query)
		}
	}
}

// TestPlanNL_RequiresDelegate covers the safety rail when foundry isn't
// wired.
func TestPlanNL_RequiresDelegate(t *testing.T) {
	fp := NewFederatedPlanner()
	_, err := fp.Plan(context.Background(), PlanRequest{
		NLQuery: "anything",
		Sources: []DataSourceRef{{Name: "x"}},
	})
	if err == nil {
		t.Fatalf("want missing-delegate error, got nil")
	}
}

// TestPlanNL_DelegateErrorBubbles up.
func TestPlanNL_DelegateErrorBubbles(t *testing.T) {
	delegate := &fakeNLDelegate{err: errors.New("foundry down")}
	fp := NewFederatedPlanner()
	fp.SetNLDelegate(delegate)
	_, err := fp.Plan(context.Background(), PlanRequest{
		NLQuery: "anything",
		Sources: []DataSourceRef{{Name: "x"}},
	})
	if err == nil {
		t.Fatalf("want bubbled error, got nil")
	}
}

// TestPlanSQL_SingleSource routes a SELECT against one catalog source
// through unchanged.
func TestPlanSQL_SingleSource(t *testing.T) {
	fp := NewFederatedPlanner()
	out, err := fp.Plan(context.Background(), PlanRequest{
		SQL: "SELECT * FROM orders WHERE revenue > 100",
		Sources: []DataSourceRef{
			{Name: "warehouse", Engine: "postgres", DSN: "postgres://x", Tables: []string{"orders"}},
		},
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(out.SubQueries) != 1 {
		t.Fatalf("want 1 sub-query, got %d", len(out.SubQueries))
	}
	if !strings.Contains(out.SubQueries[0].Query, "SELECT * FROM orders") {
		t.Fatalf("original SQL should pass through for single source: %q", out.SubQueries[0].Query)
	}
}

// TestPlanSQL_MultiSourceJoin splits a cross-source query into two
// per-source sub-queries the Execute phase can hash-join.
func TestPlanSQL_MultiSourceJoin(t *testing.T) {
	fp := NewFederatedPlanner()
	out, err := fp.Plan(context.Background(), PlanRequest{
		SQL: "SELECT * FROM orders JOIN users ON orders.user_id = users.id",
		Sources: []DataSourceRef{
			{Name: "sales", Engine: "postgres", DSN: "postgres://a", Tables: []string{"orders"}},
			{Name: "identity", Engine: "postgres", DSN: "postgres://b", Tables: []string{"users"}},
		},
		JoinKey: "user_id",
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(out.SubQueries) != 2 {
		t.Fatalf("want 2 sub-queries, got %d", len(out.SubQueries))
	}
	if out.JoinKey != "user_id" {
		t.Fatalf("join key not propagated: %q", out.JoinKey)
	}
	// Each sub-query must target exactly one of the sources.
	names := []string{out.SubQueries[0].Source, out.SubQueries[1].Source}
	if !(contains(names, "sales") && contains(names, "identity")) {
		t.Fatalf("missing expected sources: %v", names)
	}
}

// TestPlanSQL_UnknownTable rejects SQL that references a table no
// catalog source owns.
func TestPlanSQL_UnknownTable(t *testing.T) {
	fp := NewFederatedPlanner()
	_, err := fp.Plan(context.Background(), PlanRequest{
		SQL: "SELECT * FROM nonexistent",
		Sources: []DataSourceRef{
			{Name: "warehouse", Engine: "postgres", DSN: "postgres://x", Tables: []string{"orders"}},
		},
	})
	if err == nil {
		t.Fatalf("want unresolved-table error, got nil")
	}
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
