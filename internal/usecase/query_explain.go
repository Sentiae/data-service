package usecase

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

// ExplainLLM is the subset of the NL→SQL translator the explain usecase
// needs. Keeps the usecase free of the HTTP wiring (the handler passes in
// a closure around its foundry call).
type ExplainLLM interface {
	Translate(ctx context.Context, question, semanticContext string) (string, error)
}

// SemanticFieldLoader loads the semantic field context for a data source.
type SemanticFieldLoader interface {
	LoadSemanticContext(ctx context.Context, dataSourceID uuid.UUID) (string, error)
}

// QueryPlanner is an optional port that runs a real engine-side plan
// (e.g. Postgres `EXPLAIN (FORMAT JSON)`) and returns its cost + row
// estimates. When the connection is non-Postgres or the EXPLAIN fails,
// the adapter returns (nil, err) and the usecase falls back to the
// conservative static defaults.
type QueryPlanner interface {
	// ExplainPlan resolves the engine-side plan root's Total Cost +
	// Plan Rows for the given SQL against the named data source.
	// The returned cost is in the engine's native unit (Postgres plan
	// units), not USD — callers convert.
	ExplainPlan(ctx context.Context, dataSourceID uuid.UUID, sql string) (*PlanEstimate, error)
}

// PlanEstimate holds the two numbers the explain usecase cares about.
type PlanEstimate struct {
	TotalCost float64
	PlanRows  int64
}

// QueryExplain is the response shape: generatedSQL + cheap static
// estimates. "estimated" values are intentionally conservative — a future
// pass can wire `EXPLAIN (FORMAT JSON)` from the actual engine once we
// have a safe sandboxed execution path.
type QueryExplain struct {
	GeneratedSQL      string   `json:"generatedSQL"`
	EstimatedCostUsd  float64  `json:"estimatedCostUsd"`
	EstimatedRowsRead int      `json:"estimatedRowsRead"`
	AffectedTables    []string `json:"affectedTables"`
	IsWriteQuery      bool     `json:"isWriteQuery"`
}

// QueryExplainUseCase returns what Execute would have run, with a cheap
// static plan estimate, without actually running it.
type QueryExplainUseCase struct {
	LLM     ExplainLLM
	Loader  SemanticFieldLoader
	Planner QueryPlanner
}

// NewQueryExplainUseCase wires a new explain usecase.
func NewQueryExplainUseCase(llm ExplainLLM, loader SemanticFieldLoader) *QueryExplainUseCase {
	return &QueryExplainUseCase{LLM: llm, Loader: loader}
}

// NewQueryExplainUseCaseWithPlanner wires the usecase with an optional
// real EXPLAIN planner. Tests or older call sites stay on the
// planner-less constructor above.
func NewQueryExplainUseCaseWithPlanner(llm ExplainLLM, loader SemanticFieldLoader, planner QueryPlanner) *QueryExplainUseCase {
	return &QueryExplainUseCase{LLM: llm, Loader: loader, Planner: planner}
}

// Explain translates the question through the LLM path, then extracts
// static metadata about the generated SQL. It stops before executing
// anything — callers use this for "what would this cost" previews and
// write-query warnings before committing to /execute.
func (uc *QueryExplainUseCase) Explain(ctx context.Context, question string, dataSourceID uuid.UUID) (*QueryExplain, error) {
	semanticCtx := ""
	if uc.Loader != nil {
		if c, err := uc.Loader.LoadSemanticContext(ctx, dataSourceID); err == nil {
			semanticCtx = c
		}
	}

	generated := ""
	if uc.LLM != nil {
		sql, err := uc.LLM.Translate(ctx, question, semanticCtx)
		if err == nil {
			generated = sql
		}
	}

	result := &QueryExplain{
		GeneratedSQL:   generated,
		AffectedTables: extractTables(generated),
		IsWriteQuery:   IsMutation(generated),
	}

	// G75 — when a real planner is wired and the query is non-empty +
	// read-only, run EXPLAIN and lift the row / cost estimates. Writes
	// stay on the static path because EXPLAIN on DML would actually
	// prepare the statement and we want the preview path to be read-
	// only.
	if uc.Planner != nil && generated != "" && !result.IsWriteQuery {
		if plan, err := uc.Planner.ExplainPlan(ctx, dataSourceID, generated); err == nil && plan != nil {
			result.EstimatedRowsRead = int(plan.PlanRows)
			// Translate Postgres planner cost units to a rough USD
			// figure using the same 1e-5 per-row heuristic the static
			// path uses. The planner's "cost" is a proxy, not a true
			// price — this is advisory.
			result.EstimatedCostUsd = plan.TotalCost * 0.00001
			return result, nil
		}
	}

	// Fallback: 1000 rows assumed per table, $0.00001/row. Kept for
	// non-Postgres sources and EXPLAIN failures.
	result.EstimatedRowsRead = 1000 * len(result.AffectedTables)
	result.EstimatedCostUsd = float64(result.EstimatedRowsRead) * 0.00001

	return result, nil
}

// tableRefRegex pulls identifiers from FROM/JOIN/INTO/UPDATE clauses. It
// intentionally over-matches (e.g. includes schema-qualified identifiers)
// and then normalises to the table-only form.
var tableRefRegex = regexp.MustCompile(`(?i)\b(?:from|join|into|update)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)`)

// extractTables returns unique table references found in the SQL.
// This is best-effort — comments / CTEs / subqueries may fool it; the
// explain output is advisory only.
func extractTables(sql string) []string {
	if sql == "" {
		return nil
	}
	matches := tableRefRegex.FindAllStringSubmatch(sql, -1)
	seen := map[string]bool{}
	var out []string
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		name := strings.TrimSpace(m[1])
		// Strip schema prefix for display — "public.orders" → "orders".
		if idx := strings.LastIndex(name, "."); idx >= 0 {
			name = name[idx+1:]
		}
		name = strings.Trim(name, "\";'`")
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

// PostgresExplainPlanner is a minimal QueryPlanner built on top of a
// raw *sql.DB. Use a connection scoped to the data source in question
// — the planner does not know about cross-source routing.
type PostgresExplainPlanner struct {
	// Conn is the Postgres connection to run EXPLAIN against. Callers
	// can swap this with a read-only pooled connection per data source.
	Conn *sql.DB
}

// NewPostgresExplainPlanner wraps a *sql.DB. A nil Conn returns a
// planner that always fails — callers must check.
func NewPostgresExplainPlanner(conn *sql.DB) *PostgresExplainPlanner {
	return &PostgresExplainPlanner{Conn: conn}
}

// ExplainPlan runs `EXPLAIN (FORMAT JSON) <sql>` and parses the root
// plan's Total Cost + Plan Rows. dataSourceID is unused at this layer —
// the caller is responsible for providing a correctly-scoped Conn.
func (p *PostgresExplainPlanner) ExplainPlan(ctx context.Context, _ uuid.UUID, sqlText string) (*PlanEstimate, error) {
	if p == nil || p.Conn == nil {
		return nil, ErrExplainUnavailable
	}
	// Postgres returns a JSON array with one element — the plan tree.
	// We only need the root Plan's Total Cost + Plan Rows, so we can
	// shallow-decode.
	var raw []byte
	row := p.Conn.QueryRowContext(ctx, "EXPLAIN (FORMAT JSON) "+sqlText)
	if err := row.Scan(&raw); err != nil {
		return nil, err
	}
	var envelope []struct {
		Plan struct {
			TotalCost float64 `json:"Total Cost"`
			PlanRows  int64   `json:"Plan Rows"`
		} `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	if len(envelope) == 0 {
		return nil, ErrExplainUnavailable
	}
	return &PlanEstimate{
		TotalCost: envelope[0].Plan.TotalCost,
		PlanRows:  envelope[0].Plan.PlanRows,
	}, nil
}

// ErrExplainUnavailable signals the planner has no connection or the
// engine returned no plan row. Callers fall back to the static path.
var ErrExplainUnavailable = errExplainUnavailable{}

type errExplainUnavailable struct{}

func (errExplainUnavailable) Error() string { return "explain unavailable" }
