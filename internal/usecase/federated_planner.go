package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// FederatedQueryResult holds the merged result of a cross-domain query.
type FederatedQueryResult struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Sources []string         `json:"sources"`
}

// SourceAdapter executes a sub-query against a specific data source.
type SourceAdapter interface {
	Name() string
	Execute(ctx context.Context, query string) (*FederatedQueryResult, error)
}

// OrgContextSetter is an optional capability an adapter can implement
// to accept per-request row-level security context. The planner calls
// SetOrgContext before each sub-query dispatch so that downstream
// HTTP calls can stamp an `X-Sentiae-Org-Id` header. §12.1 (C9).
type OrgContextSetter interface {
	SetOrgContext(orgID uuid.UUID)
}

// RLSContextSetter is the richer per-adapter RLS hook used by the
// warehouse adapters (Snowflake / BigQuery / ClickHouse). Unlike
// OrgContextSetter this carries the requesting user so the engine's
// native row-policy mechanism (Snowflake session variable, BigQuery
// job label / policy tag, ClickHouse session setting) can scope both
// on org AND on user — matching the Postgres SET LOCAL path. §12.1.
type RLSContextSetter interface {
	SetRLSContext(orgID, userID uuid.UUID)
}

// PermissionChecker evaluates column-level access for a user.
type PermissionChecker interface {
	// CanAccessColumn checks if a user can read a specific column from a source.
	CanAccessColumn(ctx context.Context, userID uuid.UUID, source, table, column string) bool
}

// FederatedPlanner decomposes a cross-domain query into per-source
// sub-queries, executes them, and joins the results in memory.
type FederatedPlanner struct {
	adapters    map[string]SourceAdapter
	permChecker PermissionChecker
	logger      *slog.Logger
	nlDelegate  NLDelegate
}

// NewFederatedPlanner creates a new FederatedPlanner.
func NewFederatedPlanner() *FederatedPlanner {
	return &FederatedPlanner{
		adapters: make(map[string]SourceAdapter),
		logger:   slog.Default(),
	}
}

// SetLogger replaces the planner's logger (for audit-log routing).
func (fp *FederatedPlanner) SetLogger(logger *slog.Logger) {
	if logger != nil {
		fp.logger = logger
	}
}

// RegisterAdapter registers a source adapter for a specific source name.
func (fp *FederatedPlanner) RegisterAdapter(name string, adapter SourceAdapter) {
	fp.adapters[name] = adapter
}

// SetPermissionChecker sets the column-level permission checker. When set,
// every sub-query result is filtered to remove columns the requesting user
// is not allowed to read.
func (fp *FederatedPlanner) SetPermissionChecker(pc PermissionChecker) {
	fp.permChecker = pc
}

// PlanAndExecute takes a federated query descriptor and executes it.
// The query descriptor specifies sub-queries per source and a join key.
type FederatedQueryRequest struct {
	OrganizationID uuid.UUID  `json:"organization_id"`
	UserID         uuid.UUID  `json:"user_id"`
	SubQueries     []SubQuery `json:"sub_queries"`
	JoinKey        string     `json:"join_key"`
}

// SubQuery is a single query targeted at a specific source.
type SubQuery struct {
	Source string `json:"source"` // e.g., "work", "ops", "git", "postgres://..."
	Query  string `json:"query"`
	Alias  string `json:"alias"`
}

// Execute runs the federated query plan.
func (fp *FederatedPlanner) Execute(ctx context.Context, req *FederatedQueryRequest) (*FederatedQueryResult, error) {
	if len(req.SubQueries) == 0 {
		return nil, fmt.Errorf("at least one sub-query is required")
	}

	results := make(map[string]*FederatedQueryResult)
	var sources []string

	for _, sq := range req.SubQueries {
		adapter, ok := fp.adapters[sq.Source]
		if !ok {
			return nil, fmt.Errorf("no adapter registered for source %q", sq.Source)
		}

		// §12.1 (C9): if the adapter accepts per-request org context,
		// stamp it so any outbound HTTP calls carry an org-scoping
		// header. Adapters that don't care can skip the interface.
		if setter, ok := adapter.(OrgContextSetter); ok && req.OrganizationID != uuid.Nil {
			setter.SetOrgContext(req.OrganizationID)
		}
		// §12.1: warehouse adapters (Snowflake/BigQuery/ClickHouse) use
		// the richer RLSContextSetter to stamp BOTH org and user into
		// engine-native session variables / query labels so row policies
		// can enforce tenant + actor boundaries.
		if setter, ok := adapter.(RLSContextSetter); ok && req.OrganizationID != uuid.Nil {
			setter.SetRLSContext(req.OrganizationID, req.UserID)
		}

		result, err := adapter.Execute(ctx, sq.Query)
		if err != nil {
			return nil, fmt.Errorf("sub-query on %s failed: %w", sq.Source, err)
		}

		// Filter out columns the user is not allowed to read.
		if fp.permChecker != nil {
			result = fp.filterColumns(ctx, req.UserID, sq.Source, result)
		}

		alias := sq.Alias
		if alias == "" {
			alias = sq.Source
		}
		results[alias] = result
		sources = append(sources, sq.Source)
	}

	if len(results) == 1 {
		for _, r := range results {
			r.Sources = sources
			return r, nil
		}
	}

	if req.JoinKey == "" {
		return fp.concatenateResults(results, sources)
	}

	return fp.hashJoinResults(results, req.JoinKey, sources)
}

// concatenateResults appends all result rows together with source prefix on columns.
func (fp *FederatedPlanner) concatenateResults(results map[string]*FederatedQueryResult, sources []string) (*FederatedQueryResult, error) {
	merged := &FederatedQueryResult{Sources: sources}
	colSet := make(map[string]bool)

	for alias, result := range results {
		for _, col := range result.Columns {
			prefixed := alias + "." + col
			if !colSet[prefixed] {
				merged.Columns = append(merged.Columns, prefixed)
				colSet[prefixed] = true
			}
		}
		for _, row := range result.Rows {
			prefixed := make(map[string]any)
			for k, v := range row {
				prefixed[alias+"."+k] = v
			}
			merged.Rows = append(merged.Rows, prefixed)
		}
	}
	return merged, nil
}

// hashJoinResults performs an in-memory hash join on the specified key.
func (fp *FederatedPlanner) hashJoinResults(results map[string]*FederatedQueryResult, joinKey string, sources []string) (*FederatedQueryResult, error) {
	aliases := make([]string, 0, len(results))
	for alias := range results {
		aliases = append(aliases, alias)
	}

	if len(aliases) < 2 {
		return fp.concatenateResults(results, sources)
	}

	// Build hash index on the first result set
	leftAlias := aliases[0]
	leftResult := results[leftAlias]
	leftIndex := make(map[string][]map[string]any)
	for _, row := range leftResult.Rows {
		key := fmt.Sprintf("%v", row[joinKey])
		leftIndex[key] = append(leftIndex[key], row)
	}

	merged := &FederatedQueryResult{Sources: sources}
	colSet := make(map[string]bool)
	for _, col := range leftResult.Columns {
		prefixed := leftAlias + "." + col
		merged.Columns = append(merged.Columns, prefixed)
		colSet[prefixed] = true
	}

	// Join with each subsequent result set
	for _, rightAlias := range aliases[1:] {
		rightResult := results[rightAlias]
		for _, col := range rightResult.Columns {
			prefixed := rightAlias + "." + col
			if !colSet[prefixed] {
				merged.Columns = append(merged.Columns, prefixed)
				colSet[prefixed] = true
			}
		}

		for _, rightRow := range rightResult.Rows {
			key := fmt.Sprintf("%v", rightRow[joinKey])
			leftRows, ok := leftIndex[key]
			if !ok {
				continue
			}
			for _, leftRow := range leftRows {
				joinedRow := make(map[string]any)
				for k, v := range leftRow {
					joinedRow[leftAlias+"."+k] = v
				}
				for k, v := range rightRow {
					joinedRow[rightAlias+"."+k] = v
				}
				merged.Rows = append(merged.Rows, joinedRow)
			}
		}
	}

	return merged, nil
}

// SimplifyJoinKey removes alias prefix from a join key if present.
func SimplifyJoinKey(joinKey string) string {
	parts := strings.SplitN(joinKey, ".", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return joinKey
}

// filterColumns removes columns the user is not allowed to access from
// both the Columns list and every row in the result set. The source is
// passed through to the PermissionChecker; the "table" argument is
// derived from the column name (if it contains a dot) or left empty.
// §12.4 (C8): dropped columns are audit-logged so operators can spot
// over-broad queries without re-running them.
func (fp *FederatedPlanner) filterColumns(ctx context.Context, userID uuid.UUID, source string, result *FederatedQueryResult) *FederatedQueryResult {
	if result == nil {
		return result
	}

	// Determine which columns are allowed.
	allowed := make(map[string]bool, len(result.Columns))
	var denied []string
	for _, col := range result.Columns {
		table, column := splitTableColumn(col)
		if fp.permChecker.CanAccessColumn(ctx, userID, source, table, column) {
			allowed[col] = true
			continue
		}
		denied = append(denied, col)
	}

	if len(denied) > 0 && fp.logger != nil {
		fp.logger.InfoContext(ctx, "federated planner dropped restricted columns",
			"source", source,
			"user_id", userID.String(),
			"denied_columns", denied,
		)
	}

	// Rebuild the column list.
	filtered := &FederatedQueryResult{
		Sources: result.Sources,
	}
	for _, col := range result.Columns {
		if allowed[col] {
			filtered.Columns = append(filtered.Columns, col)
		}
	}

	// Rebuild each row, keeping only allowed columns.
	for _, row := range result.Rows {
		newRow := make(map[string]any, len(filtered.Columns))
		for k, v := range row {
			if allowed[k] {
				newRow[k] = v
			}
		}
		filtered.Rows = append(filtered.Rows, newRow)
	}

	return filtered
}

// splitTableColumn splits a "table.column" string into table and column
// parts. If there is no dot, table is empty and column is the full string.
func splitTableColumn(col string) (string, string) {
	parts := strings.SplitN(col, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", col
}

// -------------------------------------------------------------------
// §12.4 — planner decomposer
//
// Plan accepts a natural-language prompt OR raw SQL and returns a
// FederatedQueryRequest whose SubQueries target the right per-source
// adapters. The caller then hands that request to Execute. Splitting
// Plan from Execute lets operators preview the decomposition before
// committing (POST /federated-queries/plan) or persist it for audit.
// -------------------------------------------------------------------

// PlanRequest is the entry point for the decomposer. Exactly one of
// NLQuery or SQL must be set; Sources narrows the catalog to a subset
// of known data sources (e.g. when the caller has filtered by
// permission already).
type PlanRequest struct {
	OrganizationID uuid.UUID
	UserID         uuid.UUID

	// NLQuery is the natural-language intent. When set, the planner
	// delegates per-source SQL generation to the foundry nl_to_sql
	// endpoint via NLDelegate.
	NLQuery string

	// SQL is a raw SQL statement the caller wrote themselves. When set,
	// the planner parses the FROM/JOIN clauses to pick sources and
	// splits the statement into per-source sub-queries.
	SQL string

	// Sources lists catalog entries the planner may target. When empty
	// the caller has supplied no catalog and the planner operates in
	// best-effort mode — it only matches tables whose source is
	// embedded in the SQL itself (e.g. `FROM snowflake_sales.orders`).
	Sources []DataSourceRef

	// JoinKey is the column the Execute phase uses to hash-join
	// sub-results. When empty, results are concatenated.
	JoinKey string
}

// DataSourceRef is the narrow catalog shape the planner consumes. It
// matches the fields of domain.DataSource the planner needs without
// pulling in a compile-time dependency on the domain package (keeps
// the usecase tree free of cyclic imports).
type DataSourceRef struct {
	// Name is the planner-facing identifier used to look up the
	// SourceAdapter registered on the FederatedPlanner.
	Name string

	// Engine is the DataEngine string (snowflake, bigquery, postgres…).
	// Used to compose the DSL prefix when decomposing SQL that doesn't
	// already carry a DSN.
	Engine string

	// DSN is the connection string the adapter requires. Sub-queries
	// carry this as `<dsn> :: <sql>` so the adapter can open a fresh
	// connection without a second lookup.
	DSN string

	// Schema is the default schema to use when a table reference is
	// unqualified (`FROM orders` rather than `FROM sales.orders`).
	Schema string

	// Tables is the list of tables this source exposes. The planner
	// consults it to route table references to the right source.
	Tables []string
}

// NLDelegate turns a natural-language prompt + per-source metadata into
// a concrete SQL statement. Implementations call foundry-service's
// nl_to_sql endpoint. A nil delegate disables the NL path — Plan
// returns an error instead of silently producing nothing.
type NLDelegate interface {
	// NLToSQL converts prompt + per-source vocabulary/schema into a SQL
	// string targeting the given source. The returned SQL is passed
	// straight to the adapter via the sub-query DSL.
	NLToSQL(ctx context.Context, prompt string, source DataSourceRef) (string, error)
}

// SetNLDelegate wires the foundry-backed NL decomposer. Safe to call
// once at boot.
func (fp *FederatedPlanner) SetNLDelegate(d NLDelegate) {
	fp.nlDelegate = d
}

// Plan decomposes a federated query request into per-source sub-queries
// without executing them. Returns a FederatedQueryRequest the caller
// can send to Execute or persist for audit. §12.4.
func (fp *FederatedPlanner) Plan(ctx context.Context, req PlanRequest) (*FederatedQueryRequest, error) {
	if strings.TrimSpace(req.NLQuery) == "" && strings.TrimSpace(req.SQL) == "" {
		return nil, fmt.Errorf("planner: one of nl_query or sql is required")
	}
	if strings.TrimSpace(req.NLQuery) != "" && strings.TrimSpace(req.SQL) != "" {
		return nil, fmt.Errorf("planner: nl_query and sql are mutually exclusive")
	}
	out := &FederatedQueryRequest{
		OrganizationID: req.OrganizationID,
		UserID:         req.UserID,
		JoinKey:        req.JoinKey,
	}
	if strings.TrimSpace(req.NLQuery) != "" {
		subs, err := fp.planNL(ctx, req)
		if err != nil {
			return nil, err
		}
		out.SubQueries = subs
		return out, nil
	}
	subs, err := fp.planSQL(ctx, req)
	if err != nil {
		return nil, err
	}
	out.SubQueries = subs
	return out, nil
}

// planNL fans out the natural-language prompt across the supplied
// catalog. Each source gets its own NLToSQL call so the delegate can
// pass source-specific vocabulary + schema context. A failure on a
// single source aborts the whole plan — partial plans produce
// misleading federated results.
func (fp *FederatedPlanner) planNL(ctx context.Context, req PlanRequest) ([]SubQuery, error) {
	if fp.nlDelegate == nil {
		return nil, fmt.Errorf("planner: nl_query requires an NLDelegate; configure foundry nl_to_sql endpoint")
	}
	if len(req.Sources) == 0 {
		return nil, fmt.Errorf("planner: nl_query requires at least one catalog source")
	}
	out := make([]SubQuery, 0, len(req.Sources))
	for _, src := range req.Sources {
		sqlText, err := fp.nlDelegate.NLToSQL(ctx, req.NLQuery, src)
		if err != nil {
			return nil, fmt.Errorf("planner: nl_to_sql on %s: %w", src.Name, err)
		}
		sqlText = strings.TrimSpace(sqlText)
		if sqlText == "" {
			continue
		}
		out = append(out, SubQuery{
			Source: src.Name,
			Query:  buildSubQueryDSL(src, sqlText),
			Alias:  src.Name,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("planner: nl_to_sql produced no sub-queries for the supplied sources")
	}
	return out, nil
}

// planSQL parses the caller-supplied SQL, walks the FROM/JOIN list, and
// groups tables by the data source that owns them. Each group becomes a
// single sub-query against that source.
//
// This is intentionally a light parser — enough to route the common
// "SELECT … FROM a JOIN b" shape. Operators with advanced needs
// (CTEs, subqueries, unions) should either write the SubQueries by
// hand or run through the NL path.
func (fp *FederatedPlanner) planSQL(ctx context.Context, req PlanRequest) ([]SubQuery, error) {
	tables, err := extractTablesForPlan(req.SQL)
	if err != nil {
		return nil, fmt.Errorf("planner: parse sql: %w", err)
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("planner: sql has no FROM clause the planner can route")
	}
	// Map each referenced table to the data source that owns it.
	catalog := indexCatalog(req.Sources)
	grouped := map[string][]string{} // source name → tables referenced on that source
	unresolved := []string{}
	for _, t := range tables {
		srcName, ok := catalog[strings.ToLower(t)]
		if !ok {
			unresolved = append(unresolved, t)
			continue
		}
		grouped[srcName] = append(grouped[srcName], t)
	}
	if len(unresolved) > 0 && len(grouped) == 0 {
		return nil, fmt.Errorf("planner: no catalog source owns tables %v", unresolved)
	}
	// Produce sub-queries deterministically so snapshot tests stay stable.
	names := make([]string, 0, len(grouped))
	for n := range grouped {
		names = append(names, n)
	}
	sort.Strings(names)
	subs := make([]SubQuery, 0, len(names))
	// Index sources by name so we can look up DSN/engine on the
	// emitted sub-query.
	sourcesByName := make(map[string]DataSourceRef, len(req.Sources))
	for _, s := range req.Sources {
		sourcesByName[s.Name] = s
	}
	for _, name := range names {
		src := sourcesByName[name]
		// When only one source was routed, the original SQL flows
		// through unchanged. Otherwise, rewrite to reference only the
		// tables that belong to this source by projecting from each
		// table in turn — callers can still JOIN via JoinKey.
		var sqlText string
		if len(names) == 1 {
			sqlText = req.SQL
		} else {
			sqlText = projectionFor(grouped[name])
		}
		subs = append(subs, SubQuery{
			Source: name,
			Query:  buildSubQueryDSL(src, sqlText),
			Alias:  name,
		})
	}
	return subs, nil
}

// buildSubQueryDSL composes the `<dsn> :: <sql>` form expected by the
// warehouse adapters (Snowflake, BigQuery, ClickHouse). Sources without
// a DSN (the Sentiae cross-domain adapters) get the raw SQL.
func buildSubQueryDSL(src DataSourceRef, sqlText string) string {
	if strings.TrimSpace(src.DSN) == "" {
		return sqlText
	}
	return fmt.Sprintf("%s :: %s", src.DSN, sqlText)
}

// projectionFor builds a trivial `SELECT * FROM <t1>, <t2>, …` so each
// per-source sub-query returns a materialised result the planner can
// then join. This keeps the multi-source path truthful without trying
// to rewrite complex predicates on the fly — the caller's intent is
// captured in the original SQL, but execution is per-source.
func projectionFor(tables []string) string {
	sort.Strings(tables)
	return "SELECT * FROM " + strings.Join(tables, ", ")
}

// tableRefRe matches `FROM table` and `JOIN table` occurrences. It
// recognises dotted names (schema.table) and bracketed quote styles
// (`"orders"`, `` `orders` ``, `[orders]`). Good enough for the happy
// path — CTE'd / subquery SQL should be decomposed by hand.
var tableRefRe = regexp.MustCompile(`(?i)(?:\bFROM\b|\bJOIN\b)\s+([\w\."\x60\[\]]+)`)

// extractTablesForPlan returns the de-duped list of tables referenced by the
// FROM/JOIN clauses of sql. Ordering is preserved so the returned slice
// can drive a deterministic sub-query plan.
func extractTablesForPlan(sql string) ([]string, error) {
	seen := make(map[string]struct{})
	var tables []string
	matches := tableRefRe.FindAllStringSubmatch(sql, -1)
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		t := strings.Trim(m[1], "`\"[]")
		if t == "" {
			continue
		}
		key := strings.ToLower(t)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		tables = append(tables, t)
	}
	return tables, nil
}

// indexCatalog builds a lookup from table name → source name. When a
// table name is ambiguous (same name in two sources) the FIRST match
// wins — operators must rename tables or pick a source explicitly via
// a schema-qualified reference in the SQL.
func indexCatalog(sources []DataSourceRef) map[string]string {
	out := make(map[string]string)
	for _, src := range sources {
		for _, t := range src.Tables {
			key := strings.ToLower(t)
			if _, ok := out[key]; !ok {
				out[key] = src.Name
			}
			// Also index schema-qualified and source-qualified forms so
			// the caller can write `snowflake_sales.orders` or
			// `public.orders` and get a hit.
			if src.Schema != "" {
				qualified := strings.ToLower(src.Schema + "." + t)
				if _, ok := out[qualified]; !ok {
					out[qualified] = src.Name
				}
			}
			scoped := strings.ToLower(src.Name + "." + t)
			if _, ok := out[scoped]; !ok {
				out[scoped] = src.Name
			}
		}
	}
	return out
}
