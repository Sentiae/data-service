package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	sqlitedriver "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// fakeSelectorLLM lets tests stage a ranking outcome without reaching
// foundry-service. When err is non-nil the selector falls back to the
// deterministic keyword path.
type fakeSelectorLLM struct {
	ranks []DataSourceRank
	err   error
	seen  RankDataSourcesInput
}

func (f *fakeSelectorLLM) RankDataSources(ctx context.Context, in RankDataSourcesInput) ([]DataSourceRank, error) {
	f.seen = in
	if f.err != nil {
		return nil, f.err
	}
	return f.ranks, nil
}

// newSelectorTestDB stands up an in-memory SQLite DB with just the
// tables the selector reads. SQLite won't accept postgres-specific
// column types (jsonb, ltree) so we hand-roll a minimal schema that
// matches the columns the usecase touches.
func newSelectorTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlitedriver.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE data_sources (
			id TEXT PRIMARY KEY,
			organization_id TEXT,
			name TEXT, description TEXT,
			engine TEXT,
			connection_id TEXT, connection_dsn TEXT,
			schema TEXT,
			tables TEXT,
			status TEXT,
			last_sync_at DATETIME,
			created_by TEXT,
			created_at DATETIME, updated_at DATETIME
		)`,
		`CREATE TABLE semantic_fields (
			id TEXT PRIMARY KEY,
			data_source_id TEXT, table_name TEXT, column_name TEXT,
			business_name TEXT, description TEXT, data_type TEXT,
			aggregation TEXT, unit TEXT, tags TEXT,
			synonyms TEXT, aliases TEXT, required_role TEXT,
			created_at DATETIME, updated_at DATETIME
		)`,
	} {
		if err := db.Exec(stmt).Error; err != nil {
			t.Fatalf("exec schema: %v", err)
		}
	}
	return db
}

func seedSource(t *testing.T, db *gorm.DB, orgID uuid.UUID, name string, tables ...string) domain.DataSource {
	t.Helper()
	ds := domain.DataSource{
		ID:             uuid.New(),
		OrganizationID: orgID,
		Name:           name,
		Engine:         domain.DataEnginePostgres,
		Tables:         domain.StringArray(tables),
		Status:         domain.DataSourceStatusConnected,
		CreatedBy:      uuid.New(),
	}
	if err := db.Create(&ds).Error; err != nil {
		t.Fatalf("seed source: %v", err)
	}
	return ds
}

func TestSelectForQuestion_RequiresQuestion(t *testing.T) {
	db := newSelectorTestDB(t)
	sel := NewDataSourceSelector(db, nil)
	_, err := sel.SelectForQuestion(context.Background(), uuid.New(), uuid.New(), "   ")
	if err == nil {
		t.Fatalf("expected error for empty question")
	}
}

func TestSelectForQuestion_NoSources(t *testing.T) {
	db := newSelectorTestDB(t)
	sel := NewDataSourceSelector(db, nil)
	ranks, err := sel.SelectForQuestion(context.Background(), uuid.New(), uuid.New(), "total orders")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ranks != nil {
		t.Fatalf("expected nil ranks, got %d", len(ranks))
	}
}

func TestSelectForQuestion_LLMReturnsRanks(t *testing.T) {
	db := newSelectorTestDB(t)
	orgID := uuid.New()
	a := seedSource(t, db, orgID, "warehouse", "orders", "customers")
	b := seedSource(t, db, orgID, "marketing", "campaigns")
	fake := &fakeSelectorLLM{
		ranks: []DataSourceRank{
			{DataSourceID: b.ID, Confidence: 0.3, Reasoning: "no sales tables"},
			{DataSourceID: a.ID, Confidence: 0.95, Reasoning: "has orders table"},
		},
	}
	sel := NewDataSourceSelector(db, fake)
	ranks, err := sel.SelectForQuestion(context.Background(), orgID, uuid.New(), "how many orders last week?")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(ranks) != 2 {
		t.Fatalf("expected 2 ranks, got %d", len(ranks))
	}
	if ranks[0].DataSourceID != a.ID {
		t.Errorf("expected warehouse first, got %s", ranks[0].Name)
	}
	if ranks[0].Confidence <= ranks[1].Confidence {
		t.Errorf("expected ranks sorted desc: %v", ranks)
	}
	if fake.seen.Question == "" {
		t.Errorf("llm was never called")
	}
}

func TestSelectForQuestion_FallbackOnLLMError(t *testing.T) {
	db := newSelectorTestDB(t)
	orgID := uuid.New()
	seedSource(t, db, orgID, "customers_db", "customers", "addresses")
	seedSource(t, db, orgID, "orders_db", "orders", "order_lines")
	fake := &fakeSelectorLLM{err: errors.New("llm down")}
	sel := NewDataSourceSelector(db, fake)
	ranks, err := sel.SelectForQuestion(context.Background(), orgID, uuid.New(), "count orders today")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(ranks) != 2 {
		t.Fatalf("expected 2 ranks, got %d", len(ranks))
	}
	// Keyword fallback should put orders_db (matches "orders") ahead.
	if ranks[0].Name != "orders_db" {
		t.Errorf("expected orders_db first under fallback, got %q (full=%#v)", ranks[0].Name, ranks)
	}
}

func TestSelectForQuestion_DropsHallucinatedIDs(t *testing.T) {
	db := newSelectorTestDB(t)
	orgID := uuid.New()
	a := seedSource(t, db, orgID, "warehouse", "orders")
	bogus := uuid.New() // not in DB
	fake := &fakeSelectorLLM{
		ranks: []DataSourceRank{
			{DataSourceID: bogus, Confidence: 0.99, Reasoning: "hallucinated"},
			{DataSourceID: a.ID, Confidence: 0.7, Reasoning: "valid"},
		},
	}
	sel := NewDataSourceSelector(db, fake)
	ranks, err := sel.SelectForQuestion(context.Background(), orgID, uuid.New(), "orders today")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(ranks) != 1 {
		t.Fatalf("expected hallucinated id dropped, got %#v", ranks)
	}
	if ranks[0].DataSourceID != a.ID {
		t.Errorf("unexpected survivor: %v", ranks[0])
	}
}

func TestParseRanksJSON_AcceptsCodeFence(t *testing.T) {
	id := uuid.New()
	raw := "```json\n[{\"data_source_id\":\"" + id.String() + "\",\"confidence\":0.9,\"reasoning\":\"ok\"}]\n```"
	out, err := ParseRanksJSON(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out) != 1 || out[0].DataSourceID != id {
		t.Errorf("bad decode: %#v", out)
	}
	if out[0].Confidence != 0.9 {
		t.Errorf("confidence: %v", out[0].Confidence)
	}
}

func TestParseRanksJSON_RejectsEmpty(t *testing.T) {
	if _, err := ParseRanksJSON(""); err == nil {
		t.Errorf("expected error for empty input")
	}
}

func TestKeywordRank_ScoresOverlap(t *testing.T) {
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	candidates := []DataSourceSchemaSummary{
		{ID: ids[0], Name: "sales", Schema: "orders(id, total, customer_id)"},
		{ID: ids[1], Name: "blog", Schema: "posts(id, title, body)"},
	}
	ranks := keywordRankDataSources("how many orders today", candidates)
	if ranks[0].DataSourceID != ids[0] {
		t.Errorf("expected sales first, got %v", ranks)
	}
	if ranks[0].Confidence <= ranks[1].Confidence {
		t.Errorf("expected sales conf > blog conf; got %v", ranks)
	}
}
