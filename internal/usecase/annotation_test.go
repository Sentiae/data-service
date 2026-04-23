package usecase

import (
	"testing"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newAnnotationTestDB spins a throw-away sqlite-in-memory DB and
// migrates the OrgVocabulary model. SQLite does not support `jsonb` but
// the GORM serializer:json tag falls back to a TEXT column, which is
// sufficient to exercise the Go-side alias resolution logic.
func newAnnotationTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&domain.OrgVocabulary{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func seedVocab(t *testing.T, db *gorm.DB, orgID uuid.UUID, rows ...domain.OrgVocabulary) {
	t.Helper()
	for i := range rows {
		if rows[i].ID == uuid.Nil {
			rows[i].ID = uuid.New()
		}
		rows[i].OrganizationID = orgID
		if err := db.Create(&rows[i]).Error; err != nil {
			t.Fatalf("seed row: %v", err)
		}
	}
}

func TestResolveAnnotations_ExactBusinessTerm(t *testing.T) {
	db := newAnnotationTestDB(t)
	org := uuid.New()
	seedVocab(t, db, org,
		domain.OrgVocabulary{BusinessTerm: "Monthly Recurring Revenue", ColumnID: "subs.mrr"},
		domain.OrgVocabulary{BusinessTerm: "Active Users", ColumnID: "users.active"},
	)

	got, err := ResolveAnnotations(db, org, "monthly recurring revenue")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 match, got %d", len(got))
	}
	if got[0].ColumnID != "subs.mrr" {
		t.Errorf("wrong row: %+v", got[0])
	}
}

func TestResolveAnnotations_AliasMatch(t *testing.T) {
	db := newAnnotationTestDB(t)
	org := uuid.New()
	seedVocab(t, db, org,
		domain.OrgVocabulary{
			BusinessTerm: "Monthly Recurring Revenue",
			ColumnID:     "subs.mrr",
			Aliases:      domain.StringArray{"MRR", "monthly revenue"},
			Unit:         "USD",
			DataType:     "currency",
		},
	)

	got, err := ResolveAnnotations(db, org, "MRR")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 alias hit, got %d", len(got))
	}
	if got[0].Unit != "USD" || got[0].DataType != "currency" {
		t.Errorf("new fields missing: %+v", got[0])
	}
}

func TestResolveAnnotations_SynonymMatch(t *testing.T) {
	db := newAnnotationTestDB(t)
	org := uuid.New()
	seedVocab(t, db, org,
		domain.OrgVocabulary{
			BusinessTerm: "Customer Churn Rate",
			ColumnID:     "metrics.churn_rate",
			Synonyms:     domain.StringArray{"attrition", "churn"},
		},
	)

	got, err := ResolveAnnotations(db, org, "attrition")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected synonym hit, got %d", len(got))
	}
}

func TestResolveAnnotations_FuzzyFallback(t *testing.T) {
	db := newAnnotationTestDB(t)
	org := uuid.New()
	seedVocab(t, db, org,
		domain.OrgVocabulary{BusinessTerm: "Monthly Recurring Revenue", ColumnID: "subs.mrr"},
	)

	got, err := ResolveAnnotations(db, org, "monthly rec")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected fuzzy match, got %d", len(got))
	}
}

func TestResolveAnnotations_NoMatch(t *testing.T) {
	db := newAnnotationTestDB(t)
	org := uuid.New()
	seedVocab(t, db, org,
		domain.OrgVocabulary{BusinessTerm: "Foo", ColumnID: "x"},
	)

	got, _ := ResolveAnnotations(db, org, "nope-no-match")
	if len(got) != 0 {
		t.Errorf("expected zero rows, got %d", len(got))
	}
}

func TestResolveAnnotations_OrgScoped(t *testing.T) {
	db := newAnnotationTestDB(t)
	orgA := uuid.New()
	orgB := uuid.New()
	seedVocab(t, db, orgA,
		domain.OrgVocabulary{BusinessTerm: "Shared Term", ColumnID: "a"},
	)
	seedVocab(t, db, orgB,
		domain.OrgVocabulary{BusinessTerm: "Shared Term", ColumnID: "b"},
	)
	got, _ := ResolveAnnotations(db, orgA, "Shared Term")
	if len(got) != 1 || got[0].ColumnID != "a" {
		t.Errorf("expected orgA scoping, got %+v", got)
	}
}
