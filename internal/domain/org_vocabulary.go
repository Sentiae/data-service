package domain

import (
	"time"

	"github.com/google/uuid"
)

// OrgVocabulary is a per-organization business-term dictionary that
// backs the semantic-annotation UI (§12.2). Each row maps a concrete
// data-source column — identified by column_id — to a business term,
// synonyms, and a free-form description. Rows live alongside
// SemanticField but capture the "human-readable glossary" angle: a
// term that may span multiple columns, or a user-edited note that
// hasn't yet been promoted into the semantic layer.
//
// Rationale: the schema editor UI (out of scope for the backend PR)
// needs a single surface it can CRUD without colliding with NL→SQL's
// SemanticField rows. Storing the vocabulary entries in their own
// table keeps the two concerns separable and keeps ownership clear:
// SemanticField is authored by data owners for NL→SQL; OrgVocabulary
// is authored by any user with annotate permission.
type OrgVocabulary struct {
	ID             uuid.UUID `json:"id" gorm:"type:uuid;primary_key"`
	OrganizationID uuid.UUID `json:"organization_id" gorm:"type:uuid;not null;index:idx_org_vocab_org"`

	// ColumnID is the stable identifier for the column this annotation
	// refers to. Format is caller-defined ("<data_source_id>.<table>.<column>"
	// is the conventional shape) so operators can use it as a foreign
	// key to whatever schema-inventory they already maintain.
	ColumnID string `json:"column_id" gorm:"type:varchar(500);not null;index:idx_org_vocab_column"`

	// BusinessTerm is the human-friendly label ("Monthly Recurring Revenue").
	BusinessTerm string `json:"business_term" gorm:"type:varchar(255);not null"`

	// Synonyms are alternate terms the term might go by. Stored as JSON
	// so the portal can round-trip arrays without a join table.
	Synonyms StringArray `json:"synonyms,omitempty" gorm:"type:jsonb;serializer:json"`

	// Aliases are additional NL synonyms distinct from the
	// "display synonyms" list above. §12.2 (annotation upgrade) reserves
	// this column so the NL→SQL prompt and the UI can treat them
	// differently — e.g. aliases feed fuzzy-match resolution while
	// synonyms render next to the business term.
	Aliases StringArray `json:"aliases,omitempty" gorm:"type:jsonb;serializer:json"`

	// Unit is the display unit for numeric values referenced by the
	// term — e.g. "USD", "count", "percentage". Feeds chart axes and
	// the portal's "format as" dropdown.
	Unit string `json:"unit,omitempty" gorm:"type:varchar(50)"`

	// DataType is the logical type of the column this annotation wraps
	// — e.g. "currency", "integer", "float", "date", "boolean",
	// "string". Distinct from the raw SQL type because it carries
	// semantic information (currency vs. plain float).
	DataType string `json:"data_type,omitempty" gorm:"type:varchar(50)"`

	// Format is an optional format hint — e.g. locale ("en-US") for
	// currency, or "ISO8601" for a timestamp. The portal passes this
	// to Intl.NumberFormat / Intl.DateTimeFormat without interpreting
	// it server-side.
	Format string `json:"format,omitempty" gorm:"type:varchar(50)"`

	// Description is free-form Markdown; the portal renders it inline
	// under the business term.
	Description string `json:"description,omitempty" gorm:"type:text"`

	// CreatedBy + UpdatedBy attribute the annotation for audit + undo.
	CreatedBy uuid.UUID `json:"created_by" gorm:"type:uuid;not null"`
	UpdatedBy uuid.UUID `json:"updated_by,omitempty" gorm:"type:uuid"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName forces the per_org_vocabulary table name so migrations and
// direct SQL queries can reference it without guessing at GORM's
// pluralizer output.
func (OrgVocabulary) TableName() string {
	return "per_org_vocabulary"
}
