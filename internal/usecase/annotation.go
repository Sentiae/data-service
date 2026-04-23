package usecase

import (
	"strings"

	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"gorm.io/gorm"
)

// ResolveAnnotations returns org-scoped annotations that match `term`
// against `business_term`, `synonyms`, or `aliases` (§12.2 annotation
// upgrade).
//
// Matching strategy:
//   1. Case-insensitive exact match against business_term → return.
//   2. Exact match against any entry in synonyms[] or aliases[] JSON
//      arrays → return.
//   3. Fallback: ILIKE fuzzy match against business_term or any
//      synonym/alias entry (useful for partial user input like
//      "monthly rec").
//
// The caller feeds the returned rows into the NL→SQL prompt so the
// model sees both the canonical business term and its aliases.
func ResolveAnnotations(db *gorm.DB, orgID uuid.UUID, term string) ([]domain.OrgVocabulary, error) {
	trimmed := strings.TrimSpace(term)
	if trimmed == "" {
		return nil, nil
	}
	lower := strings.ToLower(trimmed)

	// Pull every row for the org; the org vocabulary is bounded in
	// size and this lets us run alias matching in Go without needing
	// jsonb_path_ops or similar indexes.
	var rows []domain.OrgVocabulary
	if err := db.Where("organization_id = ?", orgID).Find(&rows).Error; err != nil {
		return nil, err
	}

	// Pass 1: exact canonical match.
	exact := rows[:0:0]
	for _, r := range rows {
		if strings.EqualFold(r.BusinessTerm, trimmed) {
			exact = append(exact, r)
		}
	}
	if len(exact) > 0 {
		return exact, nil
	}

	// Pass 2: exact synonym/alias match.
	for _, r := range rows {
		if containsIgnoreCase(r.Synonyms, trimmed) || containsIgnoreCase(r.Aliases, trimmed) {
			exact = append(exact, r)
		}
	}
	if len(exact) > 0 {
		return exact, nil
	}

	// Pass 3: fuzzy ILIKE. Iterate in Go because synonyms/aliases live
	// inside JSON and are awkward to ILIKE at the SQL layer across
	// dialects.
	for _, r := range rows {
		if strings.Contains(strings.ToLower(r.BusinessTerm), lower) {
			exact = append(exact, r)
			continue
		}
		if partialMatch(r.Synonyms, lower) || partialMatch(r.Aliases, lower) {
			exact = append(exact, r)
		}
	}
	return exact, nil
}

func containsIgnoreCase(arr domain.StringArray, needle string) bool {
	for _, s := range arr {
		if strings.EqualFold(s, needle) {
			return true
		}
	}
	return false
}

func partialMatch(arr domain.StringArray, needleLower string) bool {
	for _, s := range arr {
		if strings.Contains(strings.ToLower(s), needleLower) {
			return true
		}
	}
	return false
}
