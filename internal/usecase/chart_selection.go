package usecase

import (
	"strings"
	"time"
)

// ChartType is the render hint a dashboard caller passes to the portal.
// §12.5 gap-closure: previously every query landed as a flat table; the
// portal had to guess whether a time series should render as a line
// chart. This helper looks at the query result's column shape and
// returns a deterministic "best chart for this data" recommendation.
type ChartType string

const (
	ChartTypeTable   ChartType = "table"
	ChartTypeBar     ChartType = "bar"
	ChartTypeLine    ChartType = "line"
	ChartTypeArea    ChartType = "area"
	ChartTypeScatter ChartType = "scatter"
	ChartTypePie     ChartType = "pie"
	ChartTypeKPI     ChartType = "kpi"     // single-cell metric
	ChartTypeHeatmap ChartType = "heatmap" // two categoricals + one numeric
)

// ColumnMeta describes a single column in a query result. Type is the
// SQL type string reported by the driver (lowercased); the heuristic
// normalises common dialects (postgres `timestamp with time zone`,
// snowflake `TIMESTAMP_LTZ`, bigquery `DATETIME`).
type ColumnMeta struct {
	Name string
	Type string
}

// ChartSuggestion is the recommendation the API returns.
type ChartSuggestion struct {
	Chart  ChartType `json:"chart"`
	XField string    `json:"x_field,omitempty"`
	YField string    `json:"y_field,omitempty"`
	// Confidence ∈ [0, 1]. 0.9 = unambiguous (one timestamp + one
	// numeric → line). 0.5 = fallback (table). Lets the portal show
	// "Charts suggested: …" rather than silently picking.
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason"`
}

// SuggestChart returns a chart type given the result's column shape
// and an optional row-count hint. The rules are simple on purpose;
// a machine-learning-flavoured version can replace this when there's
// enough training data. The output is deterministic so callers can
// cache based on column shape.
//
// Rules (first match wins):
//
//	1 column × 1 row (numeric)         → KPI
//	1 column                           → table
//	timestamp/date + 1 numeric         → line    (or area when "cumulative")
//	timestamp/date + >1 numeric        → line    (multi-series)
//	string category (< 12 uniques) + 1 numeric → bar
//	string category + >1 numeric       → grouped bar (returned as bar)
//	string × string + 1 numeric        → heatmap
//	2 numerics                         → scatter
//	fallback                           → table
//
// Time series priority: any column whose type matches the time-style
// check is treated as X before falling through to category detection.
func SuggestChart(cols []ColumnMeta, rowCount int, hints map[string]string) ChartSuggestion {
	if len(cols) == 0 {
		return ChartSuggestion{Chart: ChartTypeTable, Confidence: 0.5, Reason: "no columns"}
	}
	if rowCount == 1 && len(cols) == 1 && isNumeric(cols[0].Type) {
		return ChartSuggestion{
			Chart: ChartTypeKPI, YField: cols[0].Name, Confidence: 0.95,
			Reason: "single numeric cell",
		}
	}

	// Split columns by semantic bucket.
	var timeCols, numericCols, categoryCols []ColumnMeta
	for _, c := range cols {
		switch {
		case isTimestamp(c.Type):
			timeCols = append(timeCols, c)
		case isNumeric(c.Type):
			numericCols = append(numericCols, c)
		default:
			categoryCols = append(categoryCols, c)
		}
	}

	// Time series.
	if len(timeCols) == 1 && len(numericCols) >= 1 {
		chart := ChartTypeLine
		if hints["cumulative"] == "true" {
			chart = ChartTypeArea
		}
		reason := "timestamp + numeric → time series"
		if len(numericCols) > 1 {
			reason = "timestamp + multiple numerics → multi-series line"
		}
		return ChartSuggestion{
			Chart: chart, XField: timeCols[0].Name, YField: numericCols[0].Name,
			Confidence: 0.9, Reason: reason,
		}
	}

	// Categorical × numeric.
	if len(categoryCols) == 1 && len(numericCols) >= 1 {
		// Pie is rarely a good choice at scale; use bar unless the
		// caller explicitly hinted at pie AND rowCount ≤ 8.
		if hints["style"] == "pie" && rowCount > 0 && rowCount <= 8 {
			return ChartSuggestion{
				Chart: ChartTypePie, XField: categoryCols[0].Name, YField: numericCols[0].Name,
				Confidence: 0.7, Reason: "pie requested with few slices",
			}
		}
		return ChartSuggestion{
			Chart: ChartTypeBar, XField: categoryCols[0].Name, YField: numericCols[0].Name,
			Confidence: 0.85, Reason: "category + numeric → bar",
		}
	}

	// Two categoricals + one numeric.
	if len(categoryCols) == 2 && len(numericCols) == 1 {
		return ChartSuggestion{
			Chart: ChartTypeHeatmap, XField: categoryCols[0].Name, YField: numericCols[0].Name,
			Confidence: 0.7, Reason: "two categoricals + numeric → heatmap",
		}
	}

	// Two numerics — scatter.
	if len(numericCols) == 2 && len(categoryCols) == 0 && len(timeCols) == 0 {
		return ChartSuggestion{
			Chart: ChartTypeScatter, XField: numericCols[0].Name, YField: numericCols[1].Name,
			Confidence: 0.8, Reason: "two numerics → scatter",
		}
	}

	return ChartSuggestion{Chart: ChartTypeTable, Confidence: 0.4, Reason: "no clear shape match, defaulting to table"}
}

// isTimestamp matches SQL types that behave like a point in time.
// Accepts multi-dialect spellings (postgres, snowflake, bigquery, mssql).
func isTimestamp(raw string) bool {
	t := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case t == "timestamp", t == "timestamptz", t == "datetime",
		t == "date", t == "time",
		strings.Contains(t, "timestamp"),
		strings.Contains(t, "datetime"):
		return true
	}
	return false
}

// isNumeric matches SQL types that are orderable scalars suitable for
// the Y axis.
func isNumeric(raw string) bool {
	t := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case t == "int", t == "integer", t == "bigint", t == "smallint", t == "tinyint",
		t == "float", t == "double", t == "double precision", t == "real",
		t == "number", t == "numeric", t == "decimal",
		strings.Contains(t, "int"),
		strings.Contains(t, "float"),
		strings.Contains(t, "numeric"),
		strings.Contains(t, "decimal"),
		strings.HasPrefix(t, "number("):
		return true
	}
	return false
}

// ensure time import is used.
var _ = time.Now
