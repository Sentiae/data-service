package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sentiae/data-service/internal/domain"
	"github.com/sentiae/platform-kit/dsl"
	"gorm.io/gorm"
)

// DSLHandler exposes `POST /dsl/execute` for the foundry flow worker.
// Mirrors the platform-kit/dsl shape used by work-service / git-service /
// ops-service so flow steps can dispatch into data-service uniformly.
//
// Registered actions:
//
//   - query: executes `config.sql` against the data source identified by
//     `config.data_source_id` (or, if `config.dsn` + `config.engine` are
//     provided directly, against that connection). Returns
//     `{ "columns": [...], "rows": [...], "row_count": N }`.
//
// Unknown actions intentionally return 202 with an empty body so the
// flow worker can no-op forward-compat steps without failing the run.
type DSLHandler struct {
	db *gorm.DB

	mu      sync.RWMutex
	actions map[string]dsl.Action
}

// NewDSLHandler wires the handler with the database used by query.
func NewDSLHandler(db *gorm.DB) *DSLHandler {
	h := &DSLHandler{db: db, actions: make(map[string]dsl.Action)}
	h.actions["query"] = h.runQuery
	return h
}

// RegisterRoutes mounts the endpoint under the data sub-route used by
// the foundry per-service action registry.
func (h *DSLHandler) RegisterRoutes(r chi.Router) {
	r.Route("/data", func(dr chi.Router) {
		dr.Post("/dsl/execute", h.serveHTTP)
	})
}

// serveHTTP parses the request, dispatches to the registered action, and
// emits the platform-kit/dsl response shape. Unknown actions return 202.
func (h *DSLHandler) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req dsl.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeDSLError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Action == "" {
		writeDSLError(w, http.StatusBadRequest, "action is required")
		return
	}

	h.mu.RLock()
	action, ok := h.actions[req.Action]
	h.mu.RUnlock()
	if !ok {
		// Unknown action — accept and no-op for forward compatibility.
		w.WriteHeader(http.StatusAccepted)
		return
	}

	output, err := action(r.Context(), req)
	if err != nil {
		writeDSLJSON(w, http.StatusInternalServerError, dsl.Response{Error: err.Error()})
		return
	}
	writeDSLJSON(w, http.StatusOK, dsl.Response{Output: output, Completed: true})
}

// runQuery is the `query` action implementation. It reuses the same
// database/sql path the production Execute handler uses so behaviour
// stays consistent across DSL-driven and human-driven runs.
func (h *DSLHandler) runQuery(ctx context.Context, req dsl.Request) (map[string]any, error) {
	sqlText, _ := req.Config["sql"].(string)
	if sqlText == "" {
		return nil, &dsl.ActionError{Msg: "config.sql is required"}
	}

	driver, dsn, err := h.resolveConnection(ctx, req.Config)
	if err != nil {
		return nil, err
	}

	if driver == "" || dsn == "" {
		// Nothing to actually execute against — return an empty result
		// rather than failing the flow step.
		return map[string]any{
			"columns":   []string{},
			"rows":      []map[string]any{},
			"row_count": 0,
			"message":   "no DSN configured — query not executed",
		}, nil
	}

	conn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, &dsl.ActionError{Msg: "connection failed: " + err.Error()}
	}
	defer conn.Close()

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := conn.QueryContext(queryCtx, sqlText)
	if err != nil {
		return nil, &dsl.ActionError{Msg: "query failed: " + err.Error()}
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	resultRows := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, &dsl.ActionError{Msg: "scan failed: " + err.Error()}
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			row[c] = values[i]
		}
		resultRows = append(resultRows, row)
	}

	return map[string]any{
		"columns":   cols,
		"rows":      resultRows,
		"row_count": len(resultRows),
	}, nil
}

// resolveConnection returns the SQL driver + DSN to use. The caller may
// either point at a stored DataSource (config.data_source_id) or pass
// raw connection details (config.engine + config.dsn).
func (h *DSLHandler) resolveConnection(ctx context.Context, cfg map[string]any) (string, string, error) {
	if dsRaw, ok := cfg["data_source_id"].(string); ok && dsRaw != "" {
		dsID, err := uuid.Parse(dsRaw)
		if err != nil {
			return "", "", &dsl.ActionError{Msg: "invalid data_source_id"}
		}
		var ds domain.DataSource
		if err := h.db.WithContext(ctx).Where("id = ?", dsID).First(&ds).Error; err != nil {
			return "", "", &dsl.ActionError{Msg: "data source not found"}
		}
		return driverForEngine(ds.Engine), ds.ConnectionDSN, nil
	}
	dsn, _ := cfg["dsn"].(string)
	engine, _ := cfg["engine"].(string)
	if engine == "" {
		return "", dsn, nil
	}
	return driverForEngine(domain.DataEngine(engine)), dsn, nil
}

func writeDSLJSON(w http.ResponseWriter, status int, body dsl.Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeDSLError(w http.ResponseWriter, status int, msg string) {
	writeDSLJSON(w, status, dsl.Response{Error: msg})
}
