package http

import (
	"encoding/json"
	"net/http"
)

type response struct {
	Success bool       `json:"success"`
	Data    any        `json:"data,omitempty"`
	Error   *errorInfo `json:"error,omitempty"`
}

type errorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func respondJSON(w http.ResponseWriter, code int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if data != nil {
		json.NewEncoder(w).Encode(data)
	}
}

func respondSuccess(w http.ResponseWriter, data any) {
	respondJSON(w, http.StatusOK, response{Success: true, Data: data})
}

func respondCreated(w http.ResponseWriter, data any) {
	respondJSON(w, http.StatusCreated, response{Success: true, Data: data})
}

func respondError(w http.ResponseWriter, code int, errCode, message string) {
	respondJSON(w, code, response{Success: false, Error: &errorInfo{Code: errCode, Message: message}})
}

func respondBadRequest(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusBadRequest, "BAD_REQUEST", msg)
}

func respondNotFound(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusNotFound, "NOT_FOUND", msg)
}

func respondForbidden(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusForbidden, "FORBIDDEN", msg)
}

func respondConflict(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusConflict, "CONFLICT", msg)
}

func respondInternalError(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", msg)
}
