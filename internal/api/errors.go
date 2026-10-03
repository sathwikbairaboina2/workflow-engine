package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/sathwikbairaboina2/workflow-engine/internal/core"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

var statusByCode = map[string]int{
	core.CodeInvalid:        http.StatusBadRequest,
	core.CodeNotFound:       http.StatusNotFound,
	core.CodeAlreadyStarted: http.StatusConflict,
	core.CodeStaleLease:     http.StatusConflict,
	core.CodeRunClosed:      http.StatusConflict,
	core.CodeConflict:       http.StatusConflict,
	core.CodeTooLarge:       http.StatusRequestEntityTooLarge,
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("write response", "err", err)
	}
}

func writeError(w http.ResponseWriter, code string, status int, msg, runID string) {
	writeJSON(w, status, wire.ErrorBody{Error: wire.ErrorDetail{Code: code, Message: msg, RunID: runID}})
}

// writeErr maps an engine error to a response; anything unknown becomes a generic 500 and is only logged.
func writeErr(w http.ResponseWriter, err error) {
	var ce *core.Error
	if errors.As(err, &ce) {
		status, ok := statusByCode[ce.Code]
		if !ok {
			status = http.StatusInternalServerError
		}
		writeError(w, ce.Code, status, ce.Message, ce.RunID)
		return
	}
	slog.Error("internal error", "err", err)
	writeError(w, "internal", http.StatusInternalServerError, "internal error", "")
}
