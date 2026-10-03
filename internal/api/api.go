// Package api is the HTTP/JSON surface of the server.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/sathwikbairaboina2/workflow-engine/internal/core"
	"github.com/sathwikbairaboina2/workflow-engine/internal/metrics"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// Server routes HTTP requests to the engine.
type Server struct {
	e       *core.Engine
	m       *metrics.Metrics
	maxBody int64
}

// New builds a Server. maxBody caps the size of any request body in bytes.
func New(e *core.Engine, m *metrics.Metrics, maxBody int64) *Server {
	return &Server{e: e, m: m, maxBody: maxBody}
}

// decode reads the JSON body into v, replying with 400 or 413 itself on failure.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	body := http.MaxBytesReader(w, r.Body, s.maxBody)
	err := json.NewDecoder(body).Decode(v)
	if err == nil || errors.Is(err, io.EOF) { // an empty body is an empty request
		return true
	}
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, core.CodeTooLarge, http.StatusRequestEntityTooLarge, "request body too large", "")
		return false
	}
	writeError(w, core.CodeInvalid, http.StatusBadRequest, "malformed JSON body", "")
	return false
}

// call adapts an engine method that returns a value into a handler.
func call[Req, Resp any](s *Server, fn func(context.Context, Req) (Resp, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Req
		if !s.decode(w, r, &req) {
			return
		}
		resp, err := fn(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// callNoResult adapts an engine method that only returns an error.
func callNoResult[Req any](s *Server, fn func(context.Context, Req) error) http.HandlerFunc {
	return call(s, func(ctx context.Context, req Req) (struct{}, error) { return struct{}{}, fn(ctx, req) })
}

// poll adapts a long-poll method; a nil result is 204 No Content.
func poll[T any](s *Server, fn func(context.Context, wire.PollRequest) (*T, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req wire.PollRequest
		if !s.decode(w, r, &req) {
			return
		}
		task, err := fn(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		if task == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, task)
	}
}

// Handler returns the routed handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.Handle("GET /metrics", s.m.Handler())

	mux.HandleFunc("POST /v1/workflows/start", call(s, func(ctx context.Context, req wire.StartRequest) (wire.StartResponse, error) {
		run, err := s.e.StartWorkflow(ctx, req)
		return wire.StartResponse{RunID: run}, err
	}))
	mux.HandleFunc("POST /v1/workflows/signal", callNoResult(s, s.e.SignalWorkflow))
	mux.HandleFunc("POST /v1/workflows/cancel", callNoResult(s, s.e.CancelWorkflow))
	mux.HandleFunc("POST /v1/workflows/describe", call(s, s.e.Describe))
	mux.HandleFunc("POST /v1/workflows/history", call(s, s.e.History))

	mux.HandleFunc("POST /v1/tasks/workflow/poll", poll(s, s.e.PollWorkflowTask))
	mux.HandleFunc("POST /v1/tasks/workflow/complete", callNoResult(s, s.e.CompleteWorkflowTask))
	mux.HandleFunc("POST /v1/tasks/workflow/fail", callNoResult(s, s.e.FailWorkflowTask))
	mux.HandleFunc("POST /v1/tasks/activity/poll", poll(s, s.e.PollActivityTask))
	mux.HandleFunc("POST /v1/tasks/activity/complete", callNoResult(s, s.e.CompleteActivityTask))
	mux.HandleFunc("POST /v1/tasks/activity/fail", callNoResult(s, s.e.FailActivityTask))
	return mux
}
