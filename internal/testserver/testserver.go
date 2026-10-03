// Package testserver runs a real engine behind a real HTTP listener for SDK, CLI and benchmark tests.
package testserver

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/api"
	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/core"
	"github.com/sathwikbairaboina2/workflow-engine/internal/metrics"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
)

// Server is a running in-process wfd.
type Server struct {
	URL    string
	Engine *core.Engine
	Store  *store.Store
}

// Start launches a server on a temp-dir SQLite file with the real clock; everything is stopped by t.Cleanup.
func Start(t testing.TB, opts ...func(*core.Config)) *Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "wf.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := core.Config{PollTimeout: 500 * time.Millisecond, PollInterval: 50 * time.Millisecond, WorkflowTaskTimeout: 2 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}
	m := metrics.New()
	engine := core.New(st, clock.Real{}, cfg, core.WithMetrics(m))
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); engine.RunLoops(ctx, 10*time.Millisecond, 50*time.Millisecond) }()
	maxPayload := cfg.MaxPayloadBytes
	if maxPayload <= 0 {
		maxPayload = 2 << 20 // the engine default
	}
	ts := httptest.NewServer(api.New(engine, m, int64(maxPayload)+1<<20).Handler())
	t.Cleanup(func() {
		ts.CloseClientConnections()
		cancel()
		wg.Wait()
		ts.Close()
		st.Close()
	})
	return &Server{URL: ts.URL, Engine: engine, Store: st}
}
