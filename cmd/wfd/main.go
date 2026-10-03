// Command wfd is the workflow engine server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/api"
	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/core"
	"github.com/sathwikbairaboina2/workflow-engine/internal/metrics"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "wfd:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("wfd", flag.ContinueOnError)
	listen := fs.String("listen", ":7233", "address to listen on")
	dbPath := fs.String("db", "wf.db", "SQLite database path")
	wftTimeout := fs.Duration("wft-timeout", 10*time.Second, "workflow task lease timeout")
	pollTimeout := fs.Duration("poll-timeout", 20*time.Second, "long-poll duration before a 204")
	timerEvery := fs.Duration("timer-interval", 100*time.Millisecond, "how often due timers are fired")
	reapEvery := fs.Duration("reaper-interval", 500*time.Millisecond, "how often expired leases are reaped")
	maxEvents := fs.Int64("max-history-events", 50000, "events per run before it is failed with HistoryLimitExceeded")
	maxPayload := fs.Int("max-payload-bytes", 2<<20, "largest accepted payload")
	if err := fs.Parse(args); err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	m := metrics.New()
	engine := core.New(st, clock.Real{}, core.Config{
		WorkflowTaskTimeout: *wftTimeout, PollTimeout: *pollTimeout,
		MaxPayloadBytes: *maxPayload, MaxHistoryEvents: *maxEvents,
	}, core.WithMetrics(m))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	loopsDone := make(chan struct{})
	go func() { defer close(loopsDone); engine.RunLoops(ctx, *timerEvery, *reapEvery) }()

	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.New(engine, m, int64(*maxPayload)+1<<20).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		// Requests inherit ctx, so a SIGTERM wakes every long-poll instead of making Shutdown wait for it.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	slog.Info("wfd listening", "addr", *listen, "db", *dbPath)

	select {
	case err := <-serveErr:
		stop()
		<-loopsDone
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("shutdown", "err", err)
	}
	<-loopsDone
	slog.Info("wfd stopped")
	return nil
}
