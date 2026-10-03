package client_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/internal/testserver"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestStartDescribeHistory(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	run, err := c.Start(ctx(t), client.StartOptions{ID: "c1", TaskQueue: "q"}, "W", map[string]int{"n": 1})
	if err != nil || len(run) != 32 {
		t.Fatalf("run=%q err=%v", run, err)
	}
	d, err := c.Describe(ctx(t), "c1")
	if err != nil || d.Status != "running" || d.RunID != run {
		t.Fatalf("describe %+v %v", d, err)
	}
	h, err := c.History(ctx(t), "c1", "")
	if err != nil || len(h) != 2 {
		t.Fatalf("history %d %v", len(h), err)
	}
}

func TestStartAlreadyStarted(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	run, _ := c.Start(ctx(t), client.StartOptions{ID: "c1", TaskQueue: "q"}, "W", nil)
	_, err := c.Start(ctx(t), client.StartOptions{ID: "c1", TaskQueue: "q"}, "W", nil)
	if got, ok := client.IsAlreadyStarted(err); !ok || got != run {
		t.Fatalf("IsAlreadyStarted = %q, %v (err %v)", got, ok, err)
	}
}

func TestSignalCancel(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	c.Start(ctx(t), client.StartOptions{ID: "c1", TaskQueue: "q"}, "W", nil)
	if err := c.Signal(ctx(t), "c1", "go", "payload"); err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(ctx(t), "c1", "because"); err != nil {
		t.Fatal(err)
	}
	h, _ := c.History(ctx(t), "c1", "")
	var sig, can bool
	for _, e := range h {
		sig = sig || e.Type == wire.WorkflowExecutionSignaled
		can = can || e.Type == wire.WorkflowExecutionCancelRequested
	}
	if !sig || !can {
		t.Fatalf("signal=%v cancel=%v", sig, can)
	}
}

func TestHistoryFollowsPages(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	c.Start(ctx(t), client.StartOptions{ID: "c1", TaskQueue: "q"}, "W", nil)
	const signals = 2500
	// Append the signals in one transaction: 2500 separate durable commits would only slow the test down.
	err := srv.Store.WithTx(context.Background(), "test_fill", func(tx *store.Tx) error {
		w, _, err := tx.GetOpenRun("c1")
		if err != nil {
			return err
		}
		evs := make([]store.NewEvent, signals)
		for i := range evs {
			evs[i] = store.NewEvent{Type: wire.WorkflowExecutionSignaled, Time: 1, Attrs: wire.WorkflowExecutionSignaledAttrs{Name: "s"}}
		}
		_, err = tx.AppendEvents(w.RunID, w.NextEventID, evs)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.History(ctx(t), "c1", "")
	if err != nil || len(h) != signals+2 {
		t.Fatalf("history length %d (want %d), err %v", len(h), signals+2, err)
	}
	for i, e := range h {
		if e.EventID != int64(i+1) {
			t.Fatalf("page seam broke ids at %d: %d", i, e.EventID)
		}
	}
}

// flaky closes the first n connections without answering, then proxies to target.
func flaky(t *testing.T, target string, n int32) *httptest.Server {
	u, _ := url.Parse(target)
	proxy := httputil.NewSingleHostReverseProxy(u)
	var seen atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen.Add(1) <= n {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestRetriesTransportErrors(t *testing.T) {
	srv := testserver.Start(t)
	f := flaky(t, srv.URL, 2)
	c := client.New(f.URL)
	if _, err := c.Start(ctx(t), client.StartOptions{ID: "c1", TaskQueue: "q"}, "W", nil); err != nil {
		t.Fatalf("start with retries: %v", err)
	}
	f2 := flaky(t, srv.URL, 2)
	c2 := client.New(f2.URL, client.WithRetry(0))
	if _, err := c2.Start(ctx(t), client.StartOptions{ID: "c2", TaskQueue: "q"}, "W", nil); err == nil {
		t.Fatal("start without retries should fail on a dropped connection")
	}
}

func finish(t *testing.T, srv *testserver.Server, cmd wire.Command) {
	t.Helper()
	task, err := srv.Engine.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "t"})
	if err != nil || task == nil {
		t.Fatalf("poll: %v %v", task, err)
	}
	if err := srv.Engine.CompleteWorkflowTask(context.Background(), wire.CompleteWorkflowTaskRequest{LeaseToken: task.LeaseToken, Commands: []wire.Command{cmd}}); err != nil {
		t.Fatal(err)
	}
}

func TestGetResult(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)

	c.Start(ctx(t), client.StartOptions{ID: "ok", TaskQueue: "q"}, "W", nil)
	finish(t, srv, wire.Command{Type: wire.CompleteWorkflow, Result: wire.MustEncode(42)})
	var n int
	if err := c.GetResult(ctx(t), "ok", &n); err != nil || n != 42 {
		t.Fatalf("result %d %v", n, err)
	}

	c.Start(ctx(t), client.StartOptions{ID: "bad", TaskQueue: "q"}, "W", nil)
	finish(t, srv, wire.Command{Type: wire.FailWorkflow, Failure: &wire.Failure{Type: "Boom", Message: "x"}})
	var wfe *client.WorkflowFailedError
	if err := c.GetResult(ctx(t), "bad", &n); !errors.As(err, &wfe) || wfe.Failure.Type != "Boom" {
		t.Fatalf("failed run: %v", err)
	}

	c.Start(ctx(t), client.StartOptions{ID: "cx", TaskQueue: "q"}, "W", nil)
	finish(t, srv, wire.Command{Type: wire.CancelWorkflow})
	if err := c.GetResult(ctx(t), "cx", &n); !errors.Is(err, client.ErrWorkflowCanceled) {
		t.Fatalf("canceled run: %v", err)
	}
}

func TestHealth(t *testing.T) {
	srv := testserver.Start(t)
	if err := client.New(srv.URL).Health(ctx(t)); err != nil {
		t.Fatal(err)
	}
	if err := client.New("http://127.0.0.1:1", client.WithRetry(0)).Health(ctx(t)); err == nil {
		t.Fatal("unreachable server reported healthy")
	}
}
