package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/api"
	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/core"
	"github.com/sathwikbairaboina2/workflow-engine/internal/metrics"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

const maxPayload = 1024

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "wf.db"))
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New()
	cfg := core.Config{PollTimeout: 100 * time.Millisecond, PollInterval: 20 * time.Millisecond, MaxPayloadBytes: maxPayload}
	e := core.New(st, clock.Real{}, cfg, core.WithMetrics(m))
	srv := httptest.NewServer(api.New(e, m, maxPayload+1<<20).Handler())
	t.Cleanup(func() { srv.Close(); st.Close() })
	return srv
}

func post(t *testing.T, srv *httptest.Server, path string, body any) (int, []byte) {
	t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case string:
		r = strings.NewReader(b)
	case []byte:
		r = bytes.NewReader(b)
	default:
		j, _ := json.Marshal(body)
		r = bytes.NewReader(j)
	}
	resp, err := http.Post(srv.URL+path, "application/json", r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func get(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func startReq(id string) wire.StartRequest {
	return wire.StartRequest{WorkflowID: id, WorkflowType: "W", TaskQueue: "q", Input: wire.MustEncode(1)}
}

func TestHealthz(t *testing.T) {
	srv := newServer(t)
	if code, body := get(t, srv, "/healthz"); code != 200 || strings.TrimSpace(body) != "ok" {
		t.Fatalf("%d %q", code, body)
	}
}

func TestStartDescribeHistoryOverHTTP(t *testing.T) {
	srv := newServer(t)
	code, body := post(t, srv, "/v1/workflows/start", startReq("w1"))
	var sr wire.StartResponse
	if code != 200 || json.Unmarshal(body, &sr) != nil || len(sr.RunID) != 32 {
		t.Fatalf("start: %d %s", code, body)
	}
	code, body = post(t, srv, "/v1/workflows/describe", wire.DescribeRequest{WorkflowID: "w1"})
	var d wire.DescribeResponse
	if code != 200 || json.Unmarshal(body, &d) != nil || d.Status != "running" || d.RunID != sr.RunID {
		t.Fatalf("describe: %d %s", code, body)
	}
	code, body = post(t, srv, "/v1/tasks/workflow/poll", wire.PollRequest{Queue: "q", Identity: "i"})
	var task wire.WorkflowTask
	if code != 200 || json.Unmarshal(body, &task) != nil || task.LeaseToken == "" || len(task.History) != 3 {
		t.Fatalf("poll: %d %s", code, body)
	}
	code, body = post(t, srv, "/v1/workflows/history", wire.HistoryRequest{WorkflowID: "w1"})
	var h wire.HistoryResponse
	if code != 200 || json.Unmarshal(body, &h) != nil || len(h.Events) != 3 {
		t.Fatalf("history: %d %s", code, body)
	}
	code, body = post(t, srv, "/v1/tasks/workflow/complete", wire.CompleteWorkflowTaskRequest{LeaseToken: task.LeaseToken,
		Commands: []wire.Command{{Type: wire.CompleteWorkflow, Result: wire.MustEncode("done")}}})
	if code != 200 {
		t.Fatalf("complete: %d %s", code, body)
	}
}

func TestAlreadyStarted409(t *testing.T) {
	srv := newServer(t)
	_, body := post(t, srv, "/v1/workflows/start", startReq("w1"))
	var sr wire.StartResponse
	json.Unmarshal(body, &sr)
	code, body := post(t, srv, "/v1/workflows/start", startReq("w1"))
	var eb wire.ErrorBody
	if code != 409 || json.Unmarshal(body, &eb) != nil || eb.Error.Code != "workflow_already_started" || eb.Error.RunID != sr.RunID {
		t.Fatalf("%d %s", code, body)
	}
}

func TestPollTimeout204(t *testing.T) {
	srv := newServer(t)
	begin := time.Now()
	code, _ := post(t, srv, "/v1/tasks/workflow/poll", wire.PollRequest{Queue: "empty"})
	if code != 204 {
		t.Fatalf("status %d", code)
	}
	if d := time.Since(begin); d < 80*time.Millisecond || d > 2*time.Second {
		t.Fatalf("poll returned after %v", d)
	}
	code, _ = post(t, srv, "/v1/tasks/activity/poll", wire.PollRequest{Queue: "empty"})
	if code != 204 {
		t.Fatalf("activity status %d", code)
	}
}

func TestStaleLease409(t *testing.T) {
	srv := newServer(t)
	code, body := post(t, srv, "/v1/tasks/workflow/complete", wire.CompleteWorkflowTaskRequest{LeaseToken: "nope"})
	var eb wire.ErrorBody
	if code != 409 || json.Unmarshal(body, &eb) != nil || eb.Error.Code != "stale_lease" {
		t.Fatalf("%d %s", code, body)
	}
}

func TestNotFound404AndInvalid400(t *testing.T) {
	srv := newServer(t)
	if code, _ := post(t, srv, "/v1/workflows/describe", wire.DescribeRequest{WorkflowID: "nope"}); code != 404 {
		t.Fatalf("describe unknown: %d", code)
	}
	if code, _ := post(t, srv, "/v1/workflows/start", wire.StartRequest{}); code != 400 {
		t.Fatalf("empty start: %d", code)
	}
	big := startReq("w")
	big.Input = wire.MustEncode(strings.Repeat("x", 2*maxPayload))
	if code, _ := post(t, srv, "/v1/workflows/start", big); code != 413 {
		t.Fatalf("oversize payload: %d", code)
	}
}

func TestBadJSON400(t *testing.T) {
	srv := newServer(t)
	code, body := post(t, srv, "/v1/workflows/start", "{not json")
	var eb wire.ErrorBody
	if code != 400 || json.Unmarshal(body, &eb) != nil || eb.Error.Code != "invalid_request" {
		t.Fatalf("%d %s", code, body)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv := newServer(t)
	if code, _ := get(t, srv, "/v1/workflows/start"); code != 405 {
		t.Fatalf("status %d", code)
	}
}

func TestBodyTooLarge413(t *testing.T) {
	srv := newServer(t)
	body := append(append([]byte(`{"workflow_id":"`), bytes.Repeat([]byte("a"), 2<<20)...), `"}`...)
	code, _ := post(t, srv, "/v1/workflows/start", body)
	if code != 413 {
		t.Fatalf("status %d", code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := newServer(t)
	post(t, srv, "/v1/workflows/start", startReq("w1"))
	code, body := get(t, srv, "/metrics")
	if code != 200 || !strings.Contains(body, `wf_transitions_total{kind="start"} 1`) {
		t.Fatalf("%d\n%s", code, body)
	}
}
