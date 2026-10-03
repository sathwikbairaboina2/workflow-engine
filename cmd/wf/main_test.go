package main

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/sathwikbairaboina2/workflow-engine/internal/testserver"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func wf(t *testing.T, srv *testserver.Server, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(append([]string{"--server", srv.URL}, args...), &out, &errOut)
	return code, out.String(), errOut.String()
}

func startC1(t *testing.T, srv *testserver.Server) string {
	t.Helper()
	code, out, errOut := wf(t, srv, "start", "W", "--id", "c1", "--queue", "q", "--input", `{"n":1}`)
	if code != 0 {
		t.Fatalf("start: %d %s", code, errOut)
	}
	return strings.TrimSpace(out)
}

func TestStartPrintsRunID(t *testing.T) {
	srv := testserver.Start(t)
	code, out, _ := wf(t, srv, "start", "W", "--id", "c1", "--queue", "q", "--input", `{"n":1}`)
	if code != 0 || !regexp.MustCompile(`^[0-9a-f]{32}\n$`).MatchString(out) {
		t.Fatalf("code %d out %q", code, out)
	}
}

func TestStartBadJSON(t *testing.T) {
	srv := testserver.Start(t)
	code, _, errOut := wf(t, srv, "start", "W", "--id", "c1", "--input", `{nope`)
	if code != 2 || !strings.Contains(errOut, "--input") {
		t.Fatalf("code %d stderr %q", code, errOut)
	}
	if code, _, errOut := wf(t, srv, "start", "W"); code != 2 || !strings.Contains(errOut, "--id") {
		t.Fatalf("missing id: code %d stderr %q", code, errOut)
	}
}

func TestDescribeJSON(t *testing.T) {
	srv := testserver.Start(t)
	startC1(t, srv)
	code, out, _ := wf(t, srv, "describe", "c1")
	var d struct{ Status string }
	if code != 0 || json.Unmarshal([]byte(out), &d) != nil || d.Status != "running" {
		t.Fatalf("code %d out %q", code, out)
	}
	if !strings.Contains(out, `"status": "running"`) {
		t.Fatalf("not indented JSON: %q", out)
	}
}

func TestHistoryTable(t *testing.T) {
	srv := testserver.Start(t)
	startC1(t, srv)
	code, out, _ := wf(t, srv, "history", "c1")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if code != 0 || len(lines) != 3 {
		t.Fatalf("code %d lines %q", code, lines)
	}
	if got := strings.Fields(lines[0]); strings.Join(got, " ") != "ID TYPE DETAILS" {
		t.Fatalf("header %q", lines[0])
	}
	if got := strings.Join(strings.Fields(lines[1]), " "); got != "1 WorkflowExecutionStarted type=W queue=q" {
		t.Fatalf("row %q", got)
	}
	if got := strings.Join(strings.Fields(lines[2]), " "); got != "2 WorkflowTaskScheduled attempt=1" {
		t.Fatalf("row %q", got)
	}
}

func TestHistoryJSON(t *testing.T) {
	srv := testserver.Start(t)
	startC1(t, srv)
	code, out, _ := wf(t, srv, "history", "c1", "--json")
	var evs []wire.Event
	if code != 0 || json.Unmarshal([]byte(out), &evs) != nil || len(evs) != 2 || evs[0].Type != wire.WorkflowExecutionStarted {
		t.Fatalf("code %d out %q", code, out)
	}
}

func TestSignalCancel(t *testing.T) {
	srv := testserver.Start(t)
	startC1(t, srv)
	if code, _, errOut := wf(t, srv, "signal", "c1", "approve", "--payload", `"yes"`); code != 0 {
		t.Fatalf("signal: %d %s", code, errOut)
	}
	if code, _, errOut := wf(t, srv, "cancel", "c1", "--reason", "test"); code != 0 {
		t.Fatalf("cancel: %d %s", code, errOut)
	}
	_, out, _ := wf(t, srv, "history", "c1")
	if !strings.Contains(out, "name=approve") || !strings.Contains(out, "reason=test") {
		t.Fatalf("history missing signal/cancel:\n%s", out)
	}
	if code, _, _ := wf(t, srv, "signal", "nope", "x"); code != 1 {
		t.Fatalf("signal to unknown workflow: exit %d", code)
	}
}

func TestResultWaits(t *testing.T) {
	srv := testserver.Start(t)
	startC1(t, srv)
	task, err := srv.Engine.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "t"})
	if err != nil || task == nil {
		t.Fatal(err)
	}
	if err := srv.Engine.CompleteWorkflowTask(context.Background(), wire.CompleteWorkflowTaskRequest{LeaseToken: task.LeaseToken,
		Commands: []wire.Command{{Type: wire.CompleteWorkflow, Result: wire.MustEncode(map[string]int{"answer": 42})}}}); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := wf(t, srv, "result", "c1", "--timeout", "5s")
	if code != 0 || strings.TrimSpace(out) != `{"answer":42}` {
		t.Fatalf("code %d out %q err %q", code, out, errOut)
	}
}

func TestResultTimesOutAndReportsFailure(t *testing.T) {
	srv := testserver.Start(t)
	startC1(t, srv)
	if code, _, errOut := wf(t, srv, "result", "c1", "--timeout", "300ms"); code != 1 || !strings.Contains(errOut, "no result") {
		t.Fatalf("timeout: code %d err %q", code, errOut)
	}
	task, _ := srv.Engine.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "t"})
	srv.Engine.CompleteWorkflowTask(context.Background(), wire.CompleteWorkflowTaskRequest{LeaseToken: task.LeaseToken,
		Commands: []wire.Command{{Type: wire.FailWorkflow, Failure: &wire.Failure{Type: "Boom", Message: "bad"}}}})
	if code, _, errOut := wf(t, srv, "result", "c1", "--timeout", "5s"); code != 1 || !strings.Contains(errOut, "Boom") {
		t.Fatalf("failed workflow: code %d err %q", code, errOut)
	}
}

func TestHealth(t *testing.T) {
	srv := testserver.Start(t)
	if code, out, _ := wf(t, srv, "health"); code != 0 || strings.TrimSpace(out) != "ok" {
		t.Fatalf("code %d out %q", code, out)
	}
	var out, errOut bytes.Buffer
	if code := run([]string{"--server", "http://127.0.0.1:1", "health"}, &out, &errOut); code != 1 {
		t.Fatalf("unreachable server: exit %d", code)
	}
}

func TestUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(nil, &out, &errOut); code != 2 {
		t.Fatalf("exit %d", code)
	}
	for _, c := range []string{"start", "describe", "history", "signal", "cancel", "result", "health"} {
		if !strings.Contains(errOut.String(), c) {
			t.Errorf("usage does not list %q", c)
		}
	}
	errOut.Reset()
	if code := run([]string{"frobnicate"}, &out, &errOut); code != 2 {
		t.Fatalf("unknown command exit %d", code)
	}
}
