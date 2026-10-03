# workflow-engine v0.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Use superpowers:test-driven-development inside every task: write the test, run it, **see it fail for the right reason**, implement, run it green.

**Goal:** Ship a small durable workflow engine in Go (server `wfd`, Go SDK, `wf` CLI, `wfcheck` linter, chaos harness, benchmark) whose README headline comes from a measured chaos soak and a measured benchmark.

**Architecture:** One `wfd` process: HTTP/JSON API → `internal/core.Engine` (every transition is one `BEGIN IMMEDIATE` SQLite transaction through `internal/store`) plus timer and lease-reaper loops. Workers use the SDK: they long-poll tasks, replay the full history through a deterministic coroutine runtime (`sdk/internal/wfrt`), and return commands. The chaos harness spawns real processes and `SIGKILL`s them.

**Tech Stack:** Go 1.26 (only via Docker image `golang:1.26.8`), `modernc.org/sqlite v1.60.1`, `pgregory.net/rapid v1.3.0`, `golang.org/x/tools v0.51.0`, `github.com/prometheus/client_golang v1.24.1`. All four were prototyped in `golang:1.26.8` on 2026-10-04 (see spec, "Environment rules"). No other third-party modules.

**Spec:** `docs/superpowers/specs/2026-10-04-workflow-engine.md` (the "Semantics" section is normative). Decisions: `docs/adr/0001`–`0008`. Ledger: `.superpowers/sdd/2026-10-04-workflow-engine/progress.md`.

## Global Constraints

- Repo root: `C:\Users\sathwik\projects\taskarinchu\workflow-engine` (its own git repo, branch `main`). Never edit sibling directories under `taskarinchu/`.
- Module path `github.com/sathwikbairaboina2/workflow-engine`; `go.mod` directive `go 1.26.0` (what `go mod tidy` writes).
- **Go is not installed on the host.** Every Go command runs in Docker:
  - `GO <args>` in this plan means, from the repo root, Git Bash: `sh scripts/go.sh <args>`; PowerShell: `powershell -NoProfile -File scripts/go.ps1 <args>`.
  - `GOSH '<shell command>'` means `sh scripts/gosh.sh '<shell command>'` (runs `sh -c` inside the same image, for build-then-run sequences).
  - Both are created in Task 1. Containers are named `workflow-engine-*` and use volumes `workflow-engine-gomod` and `workflow-engine-gobuild`.
- The host has **no `make`**. Do not add a Makefile; use the scripts.
- Host ports: only 5400-5409. Compose publishes `wfd` on 5400. Tests bind `127.0.0.1:0` inside the container.
- Stop every container you start (`docker compose -f deploy/docker-compose.yml down -v`).
- No network in tests (module download excepted). No AWS, no LocalStack.
- Bash heredocs in the agent tool can mangle backslashes and quotes: write files with the Write tool.
- Pipe noisy output through `tail -n 30` or `grep`.
- **Numbers:** the README quotes only `bench/results/*.json` from real runs. `TestREADMEHeadline` (Task 24) enforces it.
- **Commits:** local commits are authorized. One commit per task, conventional subject (given at the end of each task), message ends with a blank line then `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`. Never push, never add remotes, never amend, never commit `.env*` or databases. If a permission check blocks a commit, note it in the ledger and continue.
- Ledger: after each task append `Task N: complete (tests: <command> -> <real result>; red seen: <how>)`. Record any deviation as `Ruling: <what> - <why> - <cost>`.
- Style: `gofmt` clean, small files, doc comments on exported identifiers, no comments that restate code, errors wrapped with `%w`.

## Review Focus (reviewers check these first)

1. **Buffering rule** (spec Semantics 2): no event is appended to a run while its workflow task holds a lease, except by that task's completion, failure or reaping. Tests: `TestSignalBufferedDuringInflightTask`, `TestActivityResultBufferedDuringInflightTask` (Task 9).
2. **Replay batching** (Semantics 6): code runs only at a `WorkflowTaskStarted` that is last or followed by `WorkflowTaskCompleted`. Tests: `TestReplaySkipsFailedTaskStep`, `TestReplayNondeterminism` (Task 15).
3. **Exactly-once timer firing** under two concurrent loops: `TestTimerFiresOnce` (Task 9).
4. **Stale leases** after reaping cannot complete: `TestStaleLeaseRejected`, `TestZombieActivityCompletionRejected` (Tasks 7, 8).
5. **Goroutine hygiene** in the dispatcher: `TestDispatcherNoGoroutineLeak` (Task 14).
6. **Fake tests**: every invariant test must fail when the guarded code is removed. Reviewers should mutate (e.g. drop the `next_event_id` check) and confirm red.

## File Structure

```
workflow-engine/
  go.mod, go.sum
  .gitignore, .gitattributes, .dockerignore
  Dockerfile                                  multi-stage, distroless static nonroot
  deploy/docker-compose.yml                   wfd (5400) + 2 transfer workers + cli profile
  scripts/go.ps1, go.sh, gosh.sh              Go in Docker
  scripts/chaos.sh, bench.sh, demo.sh, gates.sh
  readme_test.go                              TestREADMEHeadline (package workflowengine_test)
  wire/                                       public wire types
    payload.go, events.go, commands.go, api.go, wire_test.go
  internal/clock/clock.go, clock_test.go
  internal/store/
    store.go (Open, WithTx, failpoints), schema.sql, tx.go (row ops), check.go (CheckConsistency)
    store_test.go, tx_test.go, check_test.go
  internal/retry/retry.go, retry_test.go
  internal/core/
    engine.go (Engine, Config, Option, errors, transition helper), notify.go,
    start.go (start, signal, cancel, describe, history), wftask.go (poll/complete/fail workflow tasks),
    commands.go (ValidateCommands, command → event), deliver.go (append-or-buffer),
    activity.go (poll/complete/fail activity tasks), timers.go (FireDueTimers), reaper.go, loops.go,
    *_test.go, helpers_test.go
  internal/metrics/metrics.go, metrics_test.go
  internal/api/api.go, errors.go, api_test.go
  internal/testserver/testserver.go           in-process server for SDK/CLI/bench tests
  internal/wfcheck/analyzer.go, analyzer_test.go, testdata/src/...
  internal/bench/bench.go, bench_test.go
  sdk/client/client.go, errors.go, client_test.go
  sdk/internal/wfrt/
    dispatcher.go, dispatcher_test.go, env.go, api.go (ExecuteActivity, timers, signals, selector, SideEffect),
    replay.go, replay_test.go, invoke.go (reflection helpers)
  sdk/workflow/workflow.go                    facade over wfrt
  sdk/activity/activity.go, activity_test.go
  sdk/worker/worker.go, registry.go, replay.go, worker_test.go, golden_test.go, testdata/histories/*.json
  examples/transfer/transfer.go, ledger.go, transfer_test.go, ledger_test.go
  examples/transfer/cmd/transfer-worker/main.go
  cmd/wfd/main.go, cmd/wf/main.go (+ main_test.go), cmd/wfcheck/main.go, cmd/chaos/main.go, cmd/wfbench/main.go
  chaos/harness.go, procs.go, checks.go, report.go, soak_test.go
  bench/results/chaos-latest.json, chaos-control.json, bench-latest.json   (written by real runs)
  .github/workflows/ci.yml
  README.md, docs/semantics.md, docs/handoff.md, docs/adr/, docs/superpowers/
```

---

### Task 1: Scaffold, Docker toolchain, clock

**Files:** create `go.mod`, `scripts/go.ps1`, `scripts/go.sh`, `scripts/gosh.sh`, `.gitattributes` (exists, keep), `.gitignore` (exists, extend), `.dockerignore`, `internal/clock/clock.go`, `internal/clock/clock_test.go`.

- [ ] **Step 1: scripts.** `scripts/go.ps1`:

```powershell
# Runs the Go toolchain inside Docker so the host needs no Go install.
$root = (Resolve-Path "$PSScriptRoot\..").Path -replace '\\', '/'
$name = "workflow-engine-go-$PID-$(Get-Random -Maximum 99999)"
docker run --rm --name $name -v "${root}:/src" -v workflow-engine-gomod:/go/pkg/mod -v workflow-engine-gobuild:/root/.cache/go-build -w /src golang:1.26.8 go @args
exit $LASTEXITCODE
```

`scripts/go.sh`:

```sh
#!/usr/bin/env sh
# Runs the Go toolchain inside Docker so the host needs no Go install.
set -e
cd "$(dirname "$0")/.."
root="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm --name "workflow-engine-go-$$-$(date +%s)" -v "$root:/src" \
  -v workflow-engine-gomod:/go/pkg/mod -v workflow-engine-gobuild:/root/.cache/go-build \
  -w /src golang:1.26.8 go "$@"
```

`scripts/gosh.sh` is identical except the last line runs `golang:1.26.8 sh -c "$1"` and the name prefix is `workflow-engine-sh-`.

- [ ] **Step 2: module.** Create `go.mod` with `module github.com/sathwikbairaboina2/workflow-engine` and `go 1.26.0`. Run `GO version` → expect `go version go1.26.8 linux/amd64`. Run `GO env GOMOD` → expect `/src/go.mod`.
- [ ] **Step 3: hygiene.** Append to `.gitignore`: `/wfd`, `/wf`, `/dist/`, `*.test`, `*.out`, `*.db`, `*.db-wal`, `*.db-shm`, `/chaos-out/`. `.dockerignore`: `.git`, `docs`, `bench/results`, `*.md`, `.superpowers`, `chaos-out`.
- [ ] **Step 4: clock test first.** `internal/clock/clock_test.go`: `TestFakeAdvance` (fake starts at a given time; `Advance(1500*time.Millisecond)` moves `Now()` by exactly that), `TestFakeNowMillis` (`NowMS()` equals `Now().UnixMilli()`), `TestRealMonotonic` (two `Real{}.Now()` calls non-decreasing). Run `GO test ./internal/clock` → red (`undefined: NewFake`).
- [ ] **Step 5: implement** `internal/clock/clock.go`:

```go
// Package clock lets every time-dependent component take an injectable clock.
package clock

// Clock returns the current time.
type Clock interface{ Now() time.Time }

// NowMS returns c.Now() in Unix milliseconds, the unit stored everywhere in the engine.
func NowMS(c Clock) int64 { return c.Now().UnixMilli() }

// Real is the wall clock.
type Real struct{}

// Fake is a manually advanced clock, safe for concurrent use.
type Fake struct { mu sync.Mutex; t time.Time }
func NewFake(t time.Time) *Fake
func (f *Fake) Now() time.Time
func (f *Fake) Advance(d time.Duration)
func (f *Fake) Set(t time.Time)
```

(`TestFakeNowMillis` uses `clock.NowMS(f)`.) Run `GO test -race ./internal/clock` → `ok`.
- [ ] **Step 6: commit** `chore: scaffold module, docker-only Go toolchain and clock`.

---

### Task 2: `wire` package (events, commands, payloads, API types)

**Files:** `wire/payload.go`, `wire/events.go`, `wire/commands.go`, `wire/api.go`, `wire/wire_test.go`.

This package is the contract between server and SDK. Use exactly these names and JSON tags.

- [ ] **Step 1: tests first** (`wire/wire_test.go`):
  - `TestPayloadRoundTrip`: `Encode(map[string]int{"a":1})` → `Encoding=="json"`, `Decode(&m)` gives back `a:1`; JSON of the payload is `{"encoding":"json","data":{"a":1}}`.
  - `TestDecodeNilPayloadLeavesTarget`: `(*Payload)(nil).Decode(&x)` returns nil and leaves `x` unchanged.
  - `TestDecodeUnknownEncoding`: `Payload{Encoding:"proto"}` → error containing `unsupported encoding`.
  - `TestEventAttrsRoundTrip`: `NewEvent(5, ActivityTaskScheduled, 1700000000000, ActivityTaskScheduledAttrs{Seq:3, ActivityType:"Debit"})` marshals; `ev.DecodeAttrs(&a)` gives `Seq==3`.
  - `TestIsCommandEvent`: true for the six command event types (spec Semantics 6), false for `WorkflowTaskStarted` and `TimerFired`.
  - `TestPayloadSize`: `Size()` of nil is 0, otherwise `len(Data)`.
  Run → red (`undefined: Encode`).
- [ ] **Step 2: implement.**

```go
// payload.go
type Payload struct {
	Encoding string          `json:"encoding"`
	Data     json.RawMessage `json:"data,omitempty"`
}
func Encode(v any) (*Payload, error)          // nil v → (nil, nil)
func MustEncode(v any) *Payload               // panics on error; tests and constants only
func (p *Payload) Decode(v any) error         // nil p or nil v → nil
func (p *Payload) Size() int

type Failure struct {
	Type         string `json:"type"`
	Message      string `json:"message"`
	NonRetryable bool   `json:"non_retryable,omitempty"`
}

type RetryPolicy struct {
	InitialMS    int64    `json:"initial_ms"`
	Backoff      float64  `json:"backoff"`
	MaxMS        int64    `json:"max_ms"`
	MaxAttempts  int      `json:"max_attempts"` // 0 = unlimited
	NonRetryable []string `json:"non_retryable,omitempty"`
}
```

```go
// events.go
type EventType string
const (
	WorkflowExecutionStarted         EventType = "WorkflowExecutionStarted"
	WorkflowTaskScheduled            EventType = "WorkflowTaskScheduled"
	WorkflowTaskStarted              EventType = "WorkflowTaskStarted"
	WorkflowTaskCompleted            EventType = "WorkflowTaskCompleted"
	WorkflowTaskFailed               EventType = "WorkflowTaskFailed"
	ActivityTaskScheduled            EventType = "ActivityTaskScheduled"
	ActivityTaskStarted              EventType = "ActivityTaskStarted"
	ActivityTaskCompleted            EventType = "ActivityTaskCompleted"
	ActivityTaskFailed               EventType = "ActivityTaskFailed"
	ActivityTaskTimedOut             EventType = "ActivityTaskTimedOut"
	TimerStarted                     EventType = "TimerStarted"
	TimerFired                       EventType = "TimerFired"
	MarkerRecorded                   EventType = "MarkerRecorded"
	WorkflowExecutionSignaled        EventType = "WorkflowExecutionSignaled"
	WorkflowExecutionCancelRequested EventType = "WorkflowExecutionCancelRequested"
	WorkflowExecutionCompleted       EventType = "WorkflowExecutionCompleted"
	WorkflowExecutionFailed          EventType = "WorkflowExecutionFailed"
	WorkflowExecutionCanceled        EventType = "WorkflowExecutionCanceled"
)

// Event is one immutable history entry. Time is Unix milliseconds.
type Event struct {
	EventID int64           `json:"event_id"`
	Type    EventType       `json:"type"`
	Time    int64           `json:"ts"`
	Attrs   json.RawMessage `json:"attrs"`
}
func NewEvent(id int64, t EventType, ts int64, attrs any) (Event, error)
func (e Event) DecodeAttrs(v any) error
func IsCommandEvent(t EventType) bool // ActivityTaskScheduled, TimerStarted, MarkerRecorded, WorkflowExecutionCompleted/Failed/Canceled

type WorkflowExecutionStartedAttrs struct {
	WorkflowID   string   `json:"workflow_id"`
	WorkflowType string   `json:"workflow_type"`
	TaskQueue    string   `json:"task_queue"`
	Input        *Payload `json:"input,omitempty"`
}
type WorkflowTaskScheduledAttrs struct{ Attempt int `json:"attempt"` }
type WorkflowTaskStartedAttrs struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	Identity         string `json:"identity"`
}
type WorkflowTaskCompletedAttrs struct {
	ScheduledEventID int64 `json:"scheduled_event_id"`
	StartedEventID   int64 `json:"started_event_id"`
}
type WorkflowTaskFailedAttrs struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	StartedEventID   int64  `json:"started_event_id"`
	Cause            string `json:"cause"` // nondeterminism | panic | unknown_workflow_type | invalid_commands | timeout
	Message          string `json:"message,omitempty"`
}
type ActivityTaskScheduledAttrs struct {
	Seq            int64       `json:"seq"`
	ActivityType   string      `json:"activity_type"`
	TaskQueue      string      `json:"task_queue"`
	Input          *Payload    `json:"input,omitempty"`
	RetryPolicy    RetryPolicy `json:"retry_policy"`
	StartToCloseMS int64       `json:"start_to_close_ms"`
}
type ActivityTaskStartedAttrs struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	Attempt          int    `json:"attempt"`
	Identity         string `json:"identity,omitempty"`
}
type ActivityTaskCompletedAttrs struct {
	ScheduledEventID int64    `json:"scheduled_event_id"`
	Result           *Payload `json:"result,omitempty"`
}
type ActivityTaskFailedAttrs struct {
	ScheduledEventID int64   `json:"scheduled_event_id"`
	Failure          Failure `json:"failure"`
	Attempts         int     `json:"attempts"`
}
type ActivityTaskTimedOutAttrs struct {
	ScheduledEventID int64 `json:"scheduled_event_id"`
	Attempts         int   `json:"attempts"`
}
type TimerStartedAttrs struct {
	Seq        int64 `json:"seq"`
	DurationMS int64 `json:"duration_ms"`
	FireAt     int64 `json:"fire_at"`
}
type TimerFiredAttrs struct{ StartedEventID int64 `json:"started_event_id"` }
type MarkerRecordedAttrs struct {
	Seq   int64    `json:"seq"`
	Kind  string   `json:"kind"` // "side_effect"
	Value *Payload `json:"value,omitempty"`
}
type WorkflowExecutionSignaledAttrs struct {
	Name    string   `json:"name"`
	Payload *Payload `json:"payload,omitempty"`
}
type WorkflowExecutionCancelRequestedAttrs struct{ Reason string `json:"reason,omitempty"` }
type WorkflowExecutionCompletedAttrs struct{ Result *Payload `json:"result,omitempty"` }
type WorkflowExecutionFailedAttrs struct{ Failure Failure `json:"failure"` }
type WorkflowExecutionCanceledAttrs struct{ Reason string `json:"reason,omitempty"` }
```

```go
// commands.go
type CommandType string
const (
	ScheduleActivity CommandType = "ScheduleActivity"
	StartTimer       CommandType = "StartTimer"
	RecordMarker     CommandType = "RecordMarker"
	CompleteWorkflow CommandType = "CompleteWorkflow"
	FailWorkflow     CommandType = "FailWorkflow"
	CancelWorkflow   CommandType = "CancelWorkflow"
)
type Command struct {
	Type           CommandType  `json:"type"`
	Seq            int64        `json:"seq,omitempty"`
	ActivityType   string       `json:"activity_type,omitempty"`
	TaskQueue      string       `json:"task_queue,omitempty"` // default: the workflow's queue
	Input          *Payload     `json:"input,omitempty"`
	RetryPolicy    *RetryPolicy `json:"retry_policy,omitempty"`
	StartToCloseMS int64        `json:"start_to_close_ms,omitempty"`
	DurationMS     int64        `json:"duration_ms,omitempty"`
	MarkerKind     string       `json:"marker_kind,omitempty"`
	Value          *Payload     `json:"value,omitempty"`
	Result         *Payload     `json:"result,omitempty"`
	Failure        *Failure     `json:"failure,omitempty"`
	Reason         string       `json:"reason,omitempty"`
}
func (c Command) IsClose() bool
// EventTypeFor maps a command type to the event type it produces.
func EventTypeFor(t CommandType) EventType
```

```go
// api.go
type StartRequest struct {
	WorkflowID   string   `json:"workflow_id"`
	WorkflowType string   `json:"workflow_type"`
	TaskQueue    string   `json:"task_queue"`
	Input        *Payload `json:"input,omitempty"`
}
type StartResponse struct{ RunID string `json:"run_id"` }
type SignalRequest struct {
	WorkflowID string   `json:"workflow_id"`
	Name       string   `json:"name"`
	Payload    *Payload `json:"payload,omitempty"`
}
type CancelRequest struct {
	WorkflowID string `json:"workflow_id"`
	Reason     string `json:"reason,omitempty"`
}
type DescribeRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
}
type PendingActivity struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	ActivityType     string `json:"activity_type"`
	Attempt          int    `json:"attempt"`
	Leased           bool   `json:"leased"`
}
type PendingTimer struct {
	StartedEventID int64 `json:"started_event_id"`
	FireAt         int64 `json:"fire_at"`
}
type DescribeResponse struct {
	WorkflowID        string            `json:"workflow_id"`
	RunID             string            `json:"run_id"`
	WorkflowType      string            `json:"workflow_type"`
	TaskQueue         string            `json:"task_queue"`
	Status            string            `json:"status"` // running | completed | failed | canceled
	Result            *Payload          `json:"result,omitempty"`
	Failure           *Failure          `json:"failure,omitempty"`
	CreatedAt         int64             `json:"created_at"`
	ClosedAt          int64             `json:"closed_at,omitempty"`
	HistoryLength     int64             `json:"history_length"`
	PendingActivities []PendingActivity `json:"pending_activities"`
	PendingTimers     []PendingTimer    `json:"pending_timers"`
}
type HistoryRequest struct {
	WorkflowID  string `json:"workflow_id"`
	RunID       string `json:"run_id,omitempty"`
	FromEventID int64  `json:"from_event_id,omitempty"` // default 1
	PageSize    int    `json:"page_size,omitempty"`     // default and max 1000
}
type HistoryResponse struct {
	RunID       string  `json:"run_id"`
	Events      []Event `json:"events"`
	NextEventID int64   `json:"next_event_id,omitempty"` // 0 when there are no more pages
}
type PollRequest struct {
	Queue    string `json:"queue"`
	Identity string `json:"identity"`
}
type WorkflowTask struct {
	LeaseToken   string  `json:"lease_token"`
	RunID        string  `json:"run_id"`
	WorkflowID   string  `json:"workflow_id"`
	WorkflowType string  `json:"workflow_type"`
	TaskQueue    string  `json:"task_queue"`
	Attempt      int     `json:"attempt"`
	History      []Event `json:"history"`
}
type CompleteWorkflowTaskRequest struct {
	LeaseToken string    `json:"lease_token"`
	Commands   []Command `json:"commands"`
}
type FailWorkflowTaskRequest struct {
	LeaseToken string `json:"lease_token"`
	Cause      string `json:"cause"`
	Message    string `json:"message,omitempty"`
}
type ActivityTask struct {
	LeaseToken       string   `json:"lease_token"`
	RunID            string   `json:"run_id"`
	WorkflowID       string   `json:"workflow_id"`
	ScheduledEventID int64    `json:"scheduled_event_id"`
	ActivityType     string   `json:"activity_type"`
	Input            *Payload `json:"input,omitempty"`
	Attempt          int      `json:"attempt"`
	DeadlineMS       int64    `json:"deadline_ms"`
}
type CompleteActivityTaskRequest struct {
	LeaseToken string   `json:"lease_token"`
	Identity   string   `json:"identity,omitempty"`
	Result     *Payload `json:"result,omitempty"`
}
type FailActivityTaskRequest struct {
	LeaseToken string  `json:"lease_token"`
	Identity   string  `json:"identity,omitempty"`
	Failure    Failure `json:"failure"`
}
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	RunID   string `json:"run_id,omitempty"`
}
```

Run `GO test -race ./wire` → `ok`.
- [ ] **Step 3: commit** `feat(wire): event, command, payload and API types`.

---

### Task 3: SQLite store: open, schema, triggers, transactions, failpoints

**Files:** `internal/store/store.go`, `internal/store/schema.sql`, `internal/store/store_test.go`.

- [ ] **Step 1: dependency.** `GO get modernc.org/sqlite@v1.60.1` then `GO mod tidy`. Expect `modernc.org/sqlite v1.60.1` in `go.mod`.
- [ ] **Step 2: tests first** (`store_test.go`, helper `openTemp(t) *Store` using `filepath.Join(t.TempDir(), "wf.db")`):
  - `TestOpenAppliesPragmas`: `PRAGMA journal_mode` = `wal`, `PRAGMA synchronous` = `2`, `PRAGMA user_version` = `1`.
  - `TestOpenIsIdempotent`: open, close, open the same path again → no error, `user_version` still 1.
  - `TestHistoryTriggerBlocksMutation`: insert a `history_events` row directly; `UPDATE` and `DELETE` both fail with an error containing `history is append-only`.
  - `TestOneOpenRunIndex`: two `running` rows with the same `workflow_id` → second insert fails; after the first is set `completed`, the insert succeeds.
  - `TestOneWorkflowTaskPerRun`: two `kind='workflow'` task rows for one run → second fails.
  - `TestWithTxRollsBackOnError`: insert inside `WithTx` then return an error → row absent.
  - `TestFailpointBeforeCommitRollsBack`: set `store.Failpoint = func(p string){ if p == "t1/before_commit" { panic("boom") } }` (restore with `t.Cleanup`), run `WithTx(ctx, "t1", insert)` inside a `recover` wrapper → recovered `"boom"`, row absent.
  - `TestFailpointAfterCommitKeepsWrite`: same with `t1/after_commit` → panic recovered, row present.
  Run `GO test ./internal/store` → red.
- [ ] **Step 3: schema** (`schema.sql`, embedded with `//go:embed schema.sql`, applied in one transaction when `PRAGMA user_version` is 0, then `PRAGMA user_version = 1`):

```sql
CREATE TABLE workflows (
  run_id        TEXT PRIMARY KEY,
  workflow_id   TEXT NOT NULL,
  workflow_type TEXT NOT NULL,
  task_queue    TEXT NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('running','completed','failed','canceled')),
  next_event_id INTEGER NOT NULL,
  input         TEXT,
  result        TEXT,
  failure       TEXT,
  created_at    INTEGER NOT NULL,
  closed_at     INTEGER
);
CREATE UNIQUE INDEX workflows_one_open ON workflows(workflow_id) WHERE status = 'running';
CREATE INDEX workflows_by_id ON workflows(workflow_id, created_at);

CREATE TABLE history_events (
  run_id     TEXT NOT NULL,
  event_id   INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  attrs      TEXT NOT NULL,
  ts         INTEGER NOT NULL,
  PRIMARY KEY (run_id, event_id)
) WITHOUT ROWID;
CREATE TRIGGER history_no_update BEFORE UPDATE ON history_events
BEGIN SELECT RAISE(ABORT, 'history is append-only'); END;
CREATE TRIGGER history_no_delete BEFORE DELETE ON history_events
BEGIN SELECT RAISE(ABORT, 'history is append-only'); END;

CREATE TABLE tasks (
  task_id            INTEGER PRIMARY KEY,
  queue              TEXT NOT NULL,
  kind               TEXT NOT NULL CHECK (kind IN ('workflow','activity')),
  run_id             TEXT NOT NULL,
  scheduled_event_id INTEGER NOT NULL,
  started_event_id   INTEGER,
  attempt            INTEGER NOT NULL DEFAULT 1,
  timeout_ms         INTEGER NOT NULL,
  visible_at         INTEGER NOT NULL,
  lease_token        TEXT,
  lease_expires_at   INTEGER,
  UNIQUE (run_id, kind, scheduled_event_id)
);
CREATE INDEX tasks_poll ON tasks(queue, kind, visible_at);
CREATE UNIQUE INDEX tasks_lease ON tasks(lease_token) WHERE lease_token IS NOT NULL;
CREATE INDEX tasks_expiry ON tasks(lease_expires_at) WHERE lease_expires_at IS NOT NULL;
CREATE UNIQUE INDEX tasks_one_workflow_task ON tasks(run_id) WHERE kind = 'workflow';

CREATE TABLE timers (
  run_id           TEXT NOT NULL,
  started_event_id INTEGER NOT NULL,
  fire_at          INTEGER NOT NULL,
  PRIMARY KEY (run_id, started_event_id)
);
CREATE INDEX timers_due ON timers(fire_at);

CREATE TABLE buffered_events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id     TEXT NOT NULL,
  event_type TEXT NOT NULL,
  attrs      TEXT NOT NULL,
  ts         INTEGER NOT NULL
);
CREATE INDEX buffered_by_run ON buffered_events(run_id, seq);
```

- [ ] **Step 4: implement** `store.go`:

```go
// DSN used by Open. _txlock=immediate makes every BeginTx a BEGIN IMMEDIATE, so writers queue on
// busy_timeout instead of failing when a read lock upgrades (verified by prototype, ADR 0001).
const dsnParams = "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(ON)&_txlock=immediate"

type Store struct{ DB *sql.DB }
func Open(path string) (*Store, error)   // sql.Open("sqlite", "file:"+path+dsnParams); SetMaxOpenConns(8); migrate
func (s *Store) Close() error

// Failpoint, when non-nil, is called with "<tx name>/before_commit" and "<tx name>/after_commit".
// Tests set it to panic; production leaves it nil.
var Failpoint func(point string)

// WithTx runs fn in one BEGIN IMMEDIATE transaction named name. It rolls back on error or panic
// (the panic is re-raised) and commits otherwise.
func (s *Store) WithTx(ctx context.Context, name string, fn func(*Tx) error) error

type Tx struct{ tx *sql.Tx }
var ErrConflict = errors.New("store: concurrent modification")
var ErrDuplicateOpenRun = errors.New("store: workflow already has an open run")
```

Run `GO test -race ./internal/store` → `ok`.
- [ ] **Step 5: commit** `feat(store): SQLite schema with append-only history, WAL and failpoints`.

---

### Task 4: Store row operations with optimistic concurrency

**Files:** `internal/store/tx.go`, `internal/store/tx_test.go`.

- [ ] **Step 1: tests first:**
  - `TestAppendEventsDense`: insert workflow (next_event_id 1); `AppendEvents(run, 1, 2 events)` returns first id 1; `AppendEvents(run, 3, 1 event)` returns 3; `LoadHistory(run, 1, 100)` has ids `[1 2 3]`; workflow `next_event_id` is 4.
  - `TestAppendEventsConflict`: `AppendEvents(run, 1, ...)` twice with expected 1 → second returns `ErrConflict` and history length stays 1 (each in its own `WithTx`).
  - `TestAppendEventsConcurrent`: 20 goroutines each run `WithTx` that reads `next_event_id` then appends one event with that expected value → afterwards history is dense `1..20` (some calls may get `ErrConflict` and retry; with BEGIN IMMEDIATE none should).
  - `TestInsertWorkflowDuplicateOpen`: second open run with same `workflow_id` → `errors.Is(err, ErrDuplicateOpenRun)`.
  - `TestLeaseNextTaskOrderAndVisibility`: tasks visible at 100, 50, and 500 (now = 200) → leases the 50 one, then the 100 one, then none; leased rows have the token and `lease_expires_at = now + timeout_ms`.
  - `TestLeaseSkipsLeased`: a leased task is not returned again.
  - `TestRetryTaskClearsLease`: `RetryTask` sets `attempt`, `visible_at`, nulls the lease.
  - `TestDeleteTimerOnce`: `DeleteTimer` returns `true` then `false`.
  - `TestBufferedEventsFIFO`: `BufferEvent` ×3 then `TakeBufferedEvents` returns them in insert order and leaves none.
  Run → red.
- [ ] **Step 2: implement** `tx.go` with exactly these methods (all take/return Unix ms):

```go
type Workflow struct {
	RunID, WorkflowID, WorkflowType, TaskQueue, Status string
	NextEventID                                         int64
	Input, Result                                        *wire.Payload
	Failure                                              *wire.Failure
	CreatedAt, ClosedAt                                  int64
}
type NewEvent struct {
	Type  wire.EventType
	Attrs any // marshalled to JSON
	Time  int64
}
type Task struct {
	TaskID                     int64
	Queue, Kind, RunID         string // Kind: "workflow" | "activity"
	ScheduledEventID           int64
	StartedEventID             int64 // workflow tasks only, 0 if not started
	Attempt                    int
	TimeoutMS, VisibleAt       int64
	LeaseToken                 string // "" when not leased
	LeaseExpiresAt             int64
}
type Timer struct {
	RunID          string
	StartedEventID int64
	FireAt         int64
}

func (t *Tx) InsertWorkflow(w Workflow) error                       // ErrDuplicateOpenRun on the partial unique index
func (t *Tx) GetRun(runID string) (Workflow, bool, error)
func (t *Tx) GetOpenRun(workflowID string) (Workflow, bool, error)
func (t *Tx) GetLatestRun(workflowID string) (Workflow, bool, error) // ORDER BY created_at DESC, rowid DESC
func (t *Tx) CloseRun(runID, status string, result *wire.Payload, failure *wire.Failure, closedAt int64) error
// AppendEvents assigns ids expectedNext, expectedNext+1, ... It first runs
// UPDATE workflows SET next_event_id = expectedNext+len(evs) WHERE run_id=? AND next_event_id=expectedNext
// and returns ErrConflict if no row changed.
func (t *Tx) AppendEvents(runID string, expectedNext int64, evs []NewEvent) (firstID int64, err error)
func (t *Tx) LoadHistory(runID string, fromID int64, limit int) ([]wire.Event, error)
func (t *Tx) GetEvent(runID string, eventID int64) (wire.Event, bool, error)
func (t *Tx) InsertTask(task Task) (int64, error)
func (t *Tx) LeaseNextTask(queue, kind string, now int64, token string) (Task, bool, error)
	// UPDATE tasks SET lease_token=?, lease_expires_at=? + timeout_ms
	//   WHERE task_id = (SELECT task_id FROM tasks WHERE queue=? AND kind=? AND visible_at<=? AND lease_token IS NULL
	//                    ORDER BY visible_at, task_id LIMIT 1) RETURNING ...
func (t *Tx) SetTaskStarted(taskID, startedEventID int64) error
func (t *Tx) TaskByLease(token string) (Task, bool, error)
func (t *Tx) WorkflowTaskForRun(runID string) (Task, bool, error)
func (t *Tx) DeleteTask(taskID int64) error
func (t *Tx) RetryTask(taskID int64, attempt int, visibleAt int64) error
func (t *Tx) ExpiredTasks(now int64, limit int) ([]Task, error) // lease_expires_at <= now
func (t *Tx) PendingActivityTasks(runID string) ([]Task, error)
func (t *Tx) DeleteRunTasksTimersBuffer(runID string) error
func (t *Tx) InsertTimer(tm Timer) error
func (t *Tx) DeleteTimer(runID string, startedEventID int64) (bool, error)
func (t *Tx) DueTimers(now int64, limit int) ([]Timer, error) // fire_at <= now ORDER BY fire_at
func (t *Tx) TimersForRun(runID string) ([]Timer, error)
func (t *Tx) BufferEvent(runID string, ev NewEvent) error
func (t *Tx) TakeBufferedEvents(runID string) ([]NewEvent, error) // returns and deletes, FIFO
func (t *Tx) CountRuns(status string) (int, error)                 // "" = all
```

`NewEvent.Attrs` in `TakeBufferedEvents` comes back as `json.RawMessage` (marshalling a `RawMessage` is a no-op, so re-appending works). Run `GO test -race ./internal/store` → `ok`.
- [ ] **Step 3: commit** `feat(store): row operations, leases and optimistic concurrency`.

---

### Task 5: Retry policy (pure) with property test

**Files:** `internal/retry/retry.go`, `internal/retry/retry_test.go`.

- [ ] **Step 1: dependency** `GO get pgregory.net/rapid@v1.3.0`.
- [ ] **Step 2: tests first:**
  - `TestNormalizeDefaults`: zero policy → `InitialMS 1000, Backoff 2, MaxMS 100000, MaxAttempts 0`.
  - `TestBackoffClosedForm`: initial 100, backoff 2, max 1000 → attempts 1..6 give 100, 200, 400, 800, 1000, 1000 ms.
  - `TestNonRetryableByTypeAndFlag`: failure type listed in `NonRetryable` → no retry; `Failure.NonRetryable=true` → no retry.
  - `TestRetryPolicyBounds` (rapid): draw `InitialMS ∈ [1,5000]`, `Backoff ∈ [1,4]` (float), `MaxMS ∈ [InitialMS, 600000]`, `MaxAttempts ∈ [0,12]`, a list of up to 30 failures each with `Type ∈ {"A","B","Fatal"}` and `NonRetryable` bool, and policy `NonRetryable=["Fatal"]`. Simulate: `attempt := 1`; for each failure call `Next(p, attempt, f)`; if retry, assert `delay == closedForm(p, attempt)` where `closedForm = min(Initial*Backoff^(attempt-1), Max)` in ms computed with `math.Pow`, assert `delay <= Max`, assert delay is non-decreasing across the run, then `attempt++`. Assert: attempts are 1,2,3,... with no gaps; if `MaxAttempts>0` the last attempt ≤ `MaxAttempts`; a failure with type `Fatal` or `NonRetryable` stops at that attempt.
  Run → red.
- [ ] **Step 3: implement.**

```go
// Normalize fills defaults: InitialMS 1000, Backoff 2.0 (when < 1), MaxMS 100*InitialMS, MaxAttempts 0 = unlimited.
func Normalize(p wire.RetryPolicy) wire.RetryPolicy
// Delay returns min(InitialMS * Backoff^(attempt-1), MaxMS) for the attempt that just failed (1-based).
func Delay(p wire.RetryPolicy, attempt int) time.Duration
// Next decides whether the failed attempt is retried and after how long.
func Next(p wire.RetryPolicy, attempt int, f wire.Failure) (retry bool, delay time.Duration)
```

Compute in float64 milliseconds, clamp to `MaxMS` before converting, then `time.Duration(ms) * time.Millisecond` after `math.Round`. The test's `closedForm` must use the same rounding. Run `GO test -race ./internal/retry` → `ok` with `[rapid] OK, passed 100 tests` visible under `-v`.
- [ ] **Step 4: commit** `feat(retry): retry policy with closed-form backoff and property test`.

---

### Task 6: Core engine: start, describe, history, limits, single open run

**Files:** `internal/core/engine.go`, `internal/core/notify.go`, `internal/core/start.go`, `internal/core/helpers_test.go`, `internal/core/start_test.go`.

- [ ] **Step 1: tests first.** `helpers_test.go` provides `newEngine(t, cfg ...func(*Config)) (*Engine, *clock.Fake, *store.Store)` (temp db, fake clock at `2026-01-01T00:00:00Z`, short `PollTimeout` 50 ms), `start(t, e, id) string` (returns run id, input `{"n":1}`, type `"W"`, queue `"q"`) and `history(t, e, runID) []wire.Event`.
  - `TestStartAppendsStartedAndScheduled`: history types `[WorkflowExecutionStarted, WorkflowTaskScheduled]`, ids `[1 2]`, one workflow task row with `scheduled_event_id 2`; `Describe` gives `status running`, `history_length 2`.
  - `TestSingleOpenRunPerWorkflowID`: 50 goroutines call `StartWorkflow` with the same `workflow_id` → exactly 1 succeeds; the other 49 return `*core.Error` with `Code == "workflow_already_started"` and `RunID` equal to the winner's run id; `CountRuns("")` is 1.
  - `TestStartValidation`: empty `workflow_id`, `workflow_type` or `task_queue` → `invalid_request`.
  - `TestPayloadLimit`: config `MaxPayloadBytes = 1024`; input of 2000 bytes → `payload_too_large`, no run created.
  - `TestDescribeNotFound` → `not_found`.
  - `TestHistoryPaging`: `PageSize 1` returns event 1 with `NextEventID 2`; then event 2 with `NextEventID 0`.
  Run → red.
- [ ] **Step 2: implement** `engine.go`:

```go
type Config struct {
	WorkflowTaskTimeout time.Duration // default 10s
	PollTimeout         time.Duration // default 20s
	PollInterval        time.Duration // default 250ms; re-check for tasks that became visible by time
	MaxPayloadBytes     int           // default 2 << 20
	MaxHistoryEvents    int64         // default 50000
}
type Option func(*Engine)
// WithMetrics(m *metrics.Metrics) Option is added in Task 11, together with the m field; metrics calls must be nil-safe.
func WithObserver(f func(kind string, d time.Duration)) Option
type Engine struct { st *store.Store; clk clock.Clock; cfg Config; notify *notifier; obs func(string, time.Duration) }
func New(st *store.Store, clk clock.Clock, cfg Config, opts ...Option) *Engine

// Error is returned for every client-visible failure; api maps Code to an HTTP status.
type Error struct{ Code, Message, RunID string }
func (e *Error) Error() string
const (CodeInvalid = "invalid_request"; CodeNotFound = "not_found"; CodeAlreadyStarted = "workflow_already_started";
	CodeStaleLease = "stale_lease"; CodeRunClosed = "run_closed"; CodeConflict = "conflict"; CodeTooLarge = "payload_too_large")

// transition runs fn in one store transaction named kind. When fn reports changed=true and the commit
// succeeds, it calls the observer and metrics with the elapsed time. store.ErrConflict becomes CodeConflict.
func (e *Engine) transition(ctx context.Context, kind string, fn func(tx *store.Tx, now int64) (changed bool, err error)) error
```

`notify.go`: `notifier` with `wait(key string) <-chan struct{}` (returns the current channel) and `broadcast(key string)` (closes it and installs a fresh one). Keys are `"workflow/"+queue` and `"activity/"+queue`.

`start.go`: `StartWorkflow`, `Describe`, `History` per the spec. Run ids are 16 random bytes from `crypto/rand`, hex-encoded. `StartWorkflow` appends `WorkflowExecutionStarted` (attrs include `workflow_id`) and `WorkflowTaskScheduled{attempt:1}` and inserts a workflow task (`timeout_ms` = `WorkflowTaskTimeout`, `visible_at` = now), then broadcasts `"workflow/"+queue` after commit. On `store.ErrDuplicateOpenRun` it reads the open run (in a new read transaction) and returns `CodeAlreadyStarted` with its `RunID`.
Run `GO test -race ./internal/core` → `ok`.
- [ ] **Step 3: commit** `feat(core): engine skeleton, start, describe, history and limits`.

---

### Task 7: Workflow tasks: poll, complete, fail, command validation, stale leases

**Files:** `internal/core/wftask.go`, `internal/core/commands.go`, `internal/core/deliver.go`, `internal/core/wftask_test.go`, `internal/core/commands_test.go`.

- [ ] **Step 1: tests first.** Add helpers `pollWF(t, e) *wire.WorkflowTask` (uses `TryPollWorkflowTask`, fails the test if nil) and `complete(t, e, tok, cmds...)`.
  - `TestPollAppendsStartedAndReturnsFullHistory`: after start, `TryPollWorkflowTask` returns a task with history types `[Started, WFTScheduled, WFTStarted]`, `WFTStarted.scheduled_event_id == 2`, identity recorded; a second poll returns nil.
  - `TestCompleteScheduleActivity`: complete with `ScheduleActivity{Seq:1, ActivityType:"A"}` → history appends `[WFTCompleted{2,3}, ActivityTaskScheduled{seq 1}]` at ids 4,5; an activity task row exists with `scheduled_event_id 5`, queue `q`, `timeout_ms` 10000 when `StartToCloseMS` is 0; the workflow task row is gone.
  - `TestCompleteWorkflowCloses`: complete with `CompleteWorkflow{Result: {"ok":true}}` → last event `WorkflowExecutionCompleted`, `Describe.status == "completed"`, result round-trips, zero task and timer rows.
  - `TestStaleLeaseRejected`: complete with a random token → `stale_lease`; complete with the right token after `clk.Advance(WorkflowTaskTimeout + 1ms)` → `stale_lease`; history unchanged in both cases.
  - `TestOptimisticConcurrencyConflict`: two goroutines complete the same task (same token) with different commands → exactly one returns nil; the other returns `stale_lease` or `conflict`; history grew by exactly one completion batch.
  - `TestFailWorkflowTaskReschedulesWithBackoff`: `FailWorkflowTask{cause:"nondeterminism"}` → appended types exactly `[WorkflowTaskFailed, WorkflowTaskScheduled]` (no command events), new task `attempt 2`, `visible_at == now + 1s`; failing attempt 2 → `visible_at == now + 2s`; backoff caps at 30s.
  - `TestFailWorkflowTaskRejectsUnknownCause` → `invalid_request`.
  - `commands_test.go` `TestValidateCommands` (table): unknown type; `ScheduleActivity` with `Seq 0` or empty `ActivityType`; `StartTimer` with negative duration; two close commands; close command not last; input over the payload limit → each `invalid_request`; a valid mixed list → nil.
  Run → red.
- [ ] **Step 2: implement.**
  - `TryPollWorkflowTask(ctx, req) (*wire.WorkflowTask, error)`: one transition `"wft_start"`: `LeaseNextTask(queue, "workflow", now, token)`; append `WorkflowTaskStarted{scheduled_event_id, identity}`; `SetTaskStarted`; return the full history (`LoadHistory(run, 1, MaxHistoryEvents)`), `Attempt` from the task row.
  - `PollWorkflowTask(ctx, req)`: loop `{ ch := notify.wait(key); task := TryPoll...; if task != nil return; select ch / time.After(PollInterval) / ctx.Done() }` bounded by `PollTimeout`; returns `(nil, nil)` on timeout.
  - `CompleteWorkflowTask`: transition `"wft_complete"`, following spec Semantics 2 and 4 exactly:
    1. `TaskByLease(token)`; missing, wrong kind, or `lease_expires_at <= now` → `stale_lease`.
    2. Run closed → delete task, `run_closed`.
    3. `ValidateCommands(cmds, cfg)`.
    4. `buffered := TakeBufferedEvents(run)`; if a close command is present and `len(buffered) > 0`, drop the close command.
    5. Build events: `WorkflowTaskCompleted{scheduled, started}`, then one event per command (`commandEvent(cmd, run, now)`), then the buffered events, then `WorkflowTaskScheduled{attempt:1}` if the run stays open and `len(buffered) > 0`.
    6. History limit: every append in the engine goes through one helper, `e.appendEvents(tx, run, now, evs) (firstID int64, closed bool, err error)`. If `run.NextEventID-1+len(evs) > MaxHistoryEvents-1`, it appends only `WorkflowExecutionFailed{Failure{Type:"HistoryLimitExceeded"}}` instead (the last slot is reserved for it), closes the run (`CloseRun` + `DeleteRunTasksTimersBuffer`) and returns `closed=true`, so the caller skips its own row inserts. `TryPollWorkflowTask`, `deliver` and the reaper use it too.
    7. `AppendEvents(run, run.NextEventID, events)`; then, knowing each event's id, insert activity task rows (`scheduled_event_id` = its event id, queue = `cmd.TaskQueue` or the run's queue, `timeout_ms` = `StartToCloseMS` or 10000, attempt 1, visible now) and timer rows (`fire_at = now + DurationMS`); delete the workflow task row; insert the new workflow task row if scheduled.
    8. On close: `CloseRun` with status `completed`/`failed`/`canceled`, then `DeleteRunTasksTimersBuffer`.
    9. After commit: broadcast `"activity/"+queue` for each queue that got tasks and `"workflow/"+queue` if a workflow task was scheduled.
  - `FailWorkflowTask`: causes allowed `nondeterminism | panic | unknown_workflow_type | invalid_commands`. Transition `"wft_fail"`: verify lease; append `WorkflowTaskFailed`, buffered events, `WorkflowTaskScheduled{attempt+1}`; replace the task row with `visible_at = now + min(1s*2^(attempt-1), 30s)`.
  - `deliver.go`: `func (e *Engine) deliver(tx *store.Tx, run store.Workflow, now int64, evs []store.NewEvent) (scheduledWFT bool, err error)`: if the run's workflow task exists and is leased → `BufferEvent` each, return false. Otherwise append `evs` (+ `WorkflowTaskScheduled{attempt:1}` and a task row when no workflow task exists). Apply the history limit check here too. Used by Tasks 8 and 9.
  Run `GO test -race ./internal/core` → `ok`.
- [ ] **Step 3: commit** `feat(core): workflow task poll, complete and fail with lease checks`.

---

### Task 8: Activities: poll, complete, fail, retries, lease reaper

**Files:** `internal/core/activity.go`, `internal/core/reaper.go`, `internal/core/activity_test.go`, `internal/core/reaper_test.go`.

- [ ] **Step 1: tests first.** Helper `scheduleActivity(t, e, policy) (runID string)` = start + poll + complete with one `ScheduleActivity{Seq:1, ActivityType:"A", RetryPolicy:&policy, StartToCloseMS:5000}`.
  - `TestActivityCompleteAppendsAndSchedulesWFT`: `TryPollActivityTask` returns `ScheduledEventID 5`, `Attempt 1`, `DeadlineMS == now+5000`, input as sent. Complete with result `{"v":7}` → appended `[ActivityTaskStarted{5, attempt 1, identity}, ActivityTaskCompleted{5, result}, WorkflowTaskScheduled]`; one workflow task row; no activity task row.
  - `TestActivityRetryThenComplete`: policy `{InitialMS:100, Backoff:2, MaxAttempts:3}`; fail attempt 1 → no history change, task `attempt 2`, `visible_at = now+100`; poll before then returns nil; advance 100 ms; poll returns attempt 2; fail → `visible_at = now+200`; advance; attempt 3 completes → `ActivityTaskStarted.attempt == 3`.
  - `TestActivityRetriesExhausted`: `MaxAttempts 2`; two failures → `ActivityTaskFailed{attempts 2, failure type as reported}` appended plus `WorkflowTaskScheduled`.
  - `TestActivityNonRetryable`: failure with `NonRetryable:true` on attempt 1 → `ActivityTaskFailed{attempts 1}` immediately.
  - `TestActivityLeaseExpiryRetries`: poll, advance 5001 ms, `ReapExpiredLeases` → returns 1; task attempt 2, lease cleared, no history change.
  - `TestActivityLeaseExpiryFinalTimesOut`: `MaxAttempts 1`; expire + reap → `ActivityTaskTimedOut{attempts 1}` appended.
  - `TestZombieActivityCompletionRejected`: poll (token A), expire + reap, poll again (token B), complete with A → `stale_lease`, history unchanged; complete with B → ok.
  - `TestWorkflowTaskTimeout`: poll a workflow task, advance `WorkflowTaskTimeout+1ms`, reap → appended `[WorkflowTaskFailed{cause:"timeout"}, WorkflowTaskScheduled{attempt:2}]` and the new task is visible immediately; the old token now gets `stale_lease`.
  - `TestActivityResultTooLarge`: `MaxPayloadBytes 1024`, complete with a 2000-byte result → `ActivityTaskFailed` with failure type `PayloadTooLarge` (non-retryable).
  - `TestActivityCompletionAfterRunClosed`: close the run (complete workflow with the activity still pending) → the activity task row is gone; completing with the old token → `stale_lease`.
  Run → red.
- [ ] **Step 2: implement.**
  - `TryPollActivityTask` (transition `"act_start"`, no history write): lease; read the `ActivityTaskScheduled` event for type, input, `StartToCloseMS`; return `wire.ActivityTask` with `DeadlineMS = lease_expires_at`.
  - `PollActivityTask`: same long-poll loop as workflow tasks with key `"activity/"+queue`.
  - `CompleteActivityTask` (`"act_complete"`): verify lease (kind `activity`, unexpired); oversized result → treat as failure `{Type:"PayloadTooLarge", NonRetryable:true}`; delete the task; `deliver` `[ActivityTaskStarted, ActivityTaskCompleted]`.
  - `FailActivityTask` (`"act_fail"`): verify lease; policy from the scheduled event; `retry.Next`. Retry → `RetryTask(attempt+1, now+delay)` and count `wf_activity_retries_total`. Final → delete task, `deliver` `[ActivityTaskStarted{attempt}, ActivityTaskFailed{failure, attempts: attempt}]`.
  - `ReapExpiredLeases(ctx) (int, error)`: `ExpiredTasks(now, 100)` in a read transaction, then one transition `"reap"` per task that re-reads `TaskByLease` and re-checks expiry (skip if gone). Workflow kind → same append as `FailWorkflowTask` with cause `timeout` but the new task is visible immediately. Activity kind → failure `{Type:"StartToCloseTimeout"}` through `retry.Next`; final → `ActivityTaskTimedOut`. Broadcast as needed.
  Run `GO test -race ./internal/core` → `ok`.
- [ ] **Step 3: commit** `feat(core): activity tasks, server-side retries and lease reaper`.

---

### Task 9: Timers, signals, cancellation, buffering, history limit, loops

**Files:** `internal/core/timers.go`, `internal/core/loops.go`, extend `internal/core/start.go` (signal, cancel), `internal/core/timers_test.go`, `internal/core/signal_test.go`.

- [ ] **Step 1: tests first.**
  - `TestTimerNotEarly`: complete a task with `StartTimer{Seq:1, DurationMS:1000}` → `TimerStarted{fire_at: now+1000}` and a timer row. Advance 999 ms, `FireDueTimers` → 0 and no `TimerFired`. Advance 1 ms → 1, and `TimerFired{started_event_id}` + `WorkflowTaskScheduled` appended; no timer row.
  - `TestTimerFiresOnce`: 20 timers due; two goroutines each call `FireDueTimers` in a loop until both return 0 → exactly 20 `TimerFired` events in total across runs (one per timer), and every history is dense.
  - `TestSignalAppendsWhenIdle`: after the first task completes with no commands, `SignalWorkflow{name:"go", payload}` → appended `[WorkflowExecutionSignaled, WorkflowTaskScheduled]`.
  - `TestSignalBufferedDuringInflightTask`: poll (task in flight), signal twice → history unchanged; complete the task with `ScheduleActivity{Seq:1}` → appended in order `[WFTCompleted, ActivityTaskScheduled, Signaled, Signaled, WorkflowTaskScheduled]`.
  - `TestActivityResultBufferedDuringInflightTask`: the activity completes while a later workflow task is in flight → the result appears after that task's `WorkflowTaskCompleted` and command events.
  - `TestCloseDroppedWhenBuffered`: poll, signal, complete with `CompleteWorkflow` → run still `running`, appended `[WFTCompleted, Signaled, WorkflowTaskScheduled]`.
  - `TestSignalAfterCloseRejected` → `run_closed`; unknown workflow → `not_found`.
  - `TestCancelRequestAndCancelWorkflow`: `CancelWorkflow{reason}` → `WorkflowExecutionCancelRequested` appended; the next task completes with `CancelWorkflow` → status `canceled`, pending activity task and timer rows deleted.
  - `TestHistoryLengthLimit`: `MaxHistoryEvents 12`; loop signal → poll → complete-empty until the run closes → final status `failed`, last event `WorkflowExecutionFailed` with failure type `HistoryLimitExceeded`, history length ≤ 12.
  - `TestRunLoopsFiresTimersAndReaps`: real clock engine, `RunLoops(ctx, 10ms, 10ms)` in a goroutine, a 50 ms timer → `TimerFired` appears within 2 s.
  Run → red.
- [ ] **Step 2: implement.** `FireDueTimers(ctx) (int, error)`: `DueTimers(now, 100)` read, then one transition `"timer_fire"` per timer: `DeleteTimer` → if `false` skip (another loop won); `deliver` `[TimerFired]`. `SignalWorkflow` (`"signal"`) and `CancelWorkflow` (`"cancel"`) use `GetOpenRun`, payload limit, and `deliver`. `RunLoops(ctx, timerEvery, reapEvery)` runs two tickers until `ctx` ends, logging errors with `log/slog`.
  Run `GO test -race ./internal/core` → `ok`.
- [ ] **Step 3: commit** `feat(core): durable timers, signals, cancellation and event buffering`.

---

### Task 10: Consistency checker, rapid state machine, crash points

**Files:** `internal/store/check.go`, `internal/store/check_test.go`, `internal/core/statemachine_test.go`, `internal/core/crash_test.go`.

- [ ] **Step 1: checker tests first** (`check_test.go`): a hand-built consistent db → `CheckConsistency` returns no violations. Then one case per rule, each corrupting a fresh db with raw SQL: a gap in event ids; first event not `WorkflowExecutionStarted`; a closed run with a task row; an `ActivityTaskScheduled` without a terminal event and no task row; a terminal activity event that still has a task row; a `TimerStarted` without `TimerFired` and no timer row; buffered events on a run whose workflow task is not leased. Each must produce a violation string that names the run id.
- [ ] **Step 2: implement** `func CheckConsistency(ctx context.Context, db *sql.DB) ([]string, error)`, evaluated in Go per run (load history, tasks, timers, buffered events). Rules:
  1. Event ids are exactly `1..next_event_id-1`.
  2. Event 1 is `WorkflowExecutionStarted`.
  3. Closed runs have no task, timer or buffered rows.
  4. For a running run, each `ActivityTaskScheduled` E: if history **or buffered events** contain a terminal event (`ActivityTaskCompleted/Failed/TimedOut` with `scheduled_event_id = E`) → no activity task row for E; otherwise exactly one.
  5. Same for `TimerStarted` E with `TimerFired{started_event_id=E}` and timer rows.
  6. Running run: let L be the last `WorkflowTaskScheduled`. If no `WorkflowTaskCompleted` or `WorkflowTaskFailed` follows L → exactly one workflow task row with `scheduled_event_id = L`; otherwise none.
  7. Buffered events exist → the run's workflow task row exists and is leased.
- [ ] **Step 3: state machine** `TestHistoryDenseAppendOnly` (`statemachine_test.go`) using `rapid.Check` and `t.Repeat` with actions: `start` (id drawn from 5 ids), `pollAndComplete` (random command list: 0-2 `ScheduleActivity`, 0-1 `StartTimer` with 0-2000 ms, optional `RecordMarker`, optional close as last; `Seq` increasing per run, tracked in the model), `pollAndFailWF`, `pollActivity` then complete or fail (non-retryable or not), `signal`, `cancel`, `advance` (0-3000 ms) then `FireDueTimers` and `ReapExpiredLeases`, and `""` (the check). The check, after every action: `CheckConsistency` returns nothing, and for every run the new history has the previously seen history as an exact prefix (byte-equal events) and ids are dense. Each `rapid` run uses a fresh temp db. Keep it under ~20 s at the default 100 checks.
- [ ] **Step 4: crash points** `TestCrashPointConsistency` (`crash_test.go`): for each `(tx name, point)` in `{start, wft_start, wft_complete, wft_fail, act_start, act_complete, act_fail, timer_fire, reap, signal, cancel} × {before_commit, after_commit}`: on a file-backed store, drive one workflow scenario that exercises that transaction; arm `store.Failpoint` to panic on that point the first time it is hit; recover the panic in the test; close the store, reopen the same path, build a fresh engine; assert `CheckConsistency` is empty; then disarm and drive the scenario to the end (retrying the interrupted step where the protocol allows, e.g. re-poll after reap) and assert the run ends `completed` and the checker is still empty. Use subtests named `tx/point`.
  Run `GO test -race ./internal/store ./internal/core` → `ok`. Confirm red by temporarily removing the `UPDATE ... WHERE next_event_id=?` guard (the conflict tests fail) and restore it.
- [ ] **Step 5: commit** `test(core): consistency checker, rapid state machine and crash points`.

---

### Task 11: Metrics and HTTP API

**Files:** `internal/metrics/metrics.go`, `internal/metrics/metrics_test.go`, `internal/api/api.go`, `internal/api/errors.go`, `internal/api/api_test.go`.

- [ ] **Step 1: dependency** `GO get github.com/prometheus/client_golang@v1.24.1`.
- [ ] **Step 2: metrics.** `metrics.New() *Metrics` with its own registry and: `wf_transitions_total{kind}`, `wf_transition_seconds{kind}` (histogram, buckets `0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1`), `wf_lease_expiries_total{kind}`, `wf_activity_retries_total`, `wf_workflow_task_failures_total{cause}`, `wf_workflows_closed_total{status}`, plus `Handler() http.Handler`. Wire `core.WithMetrics` into `transition`, the reaper, `FailActivityTask`, `FailWorkflowTask` and `CloseRun` callers. Test: `TestMetricsExposed` scrapes the handler after one `Inc` per metric.
- [ ] **Step 3: API tests first** (`api_test.go`, `httptest.NewServer(api.New(engine, metrics).Handler())`, real clock, `PollTimeout` 100 ms):
  - `TestHealthz`: `GET /healthz` → 200 `ok`.
  - `TestStartDescribeHistoryOverHTTP`: start → 200 `{"run_id":...}`; describe → `running`; history → 3 events after a poll.
  - `TestAlreadyStarted409`: second start → 409, body `{"error":{"code":"workflow_already_started","run_id":...}}`.
  - `TestPollTimeout204`: poll an empty queue → 204 after about 100 ms.
  - `TestStaleLease409`: complete with a bad token → 409 `stale_lease`.
  - `TestBadJSON400` and `TestMethodNotAllowed` (GET on a POST route → 405).
  - `TestBodyTooLarge413`: body larger than `MaxPayloadBytes + 1 MiB` → 413.
  - `TestMetricsEndpoint`: `GET /metrics` contains `wf_transitions_total{kind="start"} 1` after one start.
  Run → red.
- [ ] **Step 4: implement.** `api.New(e *core.Engine, m *metrics.Metrics, maxBody int64) *Server`; `Handler()` uses `http.ServeMux` with method patterns (`"POST /v1/workflows/start"`, etc.). Routes: `/v1/workflows/{start,signal,cancel,describe,history}`, `/v1/tasks/workflow/{poll,complete,fail}`, `/v1/tasks/activity/{poll,complete,fail}`, `GET /healthz`, `GET /metrics`. Poll handlers pass `r.Context()`; `(nil, nil)` → 204. `errors.go` maps `core.Error` codes to statuses (spec "Interfaces"); anything else → 500 `internal` (log it, never echo internals).
  Run `GO test -race ./internal/metrics ./internal/api` → `ok`.
- [ ] **Step 5: commit** `feat(api): HTTP JSON API with long-poll and Prometheus metrics`.

---

### Task 12: `wfd` binary

**Files:** `cmd/wfd/main.go`.

- [ ] **Step 1: implement.** Flags: `--listen` (`:7233`), `--db` (`wf.db`), `--wft-timeout` (10s), `--poll-timeout` (20s), `--timer-interval` (100ms), `--reaper-interval` (500ms), `--max-history-events` (50000), `--max-payload-bytes` (2097152). Opens the store, builds engine + metrics + API, starts `RunLoops`, serves with `ReadHeaderTimeout 5s`. `SIGINT`/`SIGTERM` → `Shutdown` with 5 s timeout, cancel loops, close store. Logs with `slog` text handler: `wfd listening addr=... db=...`.
- [ ] **Step 2: smoke** (one shell in the container):
  `GOSH 'go build -o /tmp/wfd ./cmd/wfd && (/tmp/wfd --listen 127.0.0.1:7233 --db /tmp/wf.db &) && sleep 1 && wget -qO- http://127.0.0.1:7233/healthz && wget -qO- --post-data "{\"workflow_id\":\"s1\",\"workflow_type\":\"W\",\"task_queue\":\"q\"}" http://127.0.0.1:7233/v1/workflows/start'`
  Expected: `ok` then `{"run_id":"<32 hex>"}`. (`golang:1.26.8` is Debian-based and has `wget`; if not, use `curl`.)
- [ ] **Step 3: commit** `feat(wfd): server binary with background loops and graceful shutdown`.

---

### Task 13: SDK client and in-process test server

**Files:** `sdk/client/client.go`, `sdk/client/errors.go`, `sdk/client/client_test.go`, `internal/testserver/testserver.go`.

- [ ] **Step 1: test server.** `testserver.Start(t testing.TB, opts ...func(*core.Config)) *Server` with fields `URL string`, `Engine *core.Engine`, `Store *store.Store`. It uses a temp-dir SQLite file, real clock, `PollTimeout 500ms`, `WorkflowTaskTimeout 2s`, `httptest.NewServer`, and `RunLoops(ctx, 10ms, 50ms)`; `t.Cleanup` stops everything.
- [ ] **Step 2: tests first** (`client_test.go`, package `client_test`):
  - `TestStartDescribeHistory`: `Start(ctx, client.StartOptions{ID:"c1", TaskQueue:"q"}, "W", map[string]int{"n":1})` returns a run id; `Describe` → `running`; `History` → 2 events.
  - `TestStartAlreadyStarted`: second start → `client.IsAlreadyStarted(err)` returns `(runID, true)`.
  - `TestSignalCancel`: `Signal` and `Cancel` succeed; history shows both events.
  - `TestHistoryFollowsPages`: 2500 signals on a run whose task is never polled → `History` returns all events (pages of 1000).
  - `TestRetriesTransportErrors`: a listener that closes the first two connections, then proxies to the test server → `Start` succeeds; with `WithRetry(0)` the same call fails.
  - `TestGetResult`: complete the workflow directly via the engine with `CompleteWorkflow{Result: 42}`; `GetResult(ctx, "c1", &n)` gives 42; a failed run gives `*client.WorkflowFailedError`; a canceled run gives `client.ErrWorkflowCanceled`.
  - `TestHealth`.
  Run → red.
- [ ] **Step 3: implement.**

```go
type Client struct{ /* base URL, http.Client, retry budget */ }
type Option func(*Client)
func WithHTTPClient(h *http.Client) Option
func WithRetry(maxElapsed time.Duration) Option // default 30s; 0 disables. Retries transport errors and 5xx, backoff 50ms doubling to 1s.
func New(baseURL string, opts ...Option) *Client
type StartOptions struct{ ID, TaskQueue string }
func (c *Client) Start(ctx context.Context, o StartOptions, workflow any, input any) (string, error) // workflow: name string or func (name via worker naming rule, see Task 16; put the helper in sdk/internal/naming)
func (c *Client) Signal(ctx context.Context, workflowID, name string, payload any) error
func (c *Client) Cancel(ctx context.Context, workflowID, reason string) error
func (c *Client) Describe(ctx context.Context, workflowID string) (wire.DescribeResponse, error)
func (c *Client) History(ctx context.Context, workflowID, runID string) ([]wire.Event, error)
func (c *Client) GetResult(ctx context.Context, workflowID string, out any) error // polls Describe every 100ms
func (c *Client) Health(ctx context.Context) error
// Task-level calls used by the worker:
func (c *Client) PollWorkflowTask(ctx context.Context, queue, identity string) (*wire.WorkflowTask, error) // nil on 204
func (c *Client) CompleteWorkflowTask(ctx context.Context, req wire.CompleteWorkflowTaskRequest) error
func (c *Client) FailWorkflowTask(ctx context.Context, req wire.FailWorkflowTaskRequest) error
func (c *Client) PollActivityTask(ctx context.Context, queue, identity string) (*wire.ActivityTask, error)
func (c *Client) CompleteActivityTask(ctx context.Context, req wire.CompleteActivityTaskRequest) error
func (c *Client) FailActivityTask(ctx context.Context, req wire.FailActivityTaskRequest) error

type APIError struct{ Status int; Code, Message, RunID string }
func IsAlreadyStarted(err error) (runID string, ok bool)
func IsStaleLease(err error) bool
type WorkflowFailedError struct{ Failure wire.Failure }
var ErrWorkflowCanceled = errors.New("workflow canceled")
```

`sdk/internal/naming`: `func Name(fn any) string` = `runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()`, keep the part after the last `.`, trim a `-fm` suffix; strings pass through. Test: plain func → `"Debit"`; method value `(*Ledger).Debit` → `"Debit"`.
Run `GO test -race ./sdk/client ./sdk/internal/naming` → `ok`.
- [ ] **Step 4: commit** `feat(sdk): HTTP client with retries and in-process test server`.

---

### Task 14: Deterministic dispatcher

**Files:** `sdk/internal/wfrt/dispatcher.go`, `sdk/internal/wfrt/dispatcher_test.go`.

This is the riskiest code. Implement this design exactly (it matches the prototype, ADR 0005):

```go
type resumeMsg int

const (
	resumeRun resumeMsg = iota
	resumeExit
)

type yieldMsg struct {
	progressed bool
	done       bool
	panicVal   any
	stack      []byte
}

type coroutine struct {
	resume chan resumeMsg
	yield  chan yieldMsg
	done   bool
}

// exitSignal unwinds a parked coroutine when the dispatcher closes.
type exitSignal struct{}

type dispatcher struct {
	coros []*coroutine
	cur   *coroutine
}

func (d *dispatcher) spawn(fn func()) {
	c := &coroutine{resume: make(chan resumeMsg), yield: make(chan yieldMsg)}
	d.coros = append(d.coros, c)
	go func() {
		defer func() {
			r := recover()
			switch r.(type) {
			case nil:
				c.yield <- yieldMsg{done: true, progressed: true}
			case exitSignal:
				c.yield <- yieldMsg{done: true}
			default:
				c.yield <- yieldMsg{done: true, panicVal: r, stack: debug.Stack()}
			}
		}()
		if <-c.resume == resumeExit {
			panic(exitSignal{})
		}
		fn()
	}()
}

// block parks the running coroutine until cond holds. Only SDK blocking calls use it.
func (d *dispatcher) block(cond func() bool) {
	c := d.cur
	progressed := true
	for !cond() {
		c.yield <- yieldMsg{progressed: progressed}
		if <-c.resume == resumeExit {
			panic(exitSignal{})
		}
		progressed = false
	}
}

// runUntilBlocked resumes coroutines in creation order (including ones spawned during the pass)
// until a full pass makes no progress or stop() is true.
func (d *dispatcher) runUntilBlocked(stop func() bool) error {
	for {
		progressed := false
		for i := 0; i < len(d.coros); i++ {
			c := d.coros[i]
			if c.done {
				continue
			}
			d.cur = c
			c.resume <- resumeRun
			m := <-c.yield
			d.cur = nil
			c.done = m.done
			if m.panicVal != nil {
				return &PanicError{Value: m.panicVal, Stack: string(m.stack)}
			}
			if m.progressed {
				progressed = true
			}
			if stop() {
				return nil
			}
		}
		if !progressed {
			return nil
		}
	}
}

// close unwinds every unfinished coroutine and waits for each goroutine to exit.
func (d *dispatcher) close() {
	for _, c := range d.coros {
		if !c.done {
			c.resume <- resumeExit
			<-c.yield
			c.done = true
		}
	}
}

type PanicError struct {
	Value any
	Stack string
}
```

- [ ] **Step 1: tests first:**
  - `TestDispatcherDeterministicOrder`: root logs `a1`, spawns child (logs `b1`, sets flag), blocks on flag, logs `a2` → order `[a1 b1 a2]` in 1000 of 1000 runs.
  - `TestDispatcherInterleaving`: three coroutines each loop 3 times `{log; set own flag; block on next coroutine's flag}` → the log equals a fixed expected sequence on every one of 200 runs.
  - `TestDispatcherStopsWhenAllBlocked`: a coroutine blocked on a never-true condition → `runUntilBlocked` returns nil; set the condition from the test, run again → it completes.
  - `TestDispatcherPanicCaptured`: a coroutine panics with `"boom"` → `*PanicError` with `Value == "boom"` and a stack containing `dispatcher_test.go`.
  - `TestDispatcherNoGoroutineLeak`: record `runtime.NumGoroutine()`, create 100 dispatchers each with 5 parked coroutines, `close()` them, then poll up to 1 s until the count is back within +2 of the start.
  - `TestDispatcherStopFunc`: `stop` returning true after the first coroutine finishes prevents later coroutines from running in that call.
  Run `GO test -race ./sdk/internal/wfrt` → red, then implement, then `ok`.
- [ ] **Step 2: commit** `feat(sdk): deterministic coroutine dispatcher`.

---

### Task 15: Workflow API and replay engine

**Files:** `sdk/internal/wfrt/env.go`, `sdk/internal/wfrt/api.go`, `sdk/internal/wfrt/invoke.go`, `sdk/internal/wfrt/replay.go`, `sdk/internal/wfrt/replay_test.go`, `sdk/internal/wfrt/histbuilder_test.go`, `sdk/workflow/workflow.go`.

- [ ] **Step 1: public surface** (`api.go`, re-exported by `sdk/workflow`):

```go
// Context is the deterministic workflow context. Only this runtime implements it.
type Context interface{ env() *env }
type Future interface {
	Get(ctx Context, valuePtr any) error
	IsReady() bool
}
type ReceiveChannel interface {
	Receive(ctx Context, valuePtr any) (ok bool) // false only when canceled with an empty queue
	ReceiveAsync(valuePtr any) (ok bool)
}
type Selector interface {
	AddFuture(f Future, fn func(Future)) Selector
	AddReceive(ch ReceiveChannel, fn func(ReceiveChannel)) Selector
	Select(ctx Context) error // runs the first ready case in add order; ErrCanceled if canceled and none ready
}
type RetryPolicy struct {
	Initial      time.Duration
	Backoff      float64
	Max          time.Duration
	MaxAttempts  int
	NonRetryable []string
}
type ActivityOptions struct {
	TaskQueue    string
	StartToClose time.Duration // default 10s
	Retry        RetryPolicy
}
type Encoded struct{ p *wire.Payload }
func (e Encoded) Get(valuePtr any) error
type ActivityError struct {
	ActivityType string
	Failure      wire.Failure
	TimedOut     bool
}
var ErrCanceled = errors.New("workflow: canceled")

func ExecuteActivity(ctx Context, opts ActivityOptions, activity any, input any) Future
func NewTimer(ctx Context, d time.Duration) Future
func Sleep(ctx Context, d time.Duration) error
func Now(ctx Context) time.Time
func SideEffect(ctx Context, fn func() any) Encoded
func GetSignalChannel(ctx Context, name string) ReceiveChannel
func Go(ctx Context, fn func(ctx Context))
func NewSelector(ctx Context) Selector
func IsCanceled(ctx Context) bool
```

`sdk/workflow/workflow.go` declares `type Context = wfrt.Context` (and the same for `Future`, `ReceiveChannel`, `Selector`, `RetryPolicy`, `ActivityOptions`, `Encoded`, `ActivityError`), `var ErrCanceled = wfrt.ErrCanceled`, and one-line wrapper functions with doc comments.

- [ ] **Step 2: behaviour** (`env.go`, `api.go`), per spec Semantics 7-11:
  - `env` holds: dispatcher, `seq int64`, `cmds []wire.Command` (current step), `now time.Time`, `markers map[int64]*wire.Payload` (pre-scanned from all `MarkerRecorded`), `activities`/`timers map[int64]*future` keyed by seq, `eventToSeq map[int64]int64`, `signals map[string]*channel`, `canceled bool`, `rootDone bool`, `closeCmd *wire.Command`, `taskQueue string`.
  - `ExecuteActivity`: if canceled → ready future with `ErrCanceled`, no command, no seq. Else `seq++`, append `ScheduleActivity{Seq, ActivityType: naming.Name(activity), TaskQueue, Input, RetryPolicy (ms), StartToCloseMS}`.
  - `NewTimer`: `d <= 0` → ready future, no command. Else `seq++`, `StartTimer{Seq, DurationMS}`.
  - `Future.Get`: `block(func() bool { return f.ready || env.canceled })`; not ready → `ErrCanceled`; error → return it; decode value into `valuePtr` when both are non-nil.
  - `SideEffect`: `seq++`; value = `markers[seq]` if present, else `wire.MustEncode(fn())`; always append `RecordMarker{Seq, MarkerKind:"side_effect", Value}`.
  - Signal channels: FIFO of payloads; `Receive` blocks until non-empty or canceled.
  - `Go`: `disp.spawn(func(){ fn(ctx) })`.
  - Root coroutine (`invoke.go`): decode input into the function's second parameter type via reflection, call it, then build the close command: nil error → `CompleteWorkflow{Result}` (result only when the function has two return values); `errors.Is(err, ErrCanceled) && env.canceled` → `CancelWorkflow`; `*ActivityError` → `FailWorkflow{Failure: its failure}`; otherwise `FailWorkflow{Failure{Type:"WorkflowError", Message: err.Error()}}`. Store it in `env.closeCmd`, append to `cmds`, set `rootDone`. `ValidateWorkflowFunc(fn any) error` accepts `func(Context, T) (R, error)` and `func(Context, T) error` only.
- [ ] **Step 3: replay** (`replay.go`):

```go
type NondeterminismError struct{ Msg string }

// Replay runs fn against history. If the last event is WorkflowTaskStarted it returns the new
// commands for that task; otherwise it validates the whole history and returns nil commands.
func Replay(history []wire.Event, fn any) ([]wire.Command, error)
```

Algorithm (spec Semantics 6; write it as a loop over index `i`):
  1. `history[0]` must be `WorkflowExecutionStarted`; decode input; pre-scan markers; spawn the root coroutine (not run yet). `defer disp.close()`.
  2. For each event: `WorkflowTaskStarted` → `last := i == len-1`; if not last and `history[i+1].Type != WorkflowTaskCompleted`, skip it (a failed or timed-out attempt). Otherwise set `env.now` to its timestamp, reset `cmds`, and if `rootDone` set `cmds = [*closeCmd]`, else `runUntilBlocked(func() bool { return env.rootDone })` (a `*PanicError` is returned as is). If `last`, return `cmds`. Else match `cmds` against the contiguous command events starting at `i+2`: same `wire.EventTypeFor(cmd.Type)`; same `Seq` for activity, timer and marker; same `ActivityType` for activities. On match of `ActivityTaskScheduled`/`TimerStarted`, record `eventToSeq[eventID] = seq`. A mismatch, or history having more command events than `cmds`, → `*NondeterminismError` naming the event id, expected and actual. Leftover `cmds` are allowed only if exactly one close command remains (Semantics 4). Continue after the last matched command event.
  3. `ActivityTaskCompleted/Failed/TimedOut` → resolve `activities[eventToSeq[scheduled_event_id]]` (failed → `*ActivityError`; timed out → `*ActivityError{TimedOut:true}`). `TimerFired` → resolve the timer. `WorkflowExecutionSignaled` → push payload. `WorkflowExecutionCancelRequested` → `canceled = true`. Command events met outside a step's batch → `fmt.Errorf("wfrt: corrupt history at event %d", id)`. Everything else → skip.
  4. End of history without a trailing `WorkflowTaskStarted` → `nil, nil`.
- [ ] **Step 4: tests first** (`histbuilder_test.go` gives a tiny builder: `b := newHist("W", input)`; `b.task()` appends `WFTScheduled` + `WFTStarted`; `b.completed(cmds...)` appends `WFTCompleted` + command events with correct ids and returns their ids; `b.activityDone(schedID, result)`; `b.timerFired(startedID)`; `b.signal(name, v)`; `b.cancel()`; `b.failedTask(cause)`):
  - `TestReplayFirstStepSchedulesActivity`: workflow calls `ExecuteActivity("A")` → commands `[ScheduleActivity{Seq:1, ActivityType:"A"}]`.
  - `TestReplayResolvesActivityAndCompletes`: history with the activity completed (result 41) and a new task → `[CompleteWorkflow{Result: 42}]` for a workflow that returns `r+1`.
  - `TestReplaySkipsFailedTaskStep`: a `failedTask("timeout")` attempt between steps does not run code and does not shift matching.
  - `TestReplayNondeterminism`: history says activity `"A"` at seq 1; code calls `"B"` → `*NondeterminismError` mentioning `A` and `B`. Also: code calls `Sleep` where history has `ActivityTaskScheduled` → error.
  - `TestReplayExtraCommandInHistory`: history has 2 command events, code produces 1 → error.
  - `TestNowIsDeterministic`: `WFTStarted.Time = 1700000000123`; workflow returns `workflow.Now(ctx).UnixMilli()` → `CompleteWorkflow` result `1700000000123`, identical over 50 replays.
  - `TestSideEffectRecordedValueWins`: first replay calls `fn` (counter increments, value 1) and emits `RecordMarker{Seq:1, value 1}`; a history containing `MarkerRecorded{seq 1, value 99}` → returns 99 and `fn` is not called.
  - `TestReplaySignalsAndSelector`: workflow selects between a signal and a 1h timer; history has the signal → completes with the signal payload; the timer was started (seq ordering checked).
  - `TestReplayCancel`: cancel requested while sleeping → `Sleep` returns `ErrCanceled`, workflow returns it → `[CancelWorkflow]`.
  - `TestReplayTrailingCloseAllowed`: a step whose close command was dropped (history: `WFTCompleted` then `Signaled`, `WFTScheduled`, `WFTStarted`) → no error; the next step re-emits the same close.
  - `TestReplayActivityFailure`: `ActivityTaskFailed` → `Get` returns `*ActivityError` with that failure; workflow returning it → `FailWorkflow` with the same failure type.
  - `TestReplayGoroutineCount`: after 100 `Replay` calls on a workflow with 3 `workflow.Go` children parked forever, goroutine count returns to baseline (+2).
  Run `GO test -race ./sdk/...` → red, implement, then `ok`.
- [ ] **Step 5: commit** `feat(sdk): workflow API and deterministic replay engine`.

---

### Task 16: Activity package and worker

**Files:** `sdk/activity/activity.go`, `sdk/activity/activity_test.go`, `sdk/worker/worker.go`, `sdk/worker/registry.go`, `sdk/worker/replay.go`, `sdk/worker/worker_test.go`.

- [ ] **Step 1: activity package.**

```go
type Info struct {
	WorkflowID, RunID, ActivityType string
	ScheduledEventID                int64
	Attempt                         int
	IdempotencyKey                  string // RunID + ":" + ScheduledEventID
	Deadline                        time.Time
}
func GetInfo(ctx context.Context) Info
func WithInfo(ctx context.Context, info Info) context.Context // used by the worker
type Error struct{ Type, Message string; NonRetryable bool }
func NewError(typ, msg string) error
func NewNonRetryableError(typ, msg string) error
// ToFailure converts any error to a wire.Failure (Type "GenericError" unless it is an *Error).
func ToFailure(err error) wire.Failure
```

Tests: `TestIdempotencyKeyFormat` (`"r1:5"`), `TestToFailure` (plain error → `GenericError`; `NewNonRetryableError("Fatal","x")` → `NonRetryable:true`; wrapped `*Error` found through `errors.As`).
- [ ] **Step 2: worker tests first** (`worker_test.go`, package `worker_test`, using `testserver.Start`):
  - `TestFlow1EndToEnd`: workflow `func Hello(ctx workflow.Context, name string) (string, error)` runs activity `Greet` and returns its result; `Start` + `GetResult` → `"hello, ada"` within 10 s. History types in order: `Started, WFTScheduled, WFTStarted, WFTCompleted, ActivityTaskScheduled, ActivityTaskStarted, ActivityTaskCompleted, WFTScheduled, WFTStarted, WFTCompleted, WorkflowExecutionCompleted`.
  - `TestActivityRetryEndToEnd`: activity fails twice (counter) then succeeds; `MaxAttempts 5`, `Initial 10ms` → completes; `ActivityTaskStarted.attempt == 3`.
  - `TestNonRetryableEndToEnd`: activity returns `NewNonRetryableError("Fatal", ...)` → workflow fails; `GetResult` returns `*client.WorkflowFailedError` with `Failure.Type == "Fatal"`; the activity ran once.
  - `TestSleepSignalEndToEnd`: workflow waits for signal `"go"` or a 30 s timer via `Selector`; the test signals → result is the signal payload.
  - `TestCancelEndToEnd`: workflow sleeps 1 h; `Cancel` → status `canceled`.
  - `TestNondeterminismDetected`: v1 = `ExecuteActivity("A")` then `Sleep(1h)`. Run worker A with v1 until history contains `TimerStarted`, then stop it. Start worker B with v2 registered under the same name; v2 calls `ExecuteActivity("B")` first. Send any signal to force a new workflow task. Wait for `WorkflowTaskFailed{cause:"nondeterminism"}`; assert no `ActivityTaskScheduled` with type `B` was appended and the run is still `running`.
  - `TestUnknownWorkflowType`: no registration → `WorkflowTaskFailed{cause:"unknown_workflow_type"}`.
  - `TestPanicInWorkflow` → `WorkflowTaskFailed{cause:"panic"}`.
  - `TestActivityInfo`: activity echoes `activity.GetInfo(ctx).IdempotencyKey` → equals `runID + ":5"`.
  Run → red.
- [ ] **Step 3: implement.**

```go
type Options struct {
	Identity        string // default hostname:pid
	WorkflowPollers int    // default 2
	ActivityPollers int    // default 4
}
func New(c *client.Client, queue string, o Options) *Worker
func (w *Worker) RegisterWorkflow(fn any)                    // panics on a bad signature
func (w *Worker) RegisterWorkflowWithName(name string, fn any)
func (w *Worker) RegisterActivity(fn any)
func (w *Worker) RegisterActivityWithName(name string, fn any)
func (w *Worker) Run(ctx context.Context) error             // blocks; returns nil when ctx is canceled
func ReplayHistory(history []wire.Event, workflowFn any) error
func ReplayHistoryFile(path string, workflowFn any) error    // accepts a JSON []wire.Event or a wire.HistoryResponse
```

Workflow poller loop: poll (a 204 means poll again; transport errors back off 100 ms doubling to 2 s, reset on success) → look up the type (missing → fail `unknown_workflow_type`) → `wfrt.Replay` → `*NondeterminismError` → fail `nondeterminism`; `*PanicError` or other error → fail `panic`; success → `CompleteWorkflowTask`. A `stale_lease` reply is logged and dropped. `ReplayHistory` runs `wfrt.Replay` and returns its error (it ignores commands of a trailing open task).
Activity poller loop: poll → registry lookup (missing → fail non-retryable `UnknownActivityType`) → decode input into the parameter type → call with `context.WithDeadline(activity.WithInfo(ctx, info), deadline)` and recover panics (failure type `Panic`) → complete or fail with `activity.ToFailure(err)`. Completion calls retry through the client's retry budget.
Run `GO test -race ./sdk/...` → `ok`.
- [ ] **Step 4: commit** `feat(sdk): worker with workflow and activity pollers, activity info`.

---

### Task 17: Golden histories

**Files:** `sdk/worker/golden_test.go`, `sdk/worker/testdata/histories/{hello,retry,signal,cancel,sideeffect}.json`.

- [ ] **Step 1: test first.** `golden_test.go` defines five scenario workflows (reuse the Task 16 ones plus a `SideEffect` workflow) and a flag `var update = flag.Bool("update", false, "rewrite golden histories")`. With `-update`, each scenario runs end to end against `testserver.Start` and writes its closed history (`json.MarshalIndent`) to `testdata/histories/<name>.json`. Without it, `TestReplayGoldenHistories` loads each file and runs `worker.ReplayHistory` with the scenario's workflow function → must return nil; it also asserts every file ends in a close event. A second test, `TestGoldenDetectsChange`, replays `hello.json` against a modified workflow (different activity name) → `*wfrt.NondeterminismError` (via `errors.As` on the exported alias `worker.NondeterminismError`).
- [ ] **Step 2: generate** `GO test ./sdk/worker -run TestReplayGoldenHistories -update` then run without `-update` → `ok`. Commit the five JSON files.
- [ ] **Step 3: commit** `test(sdk): golden histories as the determinism regression net`.

---

### Task 18: Transfer example with an idempotent ledger

**Files:** `examples/transfer/transfer.go`, `examples/transfer/ledger.go`, `examples/transfer/ledger_test.go`, `examples/transfer/transfer_test.go`, `examples/transfer/cmd/transfer-worker/main.go`.

- [ ] **Step 1: tests first.**
  - `ledger_test.go` `TestApplyIsIdempotent` (this is spec Flow 2): `Debit` called twice with a context whose `activity.Info` has the same `IdempotencyKey` → `executions` has 2 rows, `ledger` has 1 row; `Check()` reports `ReExecutions 1, DoubleApplied 0`.
  - `TestUnsafeLedgerDoubleApplies`: same with `Unsafe: true` → `ledger` has 2 rows for that `(run_id, kind)`; `Check()` reports `DoubleApplied 1`.
  - `TestAppliedPerRun`: after a debit and a credit for run `r1`, `Applied(ctx, "r1")` returns `(1, 1)`.
  - `transfer_test.go` `TestTransferEndToEnd`: `testserver.Start` + a worker with `transfer.Register(w, ledger)`; start `Transfer{From:"acct-1", To:"acct-2", AmountCents:1200}` → result is a 32-hex receipt; ledger has a `-1200` debit on `acct-1` and a `+1200` credit on `acct-2`; history contains `MarkerRecorded` and `TimerStarted`.
  - `TestTransferReplayCommand`: run the binary's `replay` code path (`run([]string{"replay","--history",file}, &out, &errOut)`) on a history saved from the end-to-end run → exit code 0 and output `replay ok: <n> events`.
  Run → red.
- [ ] **Step 2: implement.**

```go
// transfer.go
type Input struct {
	From        string `json:"from"`
	To          string `json:"to"`
	AmountCents int64  `json:"amount_cents"`
}
type Leg struct {
	Account     string `json:"account"`
	AmountCents int64  `json:"amount_cents"`
}
// Transfer debits From, waits 200ms on a durable timer, credits To, and returns a receipt id
// generated once through SideEffect.
func Transfer(ctx workflow.Context, in Input) (string, error)
var activityOptions = workflow.ActivityOptions{StartToClose: 2 * time.Second,
	Retry: workflow.RetryPolicy{Initial: 100 * time.Millisecond, Backoff: 2, Max: time.Second, NonRetryable: []string{"InvalidAmount"}}}
func Register(w *worker.Worker, l *Ledger) // RegisterWorkflow(Transfer); RegisterActivityWithName("Debit", l.Debit); ("Credit", l.Credit)
```

The receipt uses `crypto/rand` inside the `SideEffect` closure (16 bytes, hex). Activities are called by name (`"Debit"`, `"Credit"`). An amount ≤ 0 returns `activity.NewNonRetryableError("InvalidAmount", ...)`.

```go
// ledger.go — its own SQLite file, same DSN parameters as the engine store.
// CREATE TABLE IF NOT EXISTS ledger (idempotency_key TEXT PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL,
//   account TEXT NOT NULL, amount_cents INTEGER NOT NULL, applied_at INTEGER NOT NULL);
// CREATE TABLE IF NOT EXISTS executions (id INTEGER PRIMARY KEY AUTOINCREMENT, idempotency_key TEXT NOT NULL,
//   run_id TEXT NOT NULL, kind TEXT NOT NULL, attempt INTEGER NOT NULL, pid INTEGER NOT NULL, at INTEGER NOT NULL);
type Ledger struct {
	Unsafe bool          // use a random key instead of the activity's idempotency key (control mode)
	Delay  time.Duration // sleep after the write commits and before returning: the kill window
}
func OpenLedger(path string) (*Ledger, error)
func (l *Ledger) Close() error
func (l *Ledger) Debit(ctx context.Context, leg Leg) (string, error)  // applies -AmountCents, kind "debit"
func (l *Ledger) Credit(ctx context.Context, leg Leg) (string, error) // applies +AmountCents, kind "credit"
// apply: one transaction inserting an executions row and INSERT ... ON CONFLICT(idempotency_key) DO NOTHING
// into ledger; then sleep Delay (honouring ctx); return the key.
type Report struct {
	LedgerRows, Executions, ReExecutions, DoubleApplied int
}
// Check: ReExecutions = executions - count(distinct run_id||kind) over executions;
// DoubleApplied = sum over (run_id, kind) ledger groups of (count - 1) where count > 1.
func (l *Ledger) Check(ctx context.Context) (Report, error)
func (l *Ledger) Applied(ctx context.Context, runID string) (debits, credits int, err error)
```

`cmd/transfer-worker/main.go`: `func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }`. Subcommands:
  - `run --server URL --queue transfers --ledger ledger.db --identity "" --activity-delay 30ms --unsafe-ledger=false --workflow-pollers 2 --activity-pollers 4`: registers and runs until `SIGINT`/`SIGTERM`.
  - `replay --history FILE`: `worker.ReplayHistoryFile(file, transfer.Transfer)`; prints `replay ok: <n> events` or the error (exit 1).
Run `GO test -race ./examples/...` → `ok`.
- [ ] **Step 3: commit** `feat(examples): transfer workflow with idempotent ledger and replay command`.

---

### Task 19: `wf` CLI

**Files:** `cmd/wf/main.go`, `cmd/wf/main_test.go`.

- [ ] **Step 1: tests first** (`main_test.go`, calling `run(args []string, stdout, stderr io.Writer) int` against `testserver.Start`; the server flag is `--server URL`, default `$WF_SERVER` or `http://localhost:5400`):
  - `TestStartPrintsRunID`: `wf --server U start W --id c1 --queue q --input '{"n":1}'` → exit 0, stdout is a 32-hex run id and a newline.
  - `TestStartBadJSON` → exit 2, stderr mentions `--input`.
  - `TestDescribeJSON`: `describe c1` → valid JSON with `"status": "running"`.
  - `TestHistoryTable`: `history c1` → header `ID  TYPE  DETAILS` and one line per event, e.g. `1  WorkflowExecutionStarted  type=W queue=q`.
  - `TestHistoryJSON`: `history c1 --json` → parses as `[]wire.Event`.
  - `TestSignalCancel`: `signal c1 approve --payload '"yes"'` and `cancel c1 --reason test` → exit 0.
  - `TestResultWaits`: complete the run through the engine; `result c1 --timeout 5s` prints the result JSON.
  - `TestHealth`: exit 0 when up; exit 1 for an unreachable server.
  - `TestUsage`: no args → exit 2 with usage text listing all subcommands.
  Run → red.
- [ ] **Step 2: implement** with the standard `flag` package. Grammar: `wf [--server URL] <command> <positional...> [flags]`. Take the command's positional arguments first, then `fs.Parse` the rest, so flags may follow positionals (`wf start Transfer --id t-1`). Commands: `start <type> --id --queue (default transfers) --input`, `describe <id>`, `history <id> [--json]`, `signal <id> <name> [--payload]`, `cancel <id> [--reason]`, `result <id> [--timeout 30s]`, `health`. Exit codes: 0 ok, 1 server or workflow error, 2 usage error.
  Run `GO test -race ./cmd/wf` → `ok`.
- [ ] **Step 3: commit** `feat(cli): wf start, describe, history, signal, cancel, result and health`.

---

### Task 20: `wfcheck` linter

**Files:** `internal/wfcheck/analyzer.go`, `internal/wfcheck/analyzer_test.go`, `internal/wfcheck/testdata/src/github.com/sathwikbairaboina2/workflow-engine/sdk/workflow/workflow.go` (stub), `internal/wfcheck/testdata/src/a/a.go`, `cmd/wfcheck/main.go`.

- [ ] **Step 1: dependency** `GO get golang.org/x/tools@v0.51.0`.
- [ ] **Step 2: tests first.** The stub declares `package workflow; type Context interface{ Done() }`. `a.go` has workflow functions (first parameter `workflow.Context`) with one `// want` comment per finding:

```go
func Bad(ctx workflow.Context, in string) error {
	_ = time.Now()             // want `time.Now is not deterministic in workflow code; use workflow.Now`
	time.Sleep(time.Second)    // want `time.Sleep is not deterministic in workflow code; use workflow.Sleep`
	_ = rand.Intn(3)           // want `math/rand is not deterministic in workflow code; use workflow.SideEffect`
	go func() {}()             // want `go statement in workflow code; use workflow.Go`
	select {}                  // want `select statement in workflow code; use workflow.NewSelector`
}
func NotWorkflow(in string) { _ = time.Now() } // no finding
```

Also cover `time.After`, `time.Tick`, `time.NewTimer`, `time.NewTicker`, `math/rand/v2`, and a function literal inside a workflow function (still flagged). `TestAnalyzer` runs `analysistest.Run(t, analysistest.TestData(), wfcheck.Analyzer, "a")`. Run → red.
- [ ] **Step 3: implement** `Analyzer` (name `wfcheck`) with the `inspect` pass: for each `*ast.FuncDecl` whose first parameter type is a workflow context, walk its body (including nested function literals) and report the findings above. `isWorkflowContext(t types.Type) bool`: for an alias, true if the alias is named `Context` in a package whose path ends with `/sdk/workflow`; then `types.Unalias`, strip one pointer, and accept a named type `Context` whose package path ends with `/sdk/workflow` or `/sdk/internal/wfrt`. `cmd/wfcheck/main.go` is `singlechecker.Main(wfcheck.Analyzer)`.
- [ ] **Step 4: verify** `GO test -race ./internal/wfcheck` → `ok`; `GO run ./cmd/wfcheck ./examples/... ./sdk/worker/...` → no output, exit 0. Then temporarily add `_ = time.Now()` to `Transfer`, rerun → one finding with file and line and a non-zero exit (`go run` reports `exit status 3` and exits 1, verified with x/tools v0.51.0); revert.
- [ ] **Step 5: commit** `feat(wfcheck): static analyzer for nondeterministic workflow code`.

---

### Task 21: Dockerfile, compose demo, demo script

**Files:** `Dockerfile`, `deploy/docker-compose.yml`, `scripts/demo.sh`.

- [ ] **Step 1: Dockerfile.**

```dockerfile
FROM golang:1.26.8 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/wfd ./cmd/wf ./cmd/chaos ./cmd/wfbench ./examples/transfer/cmd/transfer-worker \
 && mkdir -p /out/data /out/ledger

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/wfd /out/wf /out/chaos /out/wfbench /out/transfer-worker /
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/ledger /ledger
EXPOSE 7233
USER nonroot:nonroot
CMD ["/wfd", "--listen", ":7233", "--db", "/data/wf.db"]
```

(`cmd/chaos` and `cmd/wfbench` do not exist until Tasks 22-23: until then build only the three existing binaries, and add the other two in those tasks.)
- [ ] **Step 2: compose** `deploy/docker-compose.yml`:

```yaml
# Demo: wfd on host port 5400 (ADR 0007) plus two transfer workers sharing a ledger volume.
name: workflow-engine

services:
  wfd:
    build: { context: .., dockerfile: Dockerfile }
    image: workflow-engine:dev
    command: ["/wfd", "--listen", ":7233", "--db", "/data/wf.db", "--wft-timeout", "3s"]
    ports: ["${WF_PORT:-5400}:7233"]
    volumes: ["wf-data:/data"]
    healthcheck:
      test: ["CMD", "/wf", "--server", "http://localhost:7233", "health"]
      interval: 2s
      timeout: 2s
      retries: 15

  worker:
    image: workflow-engine:dev
    command: ["/transfer-worker", "run", "--server", "http://wfd:7233", "--ledger", "/ledger/ledger.db",
              "--queue", "transfers", "--activity-delay", "500ms"]
    depends_on:
      wfd: { condition: service_healthy }
    volumes: ["ledger:/ledger"]
    deploy: { replicas: 2 }

  cli:
    image: workflow-engine:dev
    profiles: ["cli"]
    entrypoint: ["/wf", "--server", "http://wfd:7233"]

volumes:
  wf-data: {}
  ledger: {}
```

- [ ] **Step 3: demo script** `scripts/demo.sh` (Git Bash): `docker compose -f deploy/docker-compose.yml up -d --build --wait`; start 5 transfers `demo-1..5` through the `cli` service; sleep 1; `docker kill -s KILL workflow-engine-worker-1`; `docker compose ... up -d worker`; `cli result demo-$i --timeout 60s` for each (prints 5 receipts); `cli history demo-1`; print `Done. Tear down: docker compose -f deploy/docker-compose.yml down -v`.
- [ ] **Step 4: verify** `docker build -t workflow-engine:dev .` → success; `sh scripts/demo.sh` → five receipts, and the `demo-1` history ends with `WorkflowExecutionCompleted`. Then `docker compose -f deploy/docker-compose.yml down -v` and confirm `docker ps --filter name=workflow-engine` is empty.
- [ ] **Step 5: commit** `feat(deploy): Dockerfile, compose demo on port 5400 and demo script`.

---

### Task 22: Chaos harness and the measured soak

**Files:** `chaos/harness.go`, `chaos/procs.go`, `chaos/checks.go`, `chaos/report.go`, `chaos/report_test.go`, `chaos/soak_test.go`, `cmd/chaos/main.go`, `scripts/chaos.sh`, `bench/results/chaos-latest.json`, `bench/results/chaos-control.json`.

- [ ] **Step 1: config and report.**

```go
type Config struct {
	BinDir, WorkDir, Out string
	Seed                 int64
	Workflows, Kills     int           // defaults 100, 50
	Workers              int           // default 3
	KillEvery            time.Duration // mean gap, default 300ms; each gap uniform in [0.5x, 1.5x]
	ServerKillShare      float64       // default 0.25
	RestartDelay         time.Duration // default 200ms
	ActivityDelay        time.Duration // default 30ms, passed to workers
	DrainTimeout         time.Duration // default 3m
	UnsafeLedger         bool
	Port                 int           // default 7233, inside the container only
}
type Report struct {
	Seed                  int64    `json:"seed"`
	UnsafeLedger          bool     `json:"unsafe_ledger"`
	Workflows             int      `json:"workflows"`
	Completed             int      `json:"completed"`
	Failed                int      `json:"failed"`
	Lost                  int      `json:"lost"`
	KillsTotal            int      `json:"kills_total"`
	KillsServer           int      `json:"kills_server"`
	KillsWorker           int      `json:"kills_worker"`
	ActivityExecutions    int      `json:"activity_executions"`
	ActivityReExecutions  int      `json:"activity_re_executions"`
	LedgerRows            int      `json:"ledger_rows"`
	DoubleApplied         int      `json:"double_applied"`
	MissingApplies        int      `json:"missing_applies"`
	ReplayFailures        int      `json:"replay_failures"`
	ConsistencyViolations int      `json:"consistency_violations"`
	Violations            []string `json:"violations,omitempty"` // first 20
	ServerRecoveryMSP50   float64  `json:"server_recovery_ms_p50"`
	ServerRecoveryMSMax   float64  `json:"server_recovery_ms_max"`
	DurationS             float64  `json:"duration_s"`
	GoVersion             string   `json:"go_version"`
	NumCPU                int      `json:"num_cpu"`
	StartedAt             string   `json:"started_at"`
	Pass                  bool     `json:"pass"`
}
// Pass = Lost==0 && Failed==0 && ReplayFailures==0 && ConsistencyViolations==0 && MissingApplies==0 && (UnsafeLedger || DoubleApplied==0)
func (r Report) Summary() string // the human block below
```

`report_test.go`: `TestPassRule` (table) and `TestSummaryFormat` (golden string). Summary format:

```
workflow-engine chaos report (seed 42)
  workflows started        500
  kill -9 injected         200  (server 52, workers 148)
  lost workflows           0
  double-applied effects   0
  activity re-executions   37  (absorbed by idempotency keys)
  replay failures          0
  consistency violations   0
  server recovery p50      41 ms  (max 88 ms)
  result                   PASS
```

- [ ] **Step 2: harness** (`Run(ctx, cfg) (Report, error)`):
  1. `os.RemoveAll(WorkDir)` and recreate it. `procs.go`: start `wfd --listen 127.0.0.1:<port> --db <work>/wf.db --wft-timeout 2s --poll-timeout 2s --timer-interval 50ms --reaper-interval 100ms` and `Workers` × `transfer-worker run --server http://127.0.0.1:<port> --ledger <work>/ledger.db --queue transfers --activity-delay <d> [--unsafe-ledger] --identity w<i>-g<gen>`. Each process has its own stdout/stderr log file in the work dir. `kill(p)` = `cmd.Process.Kill()` (SIGKILL) then `cmd.Wait()`; a kill counts only if the process was alive. Server restart: measure from spawn until `GET /healthz` returns 200 (poll every 10 ms) → one recovery sample.
  2. Starter goroutine: workflow ids `chaos-<seed>-<i>`; inputs from `rand.New(rand.NewPCG(uint64(seed), 1))` (accounts `acct-0..9`, amounts 1-10000); starts spaced by `Kills*KillEvery/Workflows`; each start retries through server downtime (client `WithRetry(2m)`); `IsAlreadyStarted` counts as success.
  3. Killer goroutine: `rand.New(rand.NewPCG(uint64(seed), 2))`; for `Kills` rounds: sleep a gap; pick the server with probability `ServerKillShare`, else a random worker; kill; sleep `RestartDelay`; restart.
  4. Drain: poll `Describe` for unfinished ids every 200 ms until all are closed or `DrainTimeout`.
  5. Replay check: for every workflow, fetch history and run `worker.ReplayHistory(h, transfer.Transfer)` → count failures (record messages in `Violations`).
  6. Stop workers and server with `SIGTERM`, wait. Open `wf.db` with `store.Open` → `CheckConsistency`. Open the ledger → `Check()`; for each completed run `Applied` must be `(≥1, ≥1)`, else `MissingApplies++`.
  7. Fill the report, write JSON (indent 2) to `Out`, print `Summary()`. `cmd/chaos/main.go` maps flags (`--bin-dir --work-dir --out --seed --workflows --kills --workers --kill-every --server-kill-share --restart-delay --activity-delay --drain-timeout --unsafe-ledger`) to `Config`; exit 1 if `!Pass && !UnsafeLedger`.
- [ ] **Step 3: soak test** `chaos/soak_test.go` `TestKill9Soak`: skips unless `WF_CHAOS=1`; builds `wfd` and `transfer-worker` into `t.TempDir()` with `exec.Command("go", "build", "-o", dir+"/", "./cmd/wfd", "./examples/transfer/cmd/transfer-worker")` (working dir = module root); runs `Run` with 30 workflows and 15 kills; asserts `Pass`. Run `GOSH 'WF_CHAOS=1 go test -race -run TestKill9Soak -v ./chaos 2>&1 | tail -n 20'` → `--- PASS`. Without `WF_CHAOS`, `GO test ./chaos` → `ok` (skipped).
- [ ] **Step 4: script** `scripts/chaos.sh`:

```sh
#!/usr/bin/env sh
# Builds the binaries and runs the chaos harness inside golang:1.26.8. Flags pass through to cmd/chaos.
set -e
cd "$(dirname "$0")/.."
root="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm --name "workflow-engine-chaos-$$" -v "$root:/src" \
  -v workflow-engine-gomod:/go/pkg/mod -v workflow-engine-gobuild:/root/.cache/go-build -w /src golang:1.26.8 \
  sh -c 'go build -o /tmp/bin/ ./cmd/wfd ./examples/transfer/cmd/transfer-worker && go run ./cmd/chaos --bin-dir /tmp/bin --work-dir /tmp/chaos "$@"' chaos "$@"
```

- [ ] **Step 5: measured runs** (record real output in the ledger):
  - `sh scripts/chaos.sh --workflows 500 --kills 200 --seed 42 --out bench/results/chaos-latest.json` → expect `result PASS`. If it fails, debug with superpowers:systematic-debugging (the logs are in `/tmp/chaos` inside the container; rerun with `--work-dir /src/chaos-out` to keep them; `chaos-out/` is git-ignored). Never edit the JSON by hand.
  - `sh scripts/chaos.sh --workflows 500 --kills 200 --seed 42 --unsafe-ledger --out bench/results/chaos-control.json` → record `double_applied` whatever it is. If it is 0, raise `--activity-delay 100ms` for **both** runs (rerun the safe one too) so the comparison stays like-for-like, and note it as a `Ruling:`.
  - Add `./cmd/chaos` to the Dockerfile build line.
- [ ] **Step 6: commit** `feat(chaos): kill -9 harness with invariant checks and measured soak`.

---

### Task 23: Benchmark

**Files:** `internal/bench/bench.go`, `internal/bench/bench_test.go`, `cmd/wfbench/main.go`, `scripts/bench.sh`, `bench/results/bench-latest.json`.

- [ ] **Step 1: test first** `TestBenchSmall`: `bench.Run(ctx, Config{Workflows: 20, Concurrency: 4, WorkflowPollers: 2, ActivityPollers: 4, DBPath: t.TempDir()+"/wf.db"})` → `Completed == 20`, `Transitions >= 20*6`, `TransitionsPerSec > 0`, `TransitionP99MS >= TransitionP50MS`, `WorkflowP99MS >= WorkflowP50MS`. Run → red.
- [ ] **Step 2: implement.** In-process: `store.Open(DBPath)` (real file, `synchronous=FULL`), engine with `WithObserver` collecting `(kind, duration)` samples under a mutex, `httptest.NewServer` (real TCP on 127.0.0.1), `RunLoops`, and one SDK worker with workflow `Bench(ctx, n int) (int, error)` that runs one activity `Noop(ctx, n int) (int, error)` returning `n+1`. Start `Workflows` runs with `Concurrency` starters; poll `store CountRuns("running") == 0` every 20 ms. Wall time = first start to last close. Result:

```go
type Result struct {
	Workflows, Completed  int
	WallS                 float64 `json:"wall_s"`
	WorkflowsPerSec       float64 `json:"workflows_per_s"`
	Transitions           int     `json:"transitions"`
	TransitionsPerSec     float64 `json:"transitions_per_s"`
	TransitionP50MS       float64 `json:"transition_p50_ms"`
	TransitionP99MS       float64 `json:"transition_p99_ms"`
	WorkflowP50MS         float64 `json:"workflow_e2e_p50_ms"`  // from workflows.created_at/closed_at
	WorkflowP99MS         float64 `json:"workflow_e2e_p99_ms"`
	GoVersion, GOOS, GOARCH string
	NumCPU                int
	Store                 string  `json:"store"` // "sqlite WAL synchronous=FULL, container filesystem"
	Note                  string  `json:"note"`  // "Docker Desktop on Windows 11; server, workers and load generator in one container"
	StartedAt             string  `json:"started_at"`
}
```

Percentiles: nearest-rank on sorted samples. `cmd/wfbench` flags: `--workflows 2000 --concurrency 64 --workflow-pollers 4 --activity-pollers 16 --db /tmp/wfbench/wf.db --out bench/results/bench-latest.json`; prints a short table. Add `./cmd/wfbench` to the Dockerfile build line.
- [ ] **Step 3: script and measured run.** `scripts/bench.sh` mirrors `chaos.sh` (container name `workflow-engine-bench-$$`, runs `go run ./cmd/wfbench "$@"`). Run `sh scripts/bench.sh --out bench/results/bench-latest.json` twice; keep the second run's file and record both runs' `transitions_per_s` and `transition_p99_ms` in the ledger.
- [ ] **Step 4: commit** `feat(bench): load generator with measured transitions/s and p99`.

---

### Task 24: CI, README with enforced headline, semantics page

**Files:** `.github/workflows/ci.yml`, `readme_test.go`, `README.md`, `docs/semantics.md`, `scripts/gates.sh`.

- [ ] **Step 1: headline test first** (`readme_test.go`, package `workflowengine_test` at the repo root):

```go
// TestREADMEHeadline keeps the README headline tied to measured results.
func TestREADMEHeadline(t *testing.T) {
	var safe, control struct {
		Workflows            int `json:"workflows"`
		KillsTotal           int `json:"kills_total"`
		KillsServer          int `json:"kills_server"`
		Lost                 int `json:"lost"`
		DoubleApplied        int `json:"double_applied"`
		ActivityReExecutions int `json:"activity_re_executions"`
	}
	var bench struct {
		TransitionsPerSec float64 `json:"transitions_per_s"`
		TransitionP99MS   float64 `json:"transition_p99_ms"`
	}
	mustJSON(t, "bench/results/chaos-latest.json", &safe)
	mustJSON(t, "bench/results/chaos-control.json", &control)
	mustJSON(t, "bench/results/bench-latest.json", &bench)
	want := fmt.Sprintf("**%d lost workflows and %d double-applied side effects across %d `kill -9`s (%d of the server) over %d workflows; %d activity re-executions were absorbed by idempotency keys. With the keys disabled, the same seed double-applied %d. %.0f transitions/s on SQLite (WAL, synchronous=FULL), p99 %.1f ms per transition.**",
		safe.Lost, safe.DoubleApplied, safe.KillsTotal, safe.KillsServer, safe.Workflows, safe.ActivityReExecutions,
		control.DoubleApplied, bench.TransitionsPerSec, bench.TransitionP99MS)
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), want) {
		t.Fatalf("README headline is not the measured one; put this line in README.md:\n%s", want)
	}
}
```

Run `GO test -run TestREADMEHeadline .` → red (no README yet). The failure message prints the exact line to paste.
- [ ] **Step 2: README.md** in this order: title and one-line pitch; the headline line from the test; the real `Summary()` block from the `chaos-latest.json` run (copy the program's output, do not retype numbers); "Try it in 30 seconds" (`sh scripts/demo.sh`, and `sh scripts/chaos.sh --workflows 100 --kills 50 --seed 1`); "Install" (state that `go get`/`go install` work once the repo is pushed to GitHub: `go get github.com/sathwikbairaboina2/workflow-engine/sdk/...`, `go install .../cmd/wf@latest`, `go install .../cmd/wfd@latest`, `docker build -t workflow-engine:dev .`); the SDK example (the real `Transfer` code); a mermaid architecture diagram; "Guarantees" (five bullets, linking `docs/semantics.md`); a benchmark table from `bench-latest.json` with its `note`; layout table; ADR links; "Limits" (single node, full-history replay, at-least-once activities, no heartbeats).
- [ ] **Step 3: docs/semantics.md**: what is guaranteed (spec Semantics 1-11 in plain sentences) and what is not (exactly-once activity execution, cross-node, wall-clock precision of timers beyond the timer interval, changed activity inputs going undetected by replay).
- [ ] **Step 4: CI** `.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:
  schedule:
    - cron: "17 3 * * *"
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.26"
      - name: gofmt
        run: test -z "$(gofmt -l .)" || (gofmt -l . && exit 1)
      - run: go vet ./...
      - name: wfcheck
        run: go run ./cmd/wfcheck ./examples/... ./sdk/worker/...
      - run: go test -race ./...
      - name: chaos soak (short)
        if: github.event_name != 'schedule'
        run: |
          go build -o /tmp/bin/ ./cmd/wfd ./examples/transfer/cmd/transfer-worker
          go run ./cmd/chaos --bin-dir /tmp/bin --work-dir /tmp/chaos --workflows 100 --kills 50 --seed ${{ github.run_number }} --out chaos.json
      - name: chaos soak (nightly)
        if: github.event_name == 'schedule'
        run: |
          go build -o /tmp/bin/ ./cmd/wfd ./examples/transfer/cmd/transfer-worker
          go run ./cmd/chaos --bin-dir /tmp/bin --work-dir /tmp/chaos --workflows 2000 --kills 600 --seed ${{ github.run_number }} --out chaos.json
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: chaos-report
          path: |
            chaos.json
            /tmp/chaos/*.log
          if-no-files-found: ignore
      - run: docker build -t workflow-engine:ci .
```

- [ ] **Step 5: gates script** `scripts/gates.sh` runs, in order and stopping at the first failure: gofmt check, `go vet ./...`, `go run ./cmd/wfcheck ./examples/... ./sdk/worker/...`, `go test -race -count=1 ./...` (write the log to `/tmp/test.log`, print its last 40 lines, exit with the test's exit code — `sh` has no `pipefail`) inside one `gosh.sh` call; then `sh scripts/chaos.sh --workflows 100 --kills 50 --seed 7 --out /tmp/chaos-gate.json` (the report stays inside the container); then `docker build -t workflow-engine:dev .`. Prints `GATES PASS` at the end.
- [ ] **Step 6: verify** `GO test -run TestREADMEHeadline .` → `ok`; `sh scripts/gates.sh` → `GATES PASS`.
- [ ] **Step 7: commit** `docs: README with measured headline, semantics page, CI and gates script`.

---

### Task 25: Final gates and handoff

**Files:** `docs/handoff.md`, the ledger.

- [ ] **Step 1: gates.** Run each and record the real result in the ledger:
  1. `sh scripts/gates.sh` → ends with `GATES PASS` (gofmt empty, vet clean, wfcheck clean, `go test -race -count=1 ./...` all `ok`, short chaos `result PASS`, docker build ok).
  2. `GOSH 'WF_CHAOS=1 go test -race -run TestKill9Soak ./chaos'` → `ok`.
  3. `GO test -run TestREADMEHeadline .` → `ok`.
  4. `sh scripts/demo.sh` → five receipts; then `docker compose -f deploy/docker-compose.yml down -v`.
  5. `git ls-files | grep -E '\.env|\.db$|\.db-wal$'` → no output. `git status --short` → clean after the last commit.
  6. `docker ps -a --filter name=workflow-engine --format '{{.Names}}'` → nothing of ours still running.
- [ ] **Step 2: handoff.** Append to `docs/handoff.md`: `## 2026-10-04 · Claude (Sonnet builder) · main`, what changed (one line per area), measured numbers (copied from the JSON files), what is left (v0.2 list from the spec), how to verify (the gate commands above), and any `Ruling:` lines from the ledger.
- [ ] **Step 3: commit** `docs: handoff for v0.1`.
- [ ] **Step 4:** write `FINAL: <gate summary>` to the ledger and report DONE with the commit range.
