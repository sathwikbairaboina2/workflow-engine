# workflow-engine v0.1 spec (2026-10-04)

Status: approved by brief (autopilot, no Q&A). Source design: `taskarinchu/docs/devdocs/workflow-engine.md`.
Decisions that differ from or sharpen that design are recorded in `docs/adr/0001`–`0008`.

## One line

A small Temporal in Go: workflows written as ordinary Go code survive `kill -9` of any process,
because an event-sourced SQLite history plus deterministic replay is the only source of truth.

## Thesis

"The code proposes, the deterministic core disposes." Workflow code only emits *commands*. The server
core validates them against history and appends events in one SQLite transaction per transition.
Nothing outside the history decides state.

## Portfolio bar (what v0.1 must show)

| Bar | How v0.1 meets it |
|---|---|
| 30-second wow | `sh scripts/chaos.sh` prints one report: N workflows, K `kill -9`s of server and workers, 0 lost, 0 double-applied side effects, R activity re-executions absorbed by idempotency keys. The control run (`--unsafe-ledger`, same seed) shows the duplicates that idempotency keys prevent. |
| Measured headline number | README quotes `bench/results/chaos-latest.json`, `chaos-control.json` and `bench-latest.json` produced by real runs. No number is typed by hand. |
| Installable | `go get github.com/sathwikbairaboina2/workflow-engine/sdk/...`, `go install .../cmd/wf` and `.../cmd/wfd`, Docker image `workflow-engine:dev`, compose demo on host port 5400. |
| Honest ADRs | Eight ADRs, each with a "What I gave up" section. `docs/semantics.md` says exactly what is and is not guaranteed (activities are at-least-once). |
| CI with tests | GitHub Actions: gofmt, vet, `wfcheck` on examples, `go test -race ./...`, a short chaos soak, docker build; a nightly long soak. |

## Scope (v0.1)

In:
- **Server `wfd`**: one process, HTTP/JSON API (all `POST` under `/v1/`, plus `GET /healthz`, `GET /metrics`),
  SQLite in WAL mode with `synchronous=FULL`, every transition in one `BEGIN IMMEDIATE` transaction.
- **History**: append-only (SQLite triggers abort `UPDATE`/`DELETE` on `history_events`), dense
  `event_id` 1..n per run, optimistic concurrency on `workflows.next_event_id`.
- **Task queues with leases**: workflow and activity tasks; long-poll (default 20 s, `204` on timeout);
  random lease tokens; stale or foreign tokens get `409 stale_lease`.
- **Lease reaper**: expired workflow-task leases append `WorkflowTaskFailed{cause:"timeout"}` and reschedule;
  expired activity leases count as a failed attempt.
- **Retries**: activity retry policy (`initial_ms`, `backoff`, `max_ms`, `max_attempts`, `non_retryable`) applied
  server-side without history events; only the final outcome is recorded (Temporal's choice, ADR 0003).
- **Durable timers**: `timers` table; firing deletes the row and appends `TimerFired` in one transaction, so a
  timer fires exactly once and never before `fire_at`.
- **Signals and cancellation**: buffered while a workflow task is in flight (ADR 0004). Cancel closes the run
  as `canceled` once the workflow code returns `workflow.ErrCanceled`; closing a run deletes its pending
  activity tasks and timers.
- **Go SDK**: `sdk/client`, `sdk/worker`, `sdk/workflow` (`ExecuteActivity`, `Sleep`, `NewTimer`, `Now`,
  `SideEffect`, `GetSignalChannel`, `Go`, `NewSelector`, `ErrCanceled`), `sdk/activity` (`GetInfo` with
  `IdempotencyKey`, `NewError`, `NewNonRetryableError`).
- **Deterministic replay** of full history on every workflow task, with `NondeterminismError` on mismatch
  (ADR 0002, ADR 0005). `worker.ReplayHistory` replays a saved history offline.
- **`wf` CLI**: `start`, `describe`, `history` (table or `--json`), `signal`, `cancel`, `result`, `health`.
  Offline replay is a subcommand of the example worker binary (`transfer-worker replay --history h.json`),
  because Go cannot load workflow code into `wf` at runtime (ADR 0008).
- **`wfcheck` linter** (`go/analysis`): flags `time.Now/Sleep/After/Tick/NewTimer/NewTicker`, any
  `math/rand` or `math/rand/v2` call, `go` statements and `select` statements inside functions whose first
  parameter is `workflow.Context`.
- **Transfer example** with an idempotent SQLite ledger keyed by `run_id:scheduled_event_id`, an `executions`
  table that records every attempt, and an `--unsafe-ledger` control mode that uses a random key.
- **Chaos harness** (`cmd/chaos`): spawns `wfd` and worker processes, `SIGKILL`s them on a seeded schedule,
  restarts them, drains, checks invariants, writes a JSON report (ADR 0006).
- **Benchmark** (`cmd/wfbench`): in-process server plus SDK workers over real HTTP and a real SQLite file;
  reports transitions/s, per-transition commit p50/p99 and workflow end-to-end p50/p99.
- **Failpoints** in `store.WithTx` and a consistency checker `store.CheckConsistency`.
- **Limits**: payloads over 2 MiB rejected (`413 payload_too_large` on start; activity results over the limit
  become a non-retryable `PayloadTooLarge` failure); a run reaching 50,000 events is closed as `failed`
  with `HistoryLimitExceeded`.
- Dockerfile (multi-stage, distroless static nonroot), `deploy/docker-compose.yml`, and scripts (no Makefile: the host has no make) that
  run Go only in Docker (`golang:1.26.8`), CI workflow, README, `docs/semantics.md`, `docs/DEVDOCS.md`.

Out (v0.2 or later): heartbeats, sticky cache with incremental history, child workflows, continue-as-new,
`GetVersion`, Postgres store, schedule-to-close timeouts, workflow execution timeouts, query handlers,
search attributes, web UI, multi-node server, Temporal wire compatibility, Prometheus/Grafana in compose.

## Semantics (pinned; the plan's tests check these)

1. **Workflow task lifecycle.** A run has at most one workflow task (scheduled or started). A task is
   *in flight* when its `tasks` row has a non-null `lease_token`.
2. **Buffering rule.** While a workflow task is in flight, no transition appends events to that run except
   the completion, failure or lease expiry of that task. Every other event (activity resolution, timer fired,
   signal, cancel request) goes to `buffered_events`. When the in-flight task resolves, the core appends, in
   order: `WorkflowTaskCompleted` (or `WorkflowTaskFailed`), then the command events, then the buffered
   events in arrival order, then `WorkflowTaskScheduled` if the run is still open and either buffered events
   existed or the task failed.
3. **New work when idle.** If no workflow task exists, an event that needs one is appended followed by
   `WorkflowTaskScheduled` and a new task row. If a task is scheduled but not started, the event is
   appended and no second task is created; the next poll sees it in the full history.
4. **Close commands.** `CompleteWorkflow`, `FailWorkflow`, `CancelWorkflow` close the run. A close command is
   dropped (not an error) when buffered events exist at completion; the workflow gets a new task and
   replay accepts a trailing close command with no matching event.
5. **Workflow task failure.** `nondeterminism`, `panic`, `unknown_workflow_type` and `timeout` append
   `WorkflowTaskFailed{cause,message}`, then buffered events, then `WorkflowTaskScheduled{attempt+1}` with
   the task visible after `min(1s * 2^(attempt-1), 30s)` (timeouts reschedule immediately). No command
   events are appended.
6. **Replay batching.** The SDK runs workflow code at a `WorkflowTaskStarted` event only if it is the last
   event or the next event is `WorkflowTaskCompleted`. Commands produced at a completed step must equal, in
   order, the contiguous command events after that `WorkflowTaskCompleted` (`ActivityTaskScheduled`,
   `TimerStarted`, `MarkerRecorded`, `WorkflowExecutionCompleted/Failed/Canceled`). Match on command type,
   `seq`, and `activity_type` for activities.
7. **Sequence numbers.** One per-run counter `seq` numbers every command that has an id (activity, timer,
   marker), starting at 1, in the order workflow code calls the API.
8. **`workflow.Now`** returns the timestamp (ms) of the current step's `WorkflowTaskStarted` event.
9. **`SideEffect`** always emits `RecordMarker{seq}`. It calls the function only when history has no marker
   for that `seq`; otherwise it returns the recorded value.
10. **Activities are at-least-once.** The idempotency key `run_id + ":" + scheduled_event_id` is stable across
    retries and exposed as `activity.GetInfo(ctx).IdempotencyKey`. Effectively-once side effects need the
    side-effect store to honour that key (the ledger does).
11. **Cancellation.** After `WorkflowExecutionCancelRequested` is applied in replay, every blocking SDK call that
    is not already resolved returns `workflow.ErrCanceled`. A workflow returning `ErrCanceled` emits
    `CancelWorkflow`.

## Invariants and proving tests

| # | Invariant | Test (package) |
|---|---|---|
| 1 | History is append-only and dense per run. | `TestHistoryTriggerBlocksMutation` (store), `TestHistoryDenseAppendOnly` (core, rapid state machine) |
| 2 | Concurrent completions of one task: exactly one applies. | `TestAppendEventsConflict` (store), `TestOptimisticConcurrencyConflict` (core) |
| 3 | Stale or foreign lease tokens are rejected and change nothing. | `TestStaleLeaseRejected`, `TestZombieActivityCompletionRejected` (core) |
| 4 | Divergent workflow code fails the task with `nondeterminism`; no command events appended. | `TestReplayNondeterminism` (wfrt), `TestNondeterminismDetected` (worker, end to end), `TestReplayGoldenHistories` (worker) |
| 5 | Workflow code reads time and randomness only through recorded APIs. | `TestNowIsDeterministic`, `TestSideEffectRecordedValueWins` (wfrt); `TestAnalyzer` (wfcheck) |
| 6 | Attempts strictly increase, never exceed `max_attempts`; non-retryable stops at once; backoff matches the closed form. | `TestRetryPolicyBounds` (retry, rapid) |
| 7 | A timer fires exactly once, never early. | `TestTimerFiresOnce`, `TestTimerNotEarly` (core) |
| 8 | At most one open run per `workflow_id`. | `TestSingleOpenRunPerWorkflowID` (core, 50 goroutines) |
| 9 | No lost workflows and no double-applied side effects under random `kill -9`. | `TestKill9Soak` (chaos, `WF_CHAOS=1`), `cmd/chaos` report |
| 10 | Every transition is atomic; after a crash at a failpoint, history, tasks and timers agree. | `TestCrashPointConsistency` (core), checker also runs after each soak |
| 11 | Payload and history limits are enforced. | `TestPayloadLimit`, `TestHistoryLengthLimit` (core) |

## Interfaces (pinned names)

- Module `github.com/sathwikbairaboina2/workflow-engine`, `go 1.26.0`.
- Public wire types: package `wire` (events, attrs, commands, payload envelope, API request/response types).
- Server packages: `internal/clock`, `internal/store`, `internal/retry`, `internal/core`, `internal/metrics`,
  `internal/api`, `internal/wfcheck`. Binaries: `cmd/wfd`, `cmd/wf`, `cmd/wfcheck`, `cmd/chaos`, `cmd/wfbench`.
- SDK: `sdk/client`, `sdk/worker`, `sdk/workflow`, `sdk/activity`, runtime in `sdk/internal/wfrt`.
- Example: `examples/transfer` and binary `examples/transfer/cmd/transfer-worker`.
- Error body: `{"error":{"code":string,"message":string,"run_id"?:string}}`. Codes and statuses:
  `invalid_request` 400, `not_found` 404, `workflow_already_started` 409 (with `run_id`), `stale_lease` 409,
  `run_closed` 409, `conflict` 409, `payload_too_large` 413, `internal` 500.

## Environment rules

- Go runs only in Docker (`golang:1.26.8`) via `scripts/go.ps1` / `scripts/go.sh`; named volumes
  `workflow-engine-gomod` and `workflow-engine-gobuild`; containers named `workflow-engine-*`.
- Host ports 5400-5409 only. Compose publishes `wfd` on 5400. Nothing else is published.
- No AWS, no LocalStack, no network in tests (module download excepted).
- Verified dependency pins (2026-10-04, prototyped in `golang:1.26.8`): `modernc.org/sqlite v1.60.1`
  (SQLite 3.53.4; `_pragma=` DSN params and `_txlock=immediate` work; 200 concurrent read-modify-write
  transactions lost no updates), `pgregory.net/rapid v1.3.0` (`t.Repeat` state machines),
  `golang.org/x/tools v0.51.0` (`analysistest` with GOPATH-style `testdata/src`),
  `github.com/prometheus/client_golang v1.24.1`.

## Metrics published in README (measured only)

From `bench/results/chaos-latest.json`: workflows, kills (server/worker), lost, double-applied,
re-executions, replay failures, consistency violations, server recovery p50.
From `bench/results/chaos-control.json`: double-applied with idempotency keys disabled.
From `bench/results/bench-latest.json`: transitions/s, transition commit p99, workflow end-to-end p99,
with the machine description (Docker Desktop on Windows 11, container CPU count).
