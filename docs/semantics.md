# Semantics: what workflow-engine guarantees and what it does not

This page is normative for v0.1. The tests named in the spec check every guarantee below, and the
"Not guaranteed" section is as important as the first one.

## Guaranteed

1. **One workflow task per run.** A run has at most one workflow task, scheduled or started. A task is in
   flight while a worker holds its lease.
2. **Nothing appends behind a running task.** While a workflow task is in flight, only that task's completion,
   failure or lease expiry appends events to the run. Activity results, fired timers, signals and cancel
   requests wait in a buffer. When the task resolves, the server appends, in order: `WorkflowTaskCompleted`,
   the command events, the buffered events in arrival order, then a new `WorkflowTaskScheduled` if the run is
   still open and either events were buffered or the task failed.
3. **New work wakes an idle run.** If the run has no workflow task, the event is appended followed by
   `WorkflowTaskScheduled` and a task row. If a task is scheduled but not yet started, the event is appended and
   no second task is created: the next poll sees it in the full history.
4. **Close commands can be dropped.** `CompleteWorkflow`, `FailWorkflow` and `CancelWorkflow` close the run. If
   events were buffered when the workflow asked to close, the server drops the close (it is not an error) and
   schedules a new task, because the workflow has not seen those events yet. Replay accepts that a close command
   has no matching event, and the next step emits it again.
5. **Failed workflow tasks append no commands.** `nondeterminism`, `panic`, `unknown_workflow_type`,
   `invalid_commands` and `timeout` append `WorkflowTaskFailed`, then buffered events, then
   `WorkflowTaskScheduled`. The retry becomes visible after `min(1s * 2^(attempt-1), 30s)`; a lease timeout
   reschedules at once.
6. **Replay runs code only at completed steps.** The SDK runs workflow code at a `WorkflowTaskStarted` that is
   the last event or is followed by `WorkflowTaskCompleted`. A started task that failed or timed out never ran to
   completion and is skipped. The commands a step produces must equal, in order, the command events that follow
   its `WorkflowTaskCompleted`. A mismatch is a nondeterminism error and the server appends nothing.
7. **Sequence numbers.** One counter per run numbers every command that has an id (activity, timer, marker),
   starting at 1, in the order workflow code calls the API.
8. **`workflow.Now` is deterministic.** It returns the timestamp of the current step's `WorkflowTaskStarted`.
9. **`SideEffect` runs once.** It always emits a marker. It calls your function only when history has no marker
   for its sequence number; otherwise it returns the recorded value.
10. **Cancellation.** After a cancel request is applied in replay, every blocking SDK call that is not already
    resolved returns `workflow.ErrCanceled`. A workflow that returns it closes as `canceled`. Closing a run deletes
    its pending activity tasks and timers.
11. **History is append-only and dense.** SQLite triggers abort `UPDATE` and `DELETE` on history, event ids are
    `1..n` per run, and a compare-and-set on `next_event_id` makes concurrent completions of one task apply
    exactly once.
12. **Leases.** Stale or foreign lease tokens get `409 stale_lease` and change nothing. A completion that arrives
    after its lease expired is rejected.
13. **Timers.** A timer fires exactly once and never before its `fire_at`. Firing deletes the timer row and appends
    `TimerFired` in one transaction.
14. **Atomic transitions.** Every state change is one `BEGIN IMMEDIATE` transaction. After a crash at any commit
    boundary, history, tasks, timers and buffers agree (`store.CheckConsistency`).
15. **Limits.** Payloads over 2 MiB are rejected on start and on signal; an activity result over the limit becomes
    a non-retryable `PayloadTooLarge` failure. A run that reaches 50,000 events is closed as `failed` with
    `HistoryLimitExceeded`.

## Activities are at-least-once

The server retries an activity after a failure and after a lease expiry (for example when its worker is killed).
An activity can therefore run more than once, including after its side effect has been committed. The idempotency
key `run_id:scheduled_event_id` is the same on every attempt and is available as
`activity.GetInfo(ctx).IdempotencyKey`. A side effect that honours the key (the transfer example's ledger does, with
a unique key) is applied once. The chaos control run, which ignores the key, shows what happens otherwise.

Retries are not recorded in history. Only the final outcome (`ActivityTaskCompleted`, `ActivityTaskFailed` or
`ActivityTaskTimedOut`) is, with the attempt count.

## Not guaranteed

- **Exactly-once activity execution.** See above: at-least-once, with idempotency keys for effectively-once effects.
- **Anything across nodes.** There is one `wfd` process and one SQLite file. There is no replication and no failover.
- **Timer precision beyond the timer loop.** A timer is never early, but it fires on the next pass of the timer loop
  (50 ms in the chaos runs, 100 ms by default) and then waits for a worker to take the next task.
- **Detection of changed activity inputs.** Replay compares command type, sequence number and activity name. If you
  change the input a workflow passes to an activity, replay does not notice.
- **Heartbeats, versioning and continue-as-new.** Not in v0.1. Long-running activities need a generous
  `StartToClose`; changing workflow code while runs are open needs a new workflow name or a drained queue.
- **Fairness and ordering across queues.** Tasks of one queue are leased oldest-visible first; there is no priority.
