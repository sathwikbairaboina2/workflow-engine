# ADR 0004: Buffer every event that arrives while a workflow task is in flight

Date: 2026-10-04 · Status: accepted

## Context

The design buffered only signals. But activity results, timer firings and cancel requests can also
arrive between `WorkflowTaskStarted` and `WorkflowTaskCompleted`. If they were appended there, replay
could not tell which step had seen them.

## Decision

A generic `buffered_events` table replaces `signals_buffer`. While a run's workflow task holds a lease,
every non-task event for that run is buffered. When the task completes, fails or times out, the core
appends the task's own event, then the command events, then the buffered events in arrival order, then
a new `WorkflowTaskScheduled` if needed. So the events right after any `WorkflowTaskStarted` are exactly
`WorkflowTaskCompleted` (or `WorkflowTaskFailed`) followed by that step's command events. Replay
batching relies on this.

A close command at a completion that has buffered events is dropped, and the workflow gets a new task
(Temporal does the same). Replay accepts one trailing close command with no matching event.

## What I gave up

- **Latency for signals and results** that land mid-task: they wait for the task to finish.
- **A simpler consistency check**: buffered `TimerFired` and activity results must count as history
  when the checker compares timer and task rows.
