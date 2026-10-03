# ADR 0005: Goroutine coroutines with a single-runner dispatcher

Date: 2026-10-04 · Status: accepted

## Decision

Each workflow coroutine (the root function and each `workflow.Go`) is a goroutine, but a dispatcher
resumes exactly one at a time over unbuffered channels. A coroutine yields only inside an SDK blocking
call (`Future.Get`, `Sleep`, `Receive`, `Selector.Select`). The dispatcher loops over coroutines in
creation order until one full pass makes no progress. Commands are numbered by one per-run `seq`
counter in call order. At the end of a task, every parked coroutine is resumed with an exit flag and
unwinds through a sentinel panic, so no goroutine leaks. A prototype produced the same interleaving in
200 of 200 runs under `-race`.

Replay matches commands to history on command type, `seq`, and `activity_type` for activities. Timer
durations and activity inputs are not compared, so changing a timeout or an argument is not flagged.

## What I gave up

- **Real parallelism inside workflow code**, by design.
- **Runtime protection from user goroutines.** A `go` statement or `select` in workflow code breaks
  determinism without an error. `wfcheck` catches it statically; nothing catches it at runtime.
- **Input-change detection.** Replay does not notice changed activity arguments (Temporal does not either).
