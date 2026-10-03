# ADR 0008: Fewer server packages, a public wire package, replay in the worker binary

Date: 2026-10-04 · Status: accepted

## Decision

- The design's `internal/queue`, `internal/timer` and the reaper half of `internal/retry` fold into
  `internal/core`. They share one transaction helper and one buffering rule, so splitting them would
  mean passing the transaction across packages. `internal/retry` keeps only the pure policy maths.
- Event, command, payload and API types live in a public package `wire`, because both the server and the
  public SDK need them.
- The SDK runtime (dispatcher and replayer) lives in `sdk/internal/wfrt`. `sdk/workflow` is a thin facade
  whose `Context` is an alias of the runtime type.
- `wf replay` is not built. Go cannot load workflow code into a prebuilt binary, so offline replay is
  `worker.ReplayHistory` in the SDK plus a `replay` subcommand in each worker binary
  (`transfer-worker replay --history h.json`).
- Workflows and activities take exactly one input argument after the context.

## What I gave up

- **A generic `wf replay`.** Each worker binary must expose its own replay subcommand (a few lines with the SDK helper).
- **Package-level tests for queue and timer logic.** They are tested through `core.Engine` with a fake clock.
- **Multi-argument workflow and activity signatures.**
