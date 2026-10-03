# ADR 0003: Activities are at-least-once; effectively-once needs an idempotency key

Date: 2026-10-04 · Status: accepted

## Decision

- An activity attempt can run more than once: a worker may apply a side effect and die before reporting.
  The engine never claims exactly-once execution.
- The SDK exposes `activity.GetInfo(ctx).IdempotencyKey`, equal to `run_id + ":" + scheduled_event_id`. It is
  derived from history, so it is identical across retries and across workers. This settles the design's
  open question in favour of a helper rather than a documented pattern.
- Retries are scheduled server-side on the task row (`attempt`, `visible_at`) and write no history.
  `ActivityTaskStarted` is appended only when the final attempt resolves, with that attempt number
  (Temporal's choice).
- The Transfer example ledger uses `INSERT ... ON CONFLICT(idempotency_key) DO NOTHING` and records every
  execution in a separate `executions` table. The chaos report can then show re-executions next to
  zero double-applies. `--unsafe-ledger` swaps in a random key as a control.

## What I gave up

- **Per-attempt debugging in history.** Failed attempts show up only in metrics and worker logs.
- **Side effects in systems without idempotency support** stay at-least-once. The docs say so.
- **Schedule-to-close and heartbeat timeouts** (v0.2). Only start-to-close is enforced, as the lease length.
