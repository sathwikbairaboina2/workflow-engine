# ADR 0002: Send the full history on every workflow task

Date: 2026-10-04 · Status: accepted

## Decision

A workflow task carries the run's complete history. The worker builds a fresh deterministic runtime,
replays from event 1, and returns only the new commands. Workers keep no per-run cache.

## Why

Every workflow task becomes a full determinism check. A worker crash loses nothing, because there is no
cache to lose. The replay the chaos harness runs offline is the same code that runs in production.

## What I gave up

- **Cost per task grows with history length**: O(n) bytes on the wire and O(n) replay per task, so a long
  run costs O(n²) overall. The 50,000-event cap bounds the worst case. Sticky caches with incremental
  history are v0.2.
- **Latency.** Each task re-executes all previous steps of the workflow code.
