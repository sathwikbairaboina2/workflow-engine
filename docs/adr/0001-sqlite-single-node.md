# ADR 0001: One SQLite file, one server process

Date: 2026-10-04 · Status: accepted

## Context

Every invariant in the spec (dense history, single open run, exactly-once timers, atomic transitions)
is easiest to prove when each state transition is one ACID transaction against one store.

## Decision

`wfd` is a single process that owns one SQLite database through `modernc.org/sqlite v1.60.1` (pure Go,
no cgo). The DSN sets `journal_mode(WAL)`, `synchronous(FULL)`, `busy_timeout(5000)` and
`_txlock=immediate`, so every transaction is `BEGIN IMMEDIATE` and writers queue instead of failing
with `SQLITE_BUSY` when a read lock upgrades. A prototype ran 200 concurrent read-modify-write
transactions with no lost update. History rows are protected by `BEFORE UPDATE` and `BEFORE DELETE`
triggers that `RAISE(ABORT)`.

## What I gave up

- **Horizontal scale and HA.** One writer, one node. If `wfd` is down, nothing progresses (workers retry).
  A Postgres store behind the same `store` package is v0.2.
- **Write throughput.** `synchronous=FULL` fsyncs every commit. The benchmark measures the ceiling instead of hiding it.
- **Bind-mounted databases on Windows.** The demo keeps the database on a named Docker volume, because
  SQLite locking over the Docker Desktop file bridge is not trustworthy.
