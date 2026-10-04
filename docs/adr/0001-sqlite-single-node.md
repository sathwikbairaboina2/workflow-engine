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

## Addendum 2026-10-04 (found while building)

- **Writers queue in Go first.** In the first 500-workflow chaos run, `BEGIN IMMEDIATE` returned
  `SQLITE_BUSY` after the 10 s busy timeout even though only one process wrote: SQLite's busy handler
  retries by sleeping and is not fair, and about 20 long-polling workers kept barging in. `store.WithTx`
  now takes a one-slot FIFO semaphore before it begins, so one `wfd` process never contends with itself.
  `_txlock=immediate` stays for the one-process-owns-the-file rule and for other processes (the harness
  opens the file after `wfd` stops). The cost: reads (`store.View`) wait in the same queue.
- **`busy_timeout` is the first pragma.** `journal_mode(WAL)` and crash recovery take locks too, and a
  worker that opened the shared ledger right after a `kill -9` failed with `SQLITE_BUSY` when the pragma
  order put `busy_timeout` second.
- **Durability cost depends on the disk.** On the Docker Desktop disk used here a synchronous write took
  about 160 ms (`dd oflag=dsync`), which caps `synchronous=FULL` at roughly 10 transitions/s. The README
  states which filesystem each measurement ran on (ADR 0006).
