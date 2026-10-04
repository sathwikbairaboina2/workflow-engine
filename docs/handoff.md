## 2026-10-04 · Claude (Sonnet builder) · main
- Changed: engine, SDK, wf CLI, wfcheck, transfer example, Docker demo, chaos harness (cmd/chaos), benchmark (cmd/wfbench), README with test-enforced headline, semantics page, CI, gates script.
- Measured (bench/results/*.json): chaos seed 42, 500 workflows, 200 kill -9 (54 server): 0 lost, 0 double-applied, 9 re-executions; unsafe-ledger control double-applied 22. Bench: 703 transitions/s p99 81.7 ms on tmpfs; 10 transitions/s p50 199 ms on Docker Desktop disk.
- Left: v0.2 items in the spec (multi-node, history paging in replay, heartbeats); Opus review, DEVDOCS rewrite, board update.
- Verify: `sh scripts/gates.sh`; `sh scripts/gosh.sh 'WF_CHAOS=1 go test -race -count=1 -v -run TestKill9Soak ./chaos'`; `go test -run TestREADMEHeadline .`; `sh scripts/demo.sh` then `docker compose -f deploy/docker-compose.yml down -v`.
- Rulings: tmpfs by default for scripts; in-process FIFO write semaphore (ADR 0001/0006 addenda).

## 2026-10-04 · Claude (Opus lead verifier) · main
- Changed: reviewed `f74a1bd..86db000` (FIFO write semaphore in `store.WithTx`, 503 for canceled requests, transfer ledger open retry, chaos, bench, README, CI). There are no nested `WithTx` calls, so the capacity-1 semaphore cannot deadlock. Found no correctness bugs in the engine changes.
- Fixed: the soak command in the docs was `WF_CHAOS=1 sh scripts/gosh.sh ...`. That sets the variable on the host only, so `TestKill9Soak` skipped silently in about 1 s. The correct form is `sh scripts/gosh.sh 'WF_CHAOS=1 go test ...'`. The README headline now says "database on tmpfs" inside the bold line, and `readme_test.go` matches it. Wrote `docs/DEVDOCS.md`. Ignored `/.tmp/` (scratch logs).
- Gates (real output, this session):
  - `sh scripts/gates.sh` printed `GATES PASS`. All 19 test packages were `ok`, including TestREADMEHeadline. The chaos run with seed 7 gave 50 kills (9 of the server), 0 lost, 0 double-applied, `result PASS`. The docker build succeeded.
  - `TestKill9Soak` with `WF_CHAOS=1` in the container: `--- PASS (10.25s)`.
  - `demo.sh` exited 0. It printed 5 receipts, 1 SIGKILL, and a 23-event history for demo-1 ending in `WorkflowExecutionCompleted`. Ran `down -v`; no workflow-engine containers are left.
- Left: CI has never run on GitHub because there is no remote. The `go install` paths will work only after a push. v0.2 items are in the spec.
- Verify: `sh scripts/gates.sh`; `sh scripts/gosh.sh 'WF_CHAOS=1 go test -race -count=1 -v -run TestKill9Soak ./chaos'` (look for `--- PASS`, not a 1 s `ok`).
