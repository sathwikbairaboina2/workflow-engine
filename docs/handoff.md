## 2026-10-04 · Claude (Sonnet builder) · main
- Changed: engine, SDK, wf CLI, wfcheck, transfer example, Docker demo, chaos harness (cmd/chaos), benchmark (cmd/wfbench), README with test-enforced headline, semantics page, CI, gates script.
- Measured (bench/results/*.json): chaos seed 42, 500 workflows, 200 kill -9 (54 server): 0 lost, 0 double-applied, 9 re-executions; unsafe-ledger control double-applied 22. Bench: 703 transitions/s p99 81.7 ms on tmpfs; 10 transitions/s p50 199 ms on Docker Desktop disk.
- Left: v0.2 items in the spec (multi-node, history paging in replay, heartbeats); Opus review, DEVDOCS rewrite, board update.
- Verify: `sh scripts/gates.sh`; `WF_CHAOS=1 sh scripts/gosh.sh 'go test -race -run TestKill9Soak ./chaos'`; `go test -run TestREADMEHeadline .`; `sh scripts/demo.sh` then `docker compose -f deploy/docker-compose.yml down -v`.
- Rulings: tmpfs by default for scripts; in-process FIFO write semaphore (ADR 0001/0006 addenda).
