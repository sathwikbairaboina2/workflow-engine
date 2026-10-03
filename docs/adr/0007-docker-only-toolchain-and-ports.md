# ADR 0007: Go runs in Docker; host ports 5400-5409; names start with workflow-engine-

Date: 2026-10-04 · Status: accepted

## Decision

- `scripts/go.ps1`, `scripts/go.sh` and `scripts/gosh.sh` run `go` in `golang:1.26.8` (verified as `go1.26.8
  linux/amd64`) with named volumes `workflow-engine-gomod` and `workflow-engine-gobuild`. Each container is
  named `workflow-engine-go-<pid>-<random>`. CI uses `actions/setup-go` with Go 1.26 natively.
- The compose project is named `workflow-engine`. `wfd` is published on host port 5400 (container port 7233).
  Other sessions on this machine own other port ranges.
- Chaos and benchmark runs keep their databases inside the container filesystem, not on a bind mount.

## What I gave up

- **Edit-test latency**: every command pays for container start-up.
- **Host IDE tooling** still needs a local Go install for gopls.
- **Benchmark realism**: numbers come from Docker Desktop's Linux VM on a Windows laptop, and the README says so.
