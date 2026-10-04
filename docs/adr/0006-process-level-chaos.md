# ADR 0006: The chaos harness kills processes, not containers

Date: 2026-10-04 · Status: accepted

## Decision

`cmd/chaos` spawns `wfd` and N `transfer-worker` processes itself. It sends `SIGKILL` (`os.Process.Kill`)
on a schedule drawn from a seeded PCG generator, restarts the victim after a short delay, and checks
invariants after draining. It runs the same way inside the `golang:1.26.8` container (`sh scripts/chaos.sh`) and
on the CI runner. The design's compose variant, which mounted the Docker socket to `docker kill`
sibling containers, is dropped. The compose demo still lets a person `docker kill -s KILL` a worker by hand.

## What I gave up

- **Container-level faults** (network namespace teardown, volume remount). Only process death is injected.
- **Exact reproduction across machines.** The seed fixes which process is killed and when, measured from
  harness start, but not which instruction it was running. Reports still record the seed.
- **Partition and disk-full faults**: out of scope for v0.1.

## Addendum 2026-10-04 (found while building)

`kill -9` ends a process but leaves the kernel page cache intact, so the soak tests atomicity of
transitions under process death, not durability across power loss. The chaos and benchmark scripts therefore
mount `/tmp` as tmpfs by default (`WF_TMPFS=0` switches to the container disk): on Docker Desktop an fsync
took about 160 ms, which made a 500-workflow soak run for tens of minutes without changing what it proves.
Every result file records the filesystem it ran on (`bench-latest.json` and `bench-disk.json` show both).
