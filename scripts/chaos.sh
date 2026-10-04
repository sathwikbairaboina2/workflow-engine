#!/usr/bin/env sh
# Builds the binaries and runs the chaos harness inside golang:1.26.8. Flags pass through to cmd/chaos.
# Example: sh scripts/chaos.sh --workflows 100 --kills 50 --seed 1
set -e
cd "$(dirname "$0")/.."
root="$(pwd -W 2>/dev/null || pwd)"
# /tmp is a RAM disk by default: Docker Desktop's disk can take 100ms+ per fsync, which makes every SQLite
# commit in tests, the chaos soak and the benchmark crawl. kill -9 does not lose the page cache, so the
# soak proves the same thing either way. Set WF_TMPFS=0 to use the container disk (see scripts/bench.sh).
TMPFS="--tmpfs /tmp:rw,exec,mode=1777,size=4g"
[ "${WF_TMPFS:-1}" = 0 ] && TMPFS=""
MSYS_NO_PATHCONV=1 exec docker run --rm --name "workflow-engine-chaos-$$" -v "$root:/src" $TMPFS \
  -v workflow-engine-gomod:/go/pkg/mod -v workflow-engine-gobuild:/root/.cache/go-build -w /src golang:1.26.8 \
  sh -c 'go build -o /tmp/bin/ ./cmd/wfd ./examples/transfer/cmd/transfer-worker && go run ./cmd/chaos --bin-dir /tmp/bin --work-dir /tmp/chaos "$@"' chaos "$@"
