#!/usr/bin/env sh
# Runs a shell command inside the Go toolchain image (build-then-run sequences).
set -e
cd "$(dirname "$0")/.."
root="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm --name "workflow-engine-sh-$$-$(date +%s)" -v "$root:/src" \
  -v workflow-engine-gomod:/go/pkg/mod -v workflow-engine-gobuild:/root/.cache/go-build \
  -w /src golang:1.26.8 sh -c "$1"
