#!/usr/bin/env sh
# Runs every gate in order and stops at the first failure: gofmt, vet, wfcheck, race tests (all in one
# container), a short chaos soak, then the Docker build. Ends with GATES PASS.
set -e
cd "$(dirname "$0")/.."

echo "== gofmt, vet, wfcheck, go test -race"
sh scripts/gosh.sh '
if [ -n "$(gofmt -l .)" ]; then gofmt -l .; echo "gofmt: the files above need formatting"; exit 1; fi
go vet ./... || exit 1
go run ./cmd/wfcheck ./examples/... ./sdk/worker/... || exit 1
go test -race -count=1 ./... > /tmp/test.log 2>&1
rc=$?
tail -n 40 /tmp/test.log
exit $rc
'

echo "== chaos soak (100 workflows, 50 kills, seed 7)"
sh scripts/chaos.sh --workflows 100 --kills 50 --seed 7 --out /tmp/chaos-gate.json

echo "== docker build"
docker build -t workflow-engine:dev .

echo "GATES PASS"
