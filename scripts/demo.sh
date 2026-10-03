#!/usr/bin/env sh
# 30-second demo: start wfd and two workers, run five transfers, kill -9 a worker mid-flight and
# show that every transfer still finishes with a receipt. Uses host port 5400 (WF_PORT to change).
set -e
cd "$(dirname "$0")/.."
C="docker compose -f deploy/docker-compose.yml"
R="docker compose --progress quiet -f deploy/docker-compose.yml" # quiet: one-off CLI runs would print container progress

$C up -d --build --wait

for i in 1 2 3 4 5; do
  $R run --rm -T cli start Transfer --id "demo-$i" --queue transfers \
    --input "{\"from\":\"acct-$i\",\"to\":\"acct-0\",\"amount_cents\":$((i * 1000))}" >/dev/null
done
echo "started 5 transfers; sending SIGKILL to workflow-engine-worker-1"
sleep 1
docker kill -s KILL workflow-engine-worker-1
$C up -d worker

for i in 1 2 3 4 5; do
  printf 'demo-%s receipt: ' "$i"
  $R run --rm -T cli result "demo-$i" --timeout 60s
done
echo
$R run --rm -T cli history demo-1
echo
echo "Done. Tear down: docker compose -f deploy/docker-compose.yml down -v"
