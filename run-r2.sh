#!/bin/bash
set -e
RUNS=6
ARGS="--messages 1000000 --size 256 --batch 1000 --partitions 1"
mkdir -p results-r2

reset_stack() {
  docker compose -f docker-compose.r2.yml down -v >/dev/null 2>&1 || true
}

wait_healthy() {
  echo "  waiting for brokers to be healthy..."
  for i in $(seq 1 30); do
    up=$(docker compose -f docker-compose.r2.yml ps --format '{{.Health}}' | grep -c healthy || true)
    [ "$up" -ge 2 ] && { echo "  both healthy"; return 0; }
    sleep 2
  done
  echo "  WARN: brokers not both healthy after 60s"; docker compose -f docker-compose.r2.yml ps
}

run_cell() {
  local engine=$1 fsync=$2 iggy_fsync=$3 rp_flag=$4
  echo "=== CELL: $engine fsync=$fsync ==="
  reset_stack
  IGGY_FSYNC="$iggy_fsync" RP_FSYNC_FLAG="$rp_flag" \
    docker compose -f docker-compose.r2.yml up -d >/dev/null
  wait_healthy
  for i in $(seq 1 $RUNS); do
    echo "  $engine fsync=$fsync run $i"
    ./gradlew -q bench --args="--target $engine $ARGS" 2>&1 \
      | tee "results-r2/${engine}-fsync-${fsync}-${i}.txt" | grep '^{' || true
  done
}

# Four cells. iggy_fsync sets the Iggy env var; rp_flag sets Redpanda's flag.
run_cell iggy     off false "--unsafe-bypass-fsync=true"
run_cell iggy     on  true  "--unsafe-bypass-fsync=true"
run_cell redpanda off false "--unsafe-bypass-fsync=true"
run_cell redpanda on  false "--unsafe-bypass-fsync=false"

echo "=== ALL CELLS DONE ==="
