#!/bin/bash
# Latency-only re-run with the FIXED driver (metadata warmup + low-latency props).
# Keeps the existing throughput data; overwrites only the latency result files.
set -euo pipefail

PARTITIONS=(1 2 4)
RUNS=6
SIZE=256
LAT_SAMPLES=100000
LAT_RATE=10000
DURABILITY=default

OUT=results-r3
mkdir -p "$OUT" "$OUT/cpu"

# Non-durable on both sides (matches the fair diagnostic): Redpanda fsync bypassed.
export RP_FSYNC_FLAG="--unsafe-bypass-fsync=true"

reset_stack() { docker compose down -v >/dev/null 2>&1 || true; }
wait_healthy() {
  echo "  waiting for brokers..."
  for _ in $(seq 1 40); do
    local n; n=$(docker compose ps --format '{{.Health}}' 2>/dev/null | grep -c healthy || true)
    [ "${n:-0}" -ge 2 ] && { echo "  both healthy"; return 0; }
    sleep 2
  done
  echo "  WARN: not healthy"; docker compose ps
}

run_lat() {
  local engine=$1 parts=$2 run=$3
  local tag="${engine}-latency-p${parts}-${DURABILITY}-${run}"
  local args="--target $engine --mode latency --durability $DURABILITY --size $SIZE --partitions $parts --latency-samples $LAT_SAMPLES --rate $LAT_RATE"
  echo "  [$tag]"
  mpstat -P ALL 1 > "$OUT/cpu/${tag}.mpstat" 2>/dev/null &
  local mpid=$!
  ./gradlew -q bench --args="$args" 2>&1 | tee "$OUT/${tag}.txt" | grep '^{' || true
  kill "$mpid" 2>/dev/null || true
}

for engine in iggy redpanda; do
  for parts in "${PARTITIONS[@]}"; do
    echo "=== LATENCY CELL: $engine  partitions=$parts ==="
    reset_stack
    docker compose up -d >/dev/null
    wait_healthy
    for run in $(seq 1 $RUNS); do run_lat "$engine" "$parts" "$run"; done
  done
done

reset_stack
echo "=== LATENCY RE-RUN DONE ==="
