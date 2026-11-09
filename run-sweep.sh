#!/bin/bash
# Round 3 sweep: partition scaling × both engines × throughput + latency,
# with per-core CPU captured during each run.
#
# Requires: docker compose, a built ./gradlew, and sysstat (mpstat).
#   sudo apt-get install -y sysstat
#
# Each cell: fresh brokers (down -v) so the 0.9.0 data dir is clean and no topic
# state leaks between partition counts. mpstat samples all cores for the run's
# duration into results-r3/cpu/.
set -euo pipefail

PARTITIONS=(1 2 4)
RUNS=6
MESSAGES=1000000
SIZE=256
BATCH=1000
LAT_SAMPLES=100000
LAT_RATE=10000           # 10k Hz paced -> enables coordinated-omission correction
DURABILITY="${DURABILITY:-default}"   # default | durable (set env to run the durable sweep)

OUT=results-r3
mkdir -p "$OUT" "$OUT/cpu"

# Map durability -> Redpanda fsync flag for the compose file.
if [ "$DURABILITY" = "durable" ]; then
  export RP_FSYNC_FLAG="--unsafe-bypass-fsync=false"
else
  export RP_FSYNC_FLAG="--unsafe-bypass-fsync=true"
fi

reset_stack() {
  docker compose down -v >/dev/null 2>&1 || true
}

wait_healthy() {
  echo "  waiting for brokers..."
  for _ in $(seq 1 40); do
    local n
    n=$(docker compose ps --format '{{.Health}}' 2>/dev/null | grep -c healthy || true)
    [ "${n:-0}" -ge 2 ] && { echo "  both healthy"; return 0; }
    sleep 2
  done
  echo "  WARN: brokers not both healthy"; docker compose ps
}

# run_cell <engine> <mode> <partitions> <run#>
run_cell() {
  local engine=$1 mode=$2 parts=$3 run=$4
  local tag="${engine}-${mode}-p${parts}-${DURABILITY}-${run}"
  local args

  if [ "$mode" = "throughput" ]; then
    args="--target $engine --mode throughput --durability $DURABILITY --messages $MESSAGES --size $SIZE --batch $BATCH --partitions $parts"
  else
    args="--target $engine --mode latency --durability $DURABILITY --size $SIZE --partitions $parts --latency-samples $LAT_SAMPLES --rate $LAT_RATE"
  fi

  echo "  [$tag]"
  # Capture per-core CPU in the background for the duration of this run.
  # mpstat -P ALL 1 : one sample per second, all cores; killed when the run returns.
  mpstat -P ALL 1 > "$OUT/cpu/${tag}.mpstat" 2>/dev/null &
  local mpid=$!
  ./gradlew -q bench --args="$args" 2>&1 | tee "$OUT/${tag}.txt" | grep '^{' || true
  kill "$mpid" 2>/dev/null || true
}

for engine in iggy redpanda; do
  for parts in "${PARTITIONS[@]}"; do
    echo "=== CELL: $engine  partitions=$parts  durability=$DURABILITY ==="
    reset_stack
    docker compose up -d >/dev/null
    wait_healthy
    for run in $(seq 1 $RUNS); do
      run_cell "$engine" throughput "$parts" "$run"
    done
    for run in $(seq 1 $RUNS); do
      run_cell "$engine" latency "$parts" "$run"
    done
  done
done

reset_stack
echo "=== SWEEP DONE ($DURABILITY) ==="
echo "Throughput + latency JSON in $OUT/*.txt ; per-core CPU in $OUT/cpu/*.mpstat"
