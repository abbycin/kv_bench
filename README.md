# kv_bench (Mace vs RocksDB)

Quick start for reproducible Mace vs RocksDB comparison. Full guide: [docs/repro.md](./docs/repro.md).

## 5-Minute Quickstart
1. Set your storage root (any mount path, not hardcoded to `/nvme`):

```bash
export KV_BENCH_STORAGE_ROOT=/path/to/your/storage/kvbench
mkdir -p "${KV_BENCH_STORAGE_ROOT}"
```

2. Initialize Python env once:
assume that kv_bench repo is located in `$HOME`
```bash
cd "$HOME/kv_bench/scripts"
./init.sh
source ./bin/activate
cd "$HOME/kv_bench"
```

3. Run baseline comparison (both engines append to the same CSV):

Default quick-baseline timing in these scripts is `WARMUP_SECS=3` and `MEASURE_SECS=5`.

```bash
rm -rf "${KV_BENCH_STORAGE_ROOT}/basic_mace" "${KV_BENCH_STORAGE_ROOT}/basic_rocks"
mkdir -p "${KV_BENCH_STORAGE_ROOT}/basic_mace" "${KV_BENCH_STORAGE_ROOT}/basic_rocks"

./scripts/mace.sh "${KV_BENCH_STORAGE_ROOT}/basic_mace" ./scripts/benchmark_results.csv
./scripts/rocksdb.sh "${KV_BENCH_STORAGE_ROOT}/basic_rocks" ./scripts/benchmark_results.csv
```

4. Plot results:

```bash
./scripts/bin/python ./scripts/plot.py ./scripts/benchmark_results.csv ./scripts
```

5. Print a direct comparison table from the CSV:

```bash
./scripts/bin/python ./scripts/compare_baseline.py ./scripts/benchmark_results.csv
```

## What Is Compared
- Comparison unit: rows with identical `workload_id`, `threads`, `key_size`, `value_size`, `durability_mode`, `read_path`
- Fairness rule for read-heavy workloads: `get`, `scan`, `mixed`, and `W1`-`W6` run one GC/compaction pass after prefill and before warmup/measurement, so RocksDB is not compared with GC artificially disabled while reads may have to touch multiple SSTs
- Throughput metric: workload-level `ops_per_sec` (higher is better)
  - `W1/W2/W3/W4`: mixed read+update throughput
  - `W5`: mixed read+update+scan throughput
  - `W6`: scan throughput (counted by scan requests, not scanned key count)
- Tail latency metric: workload-level `p99_us` (lower is better)
  - This is the mixed p99 of all operations executed in that workload row, not per-op-type p99
  - `W1/W2/W3/W4`: mixed read+update p99
  - `W5`: mixed read+update+scan p99
  - `W6`: scan p99

Raw CSV path: `./scripts/benchmark_results.csv`

## Full Reproduction
For phase-by-phase commands, knobs, and interpretation rules, use [docs/repro.md](./docs/repro.md).
