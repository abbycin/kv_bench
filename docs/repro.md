# kv_bench Reproduction Guide (Mace vs RocksDB)

This document defines a reproducible workflow for `mace` and `rocksdb` across phase0~phase4.
It now has two profiles:

- `local` (default in this doc): validated on this machine class, intended to run end-to-end without exhausting resources.
- `full`: benchmark-refactor target matrix (much longer runtime).

## 1. Prerequisites
- Linux
- Rust/Cargo
- CMake (to build `rocksdb_bench`)
- Python 3 (reporting/plotting)
- A persistent storage path (NVMe/SSD recommended), **not tmpfs**

## 2. Hardware + Storage Baseline
For the local profile, assume approximately:
- CPU: `6C12T`
- RAM: `32GB`
- Disk: `100GB` available benchmark storage

Before running, set paths and verify filesystem type/capacity:

```bash
# $NMVE is the NVMe SSD mount point
export KV_BENCH_STORAGE_ROOT="$NVME/repro_storage"
export KV_BENCH_RESULT_ROOT="$NVME/repro_results"
mkdir -p "${KV_BENCH_STORAGE_ROOT}" "${KV_BENCH_RESULT_ROOT}"

df -hT "${KV_BENCH_STORAGE_ROOT}" "${KV_BENCH_RESULT_ROOT}"
free -h
```

Requirements:
- `KV_BENCH_STORAGE_ROOT` and `KV_BENCH_RESULT_ROOT` must not be on `tmpfs`.
- Keep at least `25GB` free under storage root before long runs.

## 3. Initialization
Assume that kv_bench repo is located in `$HOME`
```bash
cd "$HOME/kv_bench/scripts"
./init.sh
source ./bin/activate
cd "$HOME/kv_bench"
```

## 4. Quick Baseline (W1~W6)
Clean old data:

```bash
rm -rf "${KV_BENCH_STORAGE_ROOT}/basic_mace" "${KV_BENCH_STORAGE_ROOT}/basic_rocks"
mkdir -p "${KV_BENCH_STORAGE_ROOT}/basic_mace" "${KV_BENCH_STORAGE_ROOT}/basic_rocks"
rm -f "${KV_BENCH_RESULT_ROOT}/benchmark_results.csv"
```

Run both engines (`local` profile parameters):

```bash
WARMUP_SECS=3 MEASURE_SECS=5 PREFILL_KEYS=50000 \
./scripts/mace.sh "${KV_BENCH_STORAGE_ROOT}/basic_mace" "${KV_BENCH_RESULT_ROOT}/benchmark_results.csv"

WARMUP_SECS=3 MEASURE_SECS=5 PREFILL_KEYS=50000 \
./scripts/rocksdb.sh "${KV_BENCH_STORAGE_ROOT}/basic_rocks" "${KV_BENCH_RESULT_ROOT}/benchmark_results.csv"
```

Fairness note:
- For read-heavy workloads (`get`, `scan`, and `W1`-`W6`), both harnesses run one GC/compaction pass after prefill and before warmup/measurement.
- The purpose is to make the RocksDB vs Mace comparison fairer, since RocksDB reads may need to touch multiple SSTs and should not be benchmarked with GC/compaction artificially disabled.

Generate plots:

```bash
./scripts/bin/python ./scripts/plot.py "${KV_BENCH_RESULT_ROOT}/benchmark_results.csv" "${KV_BENCH_RESULT_ROOT}"
```

## 5. Phase Reproduction
Default thread points now follow host CPU count:
- Always use powers of two up to the largest point not exceeding host CPU count (e.g., `4 -> 1 2 4`, `8 -> 1 2 4 8`, `12 -> 1 2 4 8`, `16 -> 1 2 4 8 16`)

You can still override via `PHASE1_THREADS` / `PHASE2_THREADS_TIER_M` / `PHASE3_THREADS`.

Default KV profiles in scripts:
- `P1`: `16/128`
- `P2`: `32/1024`
- `P3`: `32/4096`
- `P4`: `32/16384` (large-value group)

### 5.1 Phase 1
```bash
rm -rf "${KV_BENCH_STORAGE_ROOT}/phase1"
mkdir -p "${KV_BENCH_STORAGE_ROOT}/phase1"
rm -f "${KV_BENCH_RESULT_ROOT}/phase1_results.csv"

WARMUP_SECS=10 MEASURE_SECS=20 REPEATS=2 \
PHASE1_WORKLOADS="W1 W3 W6" \
PHASE1_PROFILES="P2" \
PHASE1_PREFILL_TIER_S_P2=200000 \
./scripts/phase1.sh "${KV_BENCH_STORAGE_ROOT}/phase1" "${KV_BENCH_RESULT_ROOT}/phase1_results.csv"
```

### 5.2 Phase 2
```bash
rm -rf "${KV_BENCH_STORAGE_ROOT}/phase2"
mkdir -p "${KV_BENCH_STORAGE_ROOT}/phase2"
rm -f "${KV_BENCH_RESULT_ROOT}/phase2_results.csv"

WARMUP_SECS=10 MEASURE_SECS=20 REPEATS=2 \
PHASE2_WORKLOADS_TIER_M="W1 W3 W6" \
PHASE2_PROFILES="P2" \
PHASE2_PREFILL_TIER_M_P2=500000 \
RUN_TIER_L_REPRESENTATIVE=0 \
./scripts/phase2.sh "${KV_BENCH_STORAGE_ROOT}/phase2" "${KV_BENCH_RESULT_ROOT}/phase2_results.csv"
```

Optional (`full` profile tier-l representative subset):

```bash
RUN_TIER_L_REPRESENTATIVE=1 TIER_L_REPEATS=1 \
./scripts/phase2.sh "${KV_BENCH_STORAGE_ROOT}/phase2" "${KV_BENCH_RESULT_ROOT}/phase2_results.csv"
```

### 5.3 Phase 3
```bash
rm -rf "${KV_BENCH_STORAGE_ROOT}/phase3"
mkdir -p "${KV_BENCH_STORAGE_ROOT}/phase3"
rm -f "${KV_BENCH_RESULT_ROOT}/phase3_results.csv"

WARMUP_SECS=10 MEASURE_SECS=20 REPEATS=2 \
PHASE3_WORKLOADS="W1 W3" \
PHASE3_DURABILITIES="relaxed durable" \
PHASE3_KEY_SIZE=32 PHASE3_VALUE_SIZE=1024 PHASE3_PREFILL_KEYS=500000 \
./scripts/phase3.sh "${KV_BENCH_STORAGE_ROOT}/phase3" "${KV_BENCH_RESULT_ROOT}/phase3_results.csv"
```

### 5.4 Phase 4 (run one engine at a time)
`local` profile (memory-safe on 32GB machines, validated on 2026-03-04):

```bash
rm -rf "${KV_BENCH_STORAGE_ROOT}/phase4_mace"
rm -f "${KV_BENCH_RESULT_ROOT}/phase4_results_mace.csv" "${KV_BENCH_RESULT_ROOT}/phase4_restart_mace.csv"

SOAK_HOURS=1 PHASE4_MAX_CYCLES=3 \
PHASE4_WORKLOAD_MAIN=W1 PHASE4_WORKLOAD_VERIFY=W1 \
SEED_MEASURE_SECS=2 RUN_MEASURE_SECS=10 CRASH_INTERVAL_SECS=3 VERIFY_MEASURE_SECS=2 WARMUP_SECS=1 \
PHASE4_THREADS=1 PHASE4_KEY_SIZE=32 PHASE4_VALUE_SIZE=128 PHASE4_PREFILL_KEYS=1000 \
./scripts/phase4_soak.sh mace "${KV_BENCH_STORAGE_ROOT}/phase4_mace" \
  "${KV_BENCH_RESULT_ROOT}/phase4_results_mace.csv" "${KV_BENCH_RESULT_ROOT}/phase4_restart_mace.csv"

rm -rf "${KV_BENCH_STORAGE_ROOT}/phase4_rocks"
rm -f "${KV_BENCH_RESULT_ROOT}/phase4_results_rocks.csv" "${KV_BENCH_RESULT_ROOT}/phase4_restart_rocks.csv"

SOAK_HOURS=1 PHASE4_MAX_CYCLES=3 \
PHASE4_WORKLOAD_MAIN=W1 PHASE4_WORKLOAD_VERIFY=W1 \
SEED_MEASURE_SECS=2 RUN_MEASURE_SECS=10 CRASH_INTERVAL_SECS=3 VERIFY_MEASURE_SECS=2 WARMUP_SECS=1 \
PHASE4_THREADS=1 PHASE4_KEY_SIZE=32 PHASE4_VALUE_SIZE=128 PHASE4_PREFILL_KEYS=1000 \
./scripts/phase4_soak.sh rocksdb "${KV_BENCH_STORAGE_ROOT}/phase4_rocks" \
  "${KV_BENCH_RESULT_ROOT}/phase4_results_rocks.csv" "${KV_BENCH_RESULT_ROOT}/phase4_restart_rocks.csv"
```

Notes:
- The previous local phase4 parameters (`W3/W6`, larger prefill/time windows) can exceed 32GB RAM on current `mace` builds.
- The local phase4 profile above keeps crash/restart semantics, but intentionally reduces write pressure so it completes on this host class.

For `full` profile, remove the `PHASE4_*` overrides and use benchmark-refactor defaults (recommend `>=64GB` RAM and ample NVMe space).

## 6. Result Files
The commands above write CSVs under `${KV_BENCH_RESULT_ROOT}`:
- `benchmark_results.csv`
- `phase1_results.csv`
- `phase2_results.csv`
- `phase3_results.csv`
- `phase4_results_*.csv`
- `phase4_restart_*.csv`

Unified schema columns include:
- `engine` (`mace` / `rocksdb`)
- `workload_id` (`W1..W6`)
- `durability_mode` (`relaxed` / `durable`)
- `threads,key_size,value_size,prefill_keys`
- `ops`
- `total_ops,ok_ops,err_ops`
- `p50_us,p95_us,p99_us,p999_us`
- `read_path`

## 7. Report Commands
```bash
./scripts/bin/python ./scripts/phase1_eval.py "${KV_BENCH_RESULT_ROOT}/phase1_results.csv"
./scripts/bin/python ./scripts/phase2_report.py "${KV_BENCH_RESULT_ROOT}/phase2_results.csv"
./scripts/bin/python ./scripts/phase3_report.py "${KV_BENCH_RESULT_ROOT}/phase3_results.csv"
./scripts/bin/python ./scripts/phase4_report.py "${KV_BENCH_RESULT_ROOT}/phase4_restart_mace.csv"
./scripts/bin/python ./scripts/phase4_report.py "${KV_BENCH_RESULT_ROOT}/phase4_restart_rocks.csv"
```

## 8. Full-Profile Toggle (Benchmark Refactor Matrix)
If you want the full benchmark-refactor matrix:
- Use default phase script matrices (no `PHASE*_*` narrowing).
- Increase `WARMUP_SECS/MEASURE_SECS/REPEATS` to target values.
- Enable `RUN_TIER_L_REPRESENTATIVE=1` as needed.
- Keep large runs on persistent NVMe storage with enough free disk.

## 9. Comparison Rules
Only compare rows with identical:
- `workload_id`
- `key_size/value_size`
- `threads`
- `durability_mode`
- `read_path`
- read-heavy workload rows are expected to include the pre-run GC/compaction pass described above

If `err_ops > 0`, investigate that case before drawing conclusions.
