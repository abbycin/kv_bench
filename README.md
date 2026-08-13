# kv_bench (Mace vs RocksDB)

Reproducible benchmark comparison of two embedded KV engines: mace and RocksDB.

## Quickstart
1. Clone the repo:

```bash
git clone https://github.com/abbycin/kv_bench
cd kv_bench
```

2. Initialize the Python env (used by the report script):

```bash
./scripts/init.sh
```

3. Run the benchmark for both engines — pass any fast storage path (e.g. an NVMe mount) as the storage root; results are appended to `./scripts/benchmark_results.csv`:

```bash
./scripts/mace.sh /path/to/nvme
./scripts/rocksdb.sh /path/to/nvme
```

4. Generate the comparison report:

```bash
./scripts/bin/python ./scripts/csv_to_html.py ./scripts/benchmark_results.csv
```

Output: `./scripts/benchmark_results.html`. 

## Engines and Harnesses
Each engine has its own self-contained benchmark binary with an identical CLI
(`--path`, `--workload W1..W6`, `--threads`, `--key-size`, `--value-size`,
`--prefill-keys`, `--warmup-secs`, `--measure-secs`, `--read-path`,
`--durability`, `--result-file`):

- mace: `src/bin/mace_bench.rs` → `target/release/mace_bench` (`cargo build --release`)
- RocksDB: `rocksdb/main.cpp` → `rocksdb/build/release/rocksdb_bench` (cmake)

The two harnesses share the same workload definitions, latency accounting and
CSV schema, but each is written directly against its engine's API.

## What Is Compared
- Comparison unit: rows with identical `workload_id`, `threads`, `key_size`, `value_size`, `durability_mode`, `read_path`
- Fairness rule: every workload (`W1`-`W6`) runs one GC/compaction pass after prefill and before warmup/measurement (mace `enable_gc()`, RocksDB compaction), so engines are not compared with GC artificially disabled while reads may have to touch stale data
- Throughput metric: workload-level `ops` (higher is better)
- Tail latency metric: workload-level `p99_us` (lower is better)
  - This is the workload-level p99 of all operations executed in that row, not per-op-type p99
- Operation semantics: every measured op is one transaction (write = `begin` + `put` + `commit`; read = snapshot or rw transaction), identical across engines, so `ops` is transactions per second
- Memory / backpressure strategy differs per engine: mace runs with `enable_backpressure=true`, RocksDB with bounded write buffers (`write_buffer_size=64MB` × `max_write_buffer_number=16`, writes block when full). Both are bounded to ~1 GiB of cache/buffer memory, so the comparison is on the same memory scale

## Why redb and sled Are Not Compared
Both were evaluated and rejected because their concurrency model cannot be
compared fairly with mace/RocksDB on the W1-W6 workloads, all of which include
multi-threaded concurrent writes.

- **redb (v4.1.0)** is a single-writer design (its design doc: "supports a single
  writer and multiple concurrent readers"; `begin_write()` blocks on a global
  write lock). Measured on this harness, its write-heavy throughput does not
  scale with threads at all (W4: ~3.2k ops/s at 1 thread, ~2.4k at 4), because
  every write serializes. Per-transaction commit cost is ~0.3-1ms, and relaxed
  mode additionally degrades quadratically over time: `process_freed_pages_nondurable`
  rescans the whole freed-page backlog on every commit. redb's own official
  benchmark confirms the same individual-write throughput (~1k txn/s), so these
  are native engine characteristics, not harness artifacts — they just are not
  comparable with concurrent-writer engines on the same workloads.
- **sled (v0.34.7)**'s transaction API (`Tree::transaction`) takes a process-global write
  lock too (`concurrency_control::write()`), so using transactions for the
  per-op write path would reproduce the same single-writer problem. Its plain
  `Tree::insert` is concurrent, but then the harness's per-op transaction
  accounting no longer matches the engine semantics; sled transactions also
  cannot scan (no rw_txn path), there is no manual compaction API (the shared
  GC/compaction fairness rule cannot be applied), and durability has no per-op
  sync switch (the default 500ms background fsync conflicts with the relaxed
  semantics). The comparison would not be apples-to-apples on any of the three
  axes the harness controls (transactions, durability, compaction).

## Workloads
- `W1`: `95%` read + `5%` update, uniform distribution
- `W2`: `95%` read + `5%` update, Zipf distribution
- `W3`: `50%` read + `50%` update, uniform distribution
- `W4`: `5%` read + `95%` update, uniform distribution
- `W5`: `70%` read + `25%` update + `5%` scan, uniform distribution
- `W6`: `100%` scan, uniform distribution; throughput is counted by scan requests, not scanned key count

Raw CSV path: `./scripts/benchmark_results.csv`

## Scripts
- `mace.sh` / `rocksdb.sh` — run the W1-W6 matrix for one engine; first argument is the storage root, second optional argument is the result CSV (defaults to `./scripts/benchmark_results.csv`)
- `csv_to_html.py` — single-page interactive HTML report (ops and p99 charts per workload, per key/value profile); colors are per key/value pair across engines, fill styles distinguish engines (mace solid, RocksDB hatch)
- `compare_baseline.py` — mace-vs-rocksdb ratio table from the CSV (pandas, use the `scripts/bin` venv)
- `thread_points.sh` — shared power-of-two thread points helper sourced by the run scripts
- `init.sh` — create the `scripts/bin` venv (pandas) used by the report scripts
