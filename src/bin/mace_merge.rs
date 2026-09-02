//! Merge-operator counter benchmark for mace.
//!
//! Workload: a pool of `--num-keys` keys (deterministic pseudo-random bytes of
//! `--key-size`), each holding a u64 little-endian counter. Every op picks a
//! key uniformly at random, then with probability `--merge-ratio` appends a
//! merge operand (+1) and otherwise reads the current counter. Value size is
//! fixed at 8 bytes (u64). Threads, key size, key count, ratio, and duration
//! are configurable; results append to a CSV shared with `rocksdb/main_merge.cpp`.
//!
//! With `--accumulate-per-key N > 0` the bench first appends exactly N merge
//! operands to every key (batched, GC/compaction stays disabled so the chains
//! genuinely accumulate), then measures a 100% get workload: get cost as a
//! function of accumulated operand-chain length.

use clap::{ArgAction, Parser};
use mace::{BucketOptions, Mace, OpCode, Options, u64_add_operator};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LAT_BUCKETS: usize = 64;
const PREFILL_BATCH: usize = 1024;
/// u64 counter: value and merge operand are 8-byte little-endian u64.
const VALUE_LEN: usize = 8;
/// external retry budget for `OpCode::Again` (merge admission under contention)
const MERGE_RETRY: u32 = 64;
const OPERAND: [u8; VALUE_LEN] = 1u64.to_le_bytes();
const ZERO: [u8; VALUE_LEN] = 0u64.to_le_bytes();

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'p', long)]
    path: String,

    #[arg(short = 'k', long, default_value_t = 32)]
    key_size: usize,

    #[arg(short = 'n', long, default_value_t = 100)]
    num_keys: usize,

    #[arg(short = 't', long, default_value_t = 4)]
    threads: usize,

    #[arg(short = 'i', long, default_value_t = 10000)]
    iterations: usize,

    #[arg(long, default_value_t = 0.7)]
    merge_ratio: f64,

    /// append exactly N merge operands to every key first, then measure 100% gets
    #[arg(long)]
    accumulate_per_key: Option<usize>,

    #[arg(long, default_value_t = 0)]
    warmup_secs: u64,

    #[arg(long, default_value_t = 0)]
    measure_secs: u64,

    #[arg(long, default_value = "relaxed")]
    durability: String,

    #[arg(long, default_value = "merge_benchmark_results.csv")]
    result_file: String,

    #[arg(long, default_value_t = true)]
    cleanup: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    no_cleanup: bool,

    #[arg(long, default_value_t = false)]
    skip_prefill: bool,

    #[arg(long, default_value_t = false)]
    reuse_path: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurabilityMode {
    Relaxed,
    Durable,
}

impl DurabilityMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "relaxed" => Some(Self::Relaxed),
            "durable" => Some(Self::Durable),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            DurabilityMode::Relaxed => "relaxed",
            DurabilityMode::Durable => "durable",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Quantiles {
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    p999_us: u64,
}

#[derive(Clone, Debug)]
struct ResultRow {
    engine: &'static str,
    mode: &'static str,
    durability_mode: DurabilityMode,
    threads: usize,
    key_size: usize,
    value_size: usize,
    prefill_keys: usize,
    read_pct: u8,
    update_pct: u8,
    warmup_secs: u64,
    measure_secs: u64,
    total_op: u64,
    ok_op: u64,
    err_op: u64,
    ops: f64,
    quantiles: Quantiles,
    elapsed_us: u64,
}

#[derive(Clone, Debug)]
struct ThreadStats {
    total_op: u64,
    err_op: u64,
    hist: [u64; LAT_BUCKETS],
}

impl Default for ThreadStats {
    fn default() -> Self {
        Self {
            total_op: 0,
            err_op: 0,
            hist: [0; LAT_BUCKETS],
        }
    }
}

fn split_ranges(total: usize, n: usize) -> Vec<usize> {
    let mut lens = Vec::with_capacity(n);
    if n == 0 {
        return lens;
    }
    let base = total / n;
    let rem = total % n;
    for tid in 0..n {
        lens.push(base + usize::from(tid < rem));
    }
    lens
}

/// deterministic pseudo-random key bytes for a key id (splitmix64 stream)
fn make_key(id: usize, key_size: usize) -> Vec<u8> {
    let mut state = id as u64;
    let mut out = Vec::with_capacity(key_size);
    while out.len() < key_size {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        out.extend_from_slice(&z.to_le_bytes());
    }
    out.truncate(key_size);
    out
}

fn latency_bucket(us: u64) -> usize {
    let v = us.max(1);
    (63 - v.leading_zeros() as usize).min(LAT_BUCKETS - 1)
}

fn histogram_quantile_us(hist: &[u64; LAT_BUCKETS], q: f64) -> u64 {
    let total: u64 = hist.iter().sum();
    if total == 0 {
        return 0;
    }
    let target = ((total as f64) * q).ceil() as u64;
    let mut acc = 0u64;
    for (idx, cnt) in hist.iter().enumerate() {
        acc += *cnt;
        if acc >= target {
            return if idx == 0 { 1 } else { 1u64 << idx };
        }
    }
    1u64 << (LAT_BUCKETS - 1)
}

fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn result_header() -> &'static str {
    "engine,workload_id,mode,durability_mode,threads,key_size,value_size,prefill_keys,shared_keyspace,distribution,zipf_theta,read_pct,update_pct,scan_pct,scan_len,read_path,warmup_secs,measure_secs,total_op,ok_op,err_op,ops,p50_us,p95_us,p99_us,p999_us,elapsed_us"
}

fn result_row_csv(row: &ResultRow, workload_id: &str) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{},{},{:.4},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        row.engine,
        workload_id,
        row.mode,
        row.durability_mode.as_str(),
        row.threads,
        row.key_size,
        row.value_size,
        row.prefill_keys,
        true,
        "uniform",
        0.99,
        row.read_pct,
        row.update_pct,
        0,
        0,
        "snapshot",
        row.warmup_secs,
        row.measure_secs,
        row.total_op,
        row.ok_op,
        row.err_op,
        row.ops as u64,
        row.quantiles.p50_us,
        row.quantiles.p95_us,
        row.quantiles.p99_us,
        row.quantiles.p999_us,
        row.elapsed_us,
    )
}

fn append_result_row(path: &str, row: &ResultRow, workload_id: &str) -> std::io::Result<()> {
    let exists = Path::new(path).exists();
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    if !exists {
        writer.write_all(result_header().as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.write_all(result_row_csv(row, workload_id).as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// merge +1 onto the key; retries with a fresh transaction on `OpCode::Again`
/// (merge admission under contention), bounded by `MERGE_RETRY`
fn run_merge(bucket: &mace::Bucket, key: &[u8]) -> bool {
    for _ in 0..MERGE_RETRY {
        let Ok(tx) = bucket.begin() else {
            return false;
        };
        match tx.merge(key, OPERAND.as_slice()) {
            Ok(()) => return tx.commit().is_ok(),
            Err(OpCode::Again) => continue,
            Err(_) => return false,
        }
    }
    false
}

/// read the current counter through a snapshot view
fn run_get(bucket: &mace::Bucket, key: &[u8]) -> bool {
    match bucket.view() {
        Ok(view) => match view.get(key) {
            Ok(v) => {
                std::hint::black_box(v.slice());
                true
            }
            Err(_) => false,
        },
        Err(_) => false,
    }
}

/// append exactly `per_key` merge operands to every key, batched per txn.
/// Contiguous per-thread key ranges keep threads disjoint (no cross-thread
/// contention); GC stays disabled so the operand chains genuinely accumulate.
fn accumulate_operands(
    bucket: &mace::Bucket,
    threads: usize,
    num_keys: usize,
    key_size: usize,
    per_key: usize,
) {
    let lens = split_ranges(num_keys, threads);
    let mut handles = Vec::with_capacity(threads);
    let mut start = 0usize;
    for (tid, len) in lens.iter().copied().enumerate() {
        let range = start..(start + len);
        start += len;
        let bucket = bucket.clone();
        handles.push(std::thread::spawn(move || {
            coreid::bind_core(tid);
            let mut in_batch = 0usize;
            let mut tx = bucket.begin().unwrap();
            for id in range {
                let key = make_key(id, key_size);
                for _ in 0..per_key {
                    tx.merge(key.as_slice(), OPERAND.as_slice()).unwrap();
                    in_batch += 1;
                    if in_batch >= PREFILL_BATCH {
                        tx.commit().unwrap();
                        tx = bucket.begin().unwrap();
                        in_batch = 0;
                    }
                }
            }
            if in_batch > 0 {
                tx.commit().unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

fn run_one_op(
    bucket: &mace::Bucket,
    rng: &mut StdRng,
    num_keys: usize,
    key_size: usize,
    merge_ratio: f64,
    accumulate: bool,
    stats: Option<&mut ThreadStats>,
) {
    let start = stats.as_ref().map(|_| Instant::now());

    let id = rng.random_range(0..num_keys);
    let key = make_key(id, key_size);
    let roll: f64 = rng.random();
    let ok = if !accumulate && roll < merge_ratio {
        run_merge(bucket, &key)
    } else {
        run_get(bucket, &key)
    };

    if let Some(stats) = stats {
        stats.total_op += 1;
        if !ok {
            stats.err_op += 1;
        }
        if let Some(start) = start {
            let us = start.elapsed().as_micros() as u64;
            let idx = latency_bucket(us);
            stats.hist[idx] += 1;
        }
    }
}

fn main() {
    let args = Args::parse();
    let path = Path::new(&args.path);
    let cleanup = args.cleanup && !args.no_cleanup;

    if args.path.is_empty() {
        eprintln!("path is empty");
        exit(1);
    }
    if path.exists() && !args.reuse_path {
        eprintln!("path {:?} already exists", args.path);
        exit(1);
    }
    if args.skip_prefill && !args.reuse_path {
        eprintln!("--skip-prefill requires --reuse-path");
        exit(1);
    }
    if args.skip_prefill && !path.exists() {
        eprintln!(
            "--skip-prefill requires existing path, but `{}` does not exist",
            args.path
        );
        exit(1);
    }
    if args.threads == 0 {
        eprintln!("threads must be greater than 0");
        exit(1);
    }
    if args.key_size < 16 {
        eprintln!("key_size must be >= 16");
        exit(1);
    }
    if args.num_keys == 0 {
        eprintln!("num_keys must be greater than 0");
        exit(1);
    }
    if !(0.0..=1.0).contains(&args.merge_ratio) {
        eprintln!("merge_ratio must be in range [0, 1]");
        exit(1);
    }

    let durability_mode = match DurabilityMode::parse(&args.durability) {
        Some(v) => v,
        None => {
            eprintln!(
                "invalid durability `{}` (supported: relaxed, durable)",
                args.durability
            );
            exit(1);
        }
    };

    let mut opt = Options::new(path);
    opt.sync_on_write = durability_mode == DurabilityMode::Durable;
    opt.concurrent_write = 16;
    opt.lru_capacity = 1 << 30;
    opt.blob_handle_cache_capacity = 256;
    opt.data_handle_cache_capacity = 256;
    opt.gc_timeout = 5 * 1000;
    opt.gc_eager = false;
    opt.data_garbage_ratio = 50;
    opt.tmp_store = cleanup;

    let mut bopt = BucketOptions::new();
    bopt.checkpoint_size = 128 << 20;
    bopt.cache_capacity = 4 << 30;
    bopt.enable_backpressure = true;
    bopt.pool_capacity = 1 << 30;
    bopt.merge_operator = u64_add_operator();

    let db = Mace::new(opt.validate().unwrap()).unwrap();
    db.disable_gc();
    let bkt = if args.reuse_path {
        db.open_bucket_with_options("default", bopt.clone())
            .or_else(|_| db.new_bucket("default", bopt.clone()))
            .unwrap()
    } else {
        db.new_bucket("default", bopt).unwrap()
    };

    if !args.skip_prefill {
        let mut fill_handles = Vec::with_capacity(args.threads);
        for tid in 0..args.threads {
            let bucket = bkt.clone();
            let key_size = args.key_size;
            let num_keys = args.num_keys;
            let threads = args.threads;
            fill_handles.push(std::thread::spawn(move || {
                coreid::bind_core(tid);
                let mut in_batch = 0usize;
                let mut tx = bucket.begin().unwrap();
                for id in (tid..num_keys).step_by(threads) {
                    let key = make_key(id, key_size);
                    tx.put(key.as_slice(), ZERO.as_slice()).unwrap();
                    in_batch += 1;
                    if in_batch >= PREFILL_BATCH {
                        tx.commit().unwrap();
                        tx = bucket.begin().unwrap();
                        in_batch = 0;
                    }
                }
                if in_batch > 0 {
                    tx.commit().unwrap();
                }
            }));
        }
        for h in fill_handles {
            h.join().unwrap();
        }
    }

    let accumulate = args.accumulate_per_key.is_some();
    let accumulate_per_key = args.accumulate_per_key.unwrap_or(0);

    if accumulate {
        // keep GC disabled so accumulated operand chains stay raw
        accumulate_operands(&bkt, args.threads, args.num_keys, args.key_size, accumulate_per_key);
    } else {
        db.enable_gc();
    }

    let op_counts = split_ranges(args.iterations, args.threads);
    let ready_barrier = Arc::new(Barrier::new(args.threads + 1));
    let measure_barrier = Arc::new(Barrier::new(args.threads + 1));
    // earliest steady-clock elapsed-ns (since bench_start) across all workers; u64::MAX = unset
    let measure_start = Arc::new(AtomicU64::new(u64::MAX));
    let bench_start = Instant::now();

    let handles: Vec<JoinHandle<ThreadStats>> = (0..args.threads)
        .map(|tid| {
            let bucket = bkt.clone();
            let ready = Arc::clone(&ready_barrier);
            let measure = Arc::clone(&measure_barrier);
            let measure_start_slot = Arc::clone(&measure_start);
            let bench_start = bench_start;
            let key_size = args.key_size;
            let num_keys = args.num_keys;
            let merge_ratio = args.merge_ratio;
            let accumulate = accumulate;
            let warmup_secs = args.warmup_secs;
            let measure_secs = args.measure_secs;
            let local_op_count = op_counts[tid];

            std::thread::spawn(move || {
                coreid::bind_core(tid);
                let seed = (now_epoch_ms() as u64)
                    ^ (tid as u64)
                        .wrapping_add(1)
                        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    ^ (num_keys as u64).wrapping_shl(7);
                let mut rng = StdRng::seed_from_u64(seed);
                let mut stats = ThreadStats::default();

                ready.wait();

                if warmup_secs > 0 {
                    let deadline = Instant::now() + Duration::from_secs(warmup_secs);
                    while Instant::now() < deadline {
                        run_one_op(
                            &bucket,
                            &mut rng,
                            num_keys,
                            key_size,
                            merge_ratio,
                            accumulate,
                            None,
                        );
                    }
                }

                measure.wait();
                measure_start_slot.fetch_min(
                    bench_start.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );

                if measure_secs > 0 {
                    let deadline = Instant::now() + Duration::from_secs(measure_secs);
                    while Instant::now() < deadline {
                        run_one_op(
                            &bucket,
                            &mut rng,
                            num_keys,
                            key_size,
                            merge_ratio,
                            accumulate,
                            Some(&mut stats),
                        );
                    }
                } else {
                    for _ in 0..local_op_count {
                        run_one_op(
                            &bucket,
                            &mut rng,
                            num_keys,
                            key_size,
                            merge_ratio,
                            accumulate,
                            Some(&mut stats),
                        );
                    }
                }

                stats
            })
        })
        .collect();

    ready_barrier.wait();
    measure_barrier.wait();
    measure_start.fetch_min(bench_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

    let mut merged_hist = [0u64; LAT_BUCKETS];
    let mut total_op = 0u64;
    let mut err_op = 0u64;

    for h in handles {
        let s = h.join().unwrap();
        total_op += s.total_op;
        err_op += s.err_op;
        for (i, v) in s.hist.iter().enumerate() {
            merged_hist[i] += *v;
        }
    }

    let measure_started_ns = measure_start.load(Ordering::Relaxed);
    let elapsed_us = if measure_started_ns == u64::MAX {
        0
    } else {
        (bench_start.elapsed().as_nanos() as u64 - measure_started_ns) / 1000
    };
    let ops = if elapsed_us == 0 {
        0.0
    } else {
        (total_op as f64) * 1_000_000.0 / (elapsed_us as f64)
    };

    let ok_op = total_op.saturating_sub(err_op);

    let quantiles = Quantiles {
        p50_us: histogram_quantile_us(&merged_hist, 0.50),
        p95_us: histogram_quantile_us(&merged_hist, 0.95),
        p99_us: histogram_quantile_us(&merged_hist, 0.99),
        p999_us: histogram_quantile_us(&merged_hist, 0.999),
    };

    let workload_id = if accumulate {
        format!("MERGE_GET_{}", accumulate_per_key)
    } else {
        format!("MERGE_{}", (args.merge_ratio * 100.0).round() as u8)
    };
    let (read_pct, update_pct) = if accumulate {
        (100u8, 0u8)
    } else {
        let merge_pct = (args.merge_ratio * 100.0).round() as u8;
        (100u8.saturating_sub(merge_pct), merge_pct)
    };

    let row = ResultRow {
        engine: "mace",
        mode: "merge_get",
        durability_mode,
        threads: args.threads,
        key_size: args.key_size,
        value_size: VALUE_LEN,
        prefill_keys: args.num_keys,
        read_pct,
        update_pct,
        warmup_secs: args.warmup_secs,
        measure_secs: args.measure_secs,
        total_op,
        ok_op,
        err_op,
        ops,
        quantiles,
        elapsed_us,
    };

    if let Err(e) = append_result_row(&args.result_file, &row, &workload_id) {
        eprintln!("failed to write result file {}: {}", args.result_file, e);
        exit(1);
    }

    println!(
        "engine=mace workload={} mode=merge_get durability={} threads={} total_op={} ok_op={} err_op={} ops={:.2} p99_us={} result_file={}",
        workload_id,
        row.durability_mode.as_str(),
        row.threads,
        row.total_op,
        row.ok_op,
        row.err_op,
        row.ops,
        row.quantiles.p99_us,
        args.result_file
    );

    drop(db);
}
