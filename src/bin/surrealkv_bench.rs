//! surrealkv harness for the W1-W6 matrix.
//!
//! Same CLI, workloads, latency accounting and CSV schema as `mace_bench`,
//! `rocksdb_bench` and `fjall_bench`, but written against surrealkv's API.
//!
//! Two engine-specific facts this file has to absorb (see README):
//!
//! * `Transaction::commit` is `async` and the tree must be built inside a tokio
//!   runtime (surrealkv spawns its flusher and compaction tasks from
//!   `build()`), so one background runtime owns the tree and every worker
//!   thread drives its own single-threaded runtime. Per-op latency is still one
//!   `commit().await`, which returns once the commit is durable (if asked) and
//!   applied to the memtable.
//! * surrealkv 1.0.0 exposes no manual compaction API (`CompactionOperations`
//!   is implemented on a crate-private type and not re-exported), so the shared
//!   "one compaction pass after prefill" rule is applied as: flush and close,
//!   reopen (the startup level check runs there), then wait a fixed settle
//!   window (`--gc-settle-secs`) before warmup/measurement.

use clap::{ArgAction, Parser};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use surrealkv::{
    CompressionType, Durability, LSMIterator, Mode, Options, Transaction, Tree, TreeBuilder,
};

const LAT_BUCKETS: usize = 64;
const PREFIX_GROUPS: usize = 1024;
const PREFILL_BATCH: usize = 1024;

/// Worker threads of the runtime that owns the tree (its flusher, its
/// compaction tasks). Not used to drive measured work.
const BACKGROUND_RUNTIME_THREADS: usize = 2;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'p', long)]
    path: String,

    #[arg(long)]
    workload: Option<String>,

    #[arg(short = 'k', long, default_value_t = 16)]
    key_size: usize,

    #[arg(short = 'v', long, default_value_t = 1024)]
    value_size: usize,

    #[arg(short = 't', long, default_value_t = 4)]
    threads: usize,

    #[arg(short = 'i', long, default_value_t = 10000)]
    iterations: usize,

    /// Value separation (WiscKey) threshold, same axis as fjall's
    /// `KvSeparationOptions::separation_threshold` and mace's `--blob-size`.
    /// surrealkv's own default is VLog off; 2048 keeps the axis aligned with
    /// the other harnesses. 0 disables value separation.
    #[arg(long, default_value_t = 2048)]
    blob_size: usize,

    #[arg(long, action = ArgAction::SetTrue)]
    compression: bool,

    #[arg(long, default_value_t = true)]
    shared_keyspace: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    no_shared_keyspace: bool,

    #[arg(long, default_value_t = 0)]
    prefill_keys: usize,

    #[arg(long, default_value_t = 0)]
    warmup_secs: u64,

    #[arg(long, default_value_t = 0)]
    measure_secs: u64,

    #[arg(long, default_value_t = 100)]
    scan_len: usize,

    #[arg(long, default_value_t = 0.99)]
    zipf_theta: f64,

    #[arg(long, default_value = "snapshot")]
    read_path: String,

    #[arg(long, default_value = "relaxed")]
    durability: String,

    /// Seconds to wait for background compaction after the post-prefill close
    /// and reopen, in place of the manual compaction pass the other engines
    /// offer. 0 skips the wait (results then include prefill compaction).
    #[arg(long, default_value_t = 30)]
    gc_settle_secs: u64,

    #[arg(long, default_value = "benchmark_results.csv")]
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
enum Distribution {
    Uniform,
    Zipf,
}

impl Distribution {
    fn as_str(self) -> &'static str {
        match self {
            Distribution::Uniform => "uniform",
            Distribution::Zipf => "zipf",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadPath {
    Snapshot,
    RwTxn,
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

    /// surrealkv's per-commit durability: `Immediate` fsyncs before commit
    /// returns, `Eventual` leaves the WAL in the OS page cache.
    fn engine(self) -> Durability {
        match self {
            DurabilityMode::Relaxed => Durability::Eventual,
            DurabilityMode::Durable => Durability::Immediate,
        }
    }
}

impl ReadPath {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "snapshot" => Some(Self::Snapshot),
            "rw_txn" | "rwtxn" | "txn" => Some(Self::RwTxn),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            ReadPath::Snapshot => "snapshot",
            ReadPath::RwTxn => "rw_txn",
        }
    }
}

#[derive(Clone, Debug)]
struct WorkloadSpec {
    id: String,
    mode_label: String,
    distribution: Distribution,
    read_pct: u8,
    update_pct: u8,
    scan_pct: u8,
    scan_len: usize,
    requires_prefill: bool,
}

#[derive(Clone, Copy, Debug)]
struct ThreadRange {
    start: usize,
    len: usize,
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
    workload_id: String,
    mode: String,
    durability_mode: DurabilityMode,
    threads: usize,
    key_size: usize,
    value_size: usize,
    prefill_keys: usize,
    shared_keyspace: bool,
    distribution: Distribution,
    zipf_theta: f64,
    read_pct: u8,
    update_pct: u8,
    scan_pct: u8,
    scan_len: usize,
    read_path: ReadPath,
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

fn parse_workload(args: &Args) -> Result<WorkloadSpec, String> {
    let w = match args.workload.as_ref() {
        Some(w) => w,
        None => return Err("missing --workload (supported: W1, W2, W3, W4, W5, W6)".into()),
    };
    let id = w.trim().to_ascii_uppercase();
    let spec = match id.as_str() {
        "W1" => WorkloadSpec {
            id,
            mode_label: "mixed".into(),
            distribution: Distribution::Uniform,
            read_pct: 95,
            update_pct: 5,
            scan_pct: 0,
            scan_len: args.scan_len,
            requires_prefill: true,
        },
        "W2" => WorkloadSpec {
            id,
            mode_label: "mixed".into(),
            distribution: Distribution::Zipf,
            read_pct: 95,
            update_pct: 5,
            scan_pct: 0,
            scan_len: args.scan_len,
            requires_prefill: true,
        },
        "W3" => WorkloadSpec {
            id,
            mode_label: "mixed".into(),
            distribution: Distribution::Uniform,
            read_pct: 50,
            update_pct: 50,
            scan_pct: 0,
            scan_len: args.scan_len,
            requires_prefill: true,
        },
        "W4" => WorkloadSpec {
            id,
            mode_label: "mixed".into(),
            distribution: Distribution::Uniform,
            read_pct: 5,
            update_pct: 95,
            scan_pct: 0,
            scan_len: args.scan_len,
            requires_prefill: true,
        },
        "W5" => WorkloadSpec {
            id,
            mode_label: "mixed".into(),
            distribution: Distribution::Uniform,
            read_pct: 70,
            update_pct: 25,
            scan_pct: 5,
            scan_len: args.scan_len,
            requires_prefill: true,
        },
        "W6" => WorkloadSpec {
            id,
            mode_label: "scan".into(),
            distribution: Distribution::Uniform,
            read_pct: 0,
            update_pct: 0,
            scan_pct: 100,
            scan_len: args.scan_len,
            requires_prefill: true,
        },
        _ => {
            return Err(format!(
                "invalid workload `{}` (supported: W1, W2, W3, W4, W5, W6)",
                w
            ));
        }
    };
    Ok(spec)
}

fn workload_runs_gc(spec: &WorkloadSpec) -> bool {
    spec.requires_prefill
}

fn split_ranges(total: usize, n: usize) -> Vec<ThreadRange> {
    let mut ranges = Vec::with_capacity(n);
    if n == 0 {
        return ranges;
    }
    let base = total / n;
    let rem = total % n;
    let mut start = 0;
    for tid in 0..n {
        let len = base + usize::from(tid < rem);
        ranges.push(ThreadRange { start, len });
        start += len;
    }
    ranges
}

fn make_shared_key(id: usize, key_size: usize) -> Vec<u8> {
    let group = id % PREFIX_GROUPS;
    let mut key = format!("s{:03x}_{:010x}", group, id).into_bytes();
    if key.len() < key_size {
        key.resize(key_size, b'x');
    }
    key
}

fn make_thread_key(tid: usize, local_id: usize, key_size: usize) -> Vec<u8> {
    let mut key = format!("t{:03x}_{:010x}", tid % PREFIX_GROUPS, local_id).into_bytes();
    if key.len() < key_size {
        key.resize(key_size, b'x');
    }
    key
}

fn make_shared_prefix(key_id: usize) -> Vec<u8> {
    let group = key_id % PREFIX_GROUPS;
    format!("s{:03x}_", group).into_bytes()
}

fn make_thread_prefix(tid: usize) -> Vec<u8> {
    format!("t{:03x}_", tid % PREFIX_GROUPS).into_bytes()
}

/// Exclusive upper bound of `prefix`, so `range(prefix, upper)` walks exactly
/// the keys that start with `prefix`. Every prefix this harness builds ends in
/// `b'_'`, so the bound always exists.
fn prefix_upper(prefix: &[u8]) -> Vec<u8> {
    let mut upper = prefix.to_vec();
    let last = upper.last_mut().expect("prefix is never empty");
    assert!(*last < 0xff, "prefix ends in 0xff, which has no finite upper bound");
    *last += 1;
    upper
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

fn csv_escape(raw: &str) -> String {
    raw.replace([',', '\n', '\r'], " ")
}

fn result_header() -> &'static str {
    "engine,workload_id,mode,durability_mode,threads,key_size,value_size,prefill_keys,shared_keyspace,distribution,zipf_theta,read_pct,update_pct,scan_pct,scan_len,read_path,warmup_secs,measure_secs,total_op,ok_op,err_op,ops,p50_us,p95_us,p99_us,p999_us,elapsed_us"
}

fn result_row_csv(row: &ResultRow) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{:.4},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        row.engine,
        csv_escape(&row.workload_id),
        csv_escape(&row.mode),
        row.durability_mode.as_str(),
        row.threads,
        row.key_size,
        row.value_size,
        row.prefill_keys,
        row.shared_keyspace,
        row.distribution.as_str(),
        row.zipf_theta,
        row.read_pct,
        row.update_pct,
        row.scan_pct,
        row.scan_len,
        row.read_path.as_str(),
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

fn append_result_row(path: &str, row: &ResultRow) -> std::io::Result<()> {
    let exists = Path::new(path).exists();
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    if !exists {
        writer.write_all(result_header().as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.write_all(result_row_csv(row).as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn sample_zipf_like(rng: &mut StdRng, n: usize, theta: f64) -> usize {
    if n <= 1 {
        return 0;
    }
    let t = theta.clamp(0.0001, 0.9999);
    let u: f64 = rng.random();
    let scaled = u.powf(1.0 / (1.0 - t));
    let idx = (scaled * n as f64) as usize;
    idx.min(n - 1)
}

fn pick_key_id(
    rng: &mut StdRng,
    distribution: Distribution,
    theta: f64,
    shared: bool,
    prefill_keys: usize,
    local_len: usize,
) -> Option<usize> {
    if shared {
        if prefill_keys == 0 {
            return None;
        }
        return Some(match distribution {
            Distribution::Uniform => rng.random_range(0..prefill_keys),
            Distribution::Zipf => sample_zipf_like(rng, prefill_keys, theta),
        });
    }

    if local_len == 0 {
        return None;
    }

    Some(match distribution {
        Distribution::Uniform => rng.random_range(0..local_len),
        Distribution::Zipf => sample_zipf_like(rng, local_len, theta),
    })
}

#[derive(Clone, Copy)]
enum OpKind {
    Read,
    Update,
    Scan,
}

fn pick_op_kind(rng: &mut StdRng, spec: &WorkloadSpec) -> OpKind {
    if spec.scan_pct == 100 {
        return OpKind::Scan;
    }
    let roll: u8 = rng.random_range(0..100);
    if roll < spec.read_pct {
        OpKind::Read
    } else if roll < spec.read_pct.saturating_add(spec.update_pct) {
        OpKind::Update
    } else {
        OpKind::Scan
    }
}

fn background_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(BACKGROUND_RUNTIME_THREADS)
        .enable_time()
        .build()
        .expect("failed to build background tokio runtime")
}

fn worker_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build worker tokio runtime")
}

/// Fairness: same memory scale as the other harnesses (mace and fjall run a
/// 4 GiB cache, RocksDB a 5 GiB block cache), no compression unless explicitly
/// requested, and value separation at `--blob-size` like fjall's
/// `KvSeparationOptions`.
fn build_tree(path: &str, blob_size: usize, compression: bool) -> Tree {
    let mut opts = Options::default()
        .with_path(PathBuf::from(path))
        .with_block_cache_capacity(4 << 30)
        .with_enable_vlog(blob_size > 0)
        .with_vlog_value_threshold(blob_size.max(1));
    if compression {
        opts = opts.with_compression_per_level(vec![CompressionType::SnappyCompression]);
    } else {
        // fair comparison default: no compression on any level, as in the other
        // harnesses (surrealkv's own default is already uncompressed)
        opts = opts.without_compression();
    }

    // The tree is built, and its background tasks spawned, inside the runtime
    // that will keep polling them for the whole run.
    TreeBuilder::with_options(opts).build().unwrap_or_else(|e| {
        eprintln!("failed to open surrealkv tree at {}: {e}", path);
        exit(1);
    })
}

fn main() {
    let args = Args::parse();
    let path = Path::new(&args.path);
    let shared_keyspace = args.shared_keyspace && !args.no_shared_keyspace;
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
    if args.key_size < 16 || args.value_size < 16 {
        eprintln!("key_size and value_size must be >= 16");
        exit(1);
    }
    if !(0.0..1.0).contains(&args.zipf_theta) {
        eprintln!("zipf_theta must be in range (0, 1)");
        exit(1);
    }

    let read_path = match ReadPath::parse(&args.read_path) {
        Some(r) => r,
        None => {
            eprintln!(
                "invalid read_path `{}` (supported: snapshot, rw_txn)",
                args.read_path
            );
            exit(1);
        }
    };

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

    let workload = match parse_workload(&args) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("{}", e);
            exit(1);
        }
    };

    let mixed_workload = workload.read_pct > 0 && workload.update_pct > 0;
    if mixed_workload && !shared_keyspace {
        eprintln!("mixed workloads require shared keyspace");
        exit(1);
    }

    let prefill_keys = if workload.requires_prefill {
        if args.prefill_keys > 0 {
            args.prefill_keys
        } else {
            args.iterations.max(1)
        }
    } else {
        args.prefill_keys
    };

    if workload.requires_prefill && prefill_keys == 0 {
        eprintln!("prefill_keys must be > 0 for read/mixed/scan workloads");
        exit(1);
    }

    let thread_prefill_ranges = split_ranges(prefill_keys, args.threads);

    let blob_size = args.blob_size;
    let compression = args.compression;
    // The runtime that owns the tree's background tasks for the whole run. It
    // stays alive and keeps polling them while the worker threads drive their
    // own runtimes.
    let rt = background_runtime();
    let mut tree = Arc::new({
        let _entry = rt.enter();
        build_tree(&args.path, blob_size, compression)
    });
    let value = Arc::new(vec![b'0'; args.value_size]);

    if workload.requires_prefill && !args.skip_prefill {
        let mut fill_handles = Vec::with_capacity(thread_prefill_ranges.len());
        for (tid, tr) in thread_prefill_ranges.iter().copied().enumerate() {
            let tree = Arc::clone(&tree);
            let v = value.clone();
            let key_size = args.key_size;
            let shared = shared_keyspace;
            fill_handles.push(std::thread::spawn(move || {
                coreid::bind_core(tid);
                let rt = worker_runtime();
                rt.block_on(async move {
                    for chunk in (0..tr.len).collect::<Vec<_>>().chunks(PREFILL_BATCH) {
                        // prefill ranges are disjoint across threads, so no
                        // write conflict is expected; retry anyway to be safe
                        loop {
                            let mut tx = tree.begin().unwrap_or_else(|e| {
                                panic!("surrealkv prefill begin failed: {e}");
                            });
                            for &i in chunk {
                                let key = if shared {
                                    make_shared_key(tr.start + i, key_size)
                                } else {
                                    make_thread_key(tid, i, key_size)
                                };
                                tx.set(key.as_slice(), &v[..]).unwrap_or_else(|e| {
                                    panic!("surrealkv prefill set failed: {e}");
                                });
                            }
                            match tx.commit().await {
                                Ok(()) => break,
                                Err(_) => continue,
                            }
                        }
                    }
                });
            }));
        }
        for h in fill_handles {
            h.join().unwrap();
        }
    }

    if workload_runs_gc(&workload) {
        // surrealkv 1.0.0 has no manual compaction API, so the shared "one
        // compaction pass after prefill, before measurement" rule becomes:
        // flush and close (close flushes the memtable), reopen (the startup
        // level check runs there) and settle for background compaction.
        if let Err(e) = rt.block_on(async { tree.close().await }) {
            eprintln!("surrealkv close after prefill failed: {e}");
            exit(1);
        }
        tree = Arc::new({
            let _entry = rt.enter();
            build_tree(&args.path, blob_size, compression)
        });
        eprintln!(
            "surrealkv: prefill closed and reopened, settling {}s for background compaction",
            args.gc_settle_secs
        );
        if args.gc_settle_secs > 0 {
            std::thread::sleep(Duration::from_secs(args.gc_settle_secs));
        }
    }

    let op_counts = split_ranges(args.iterations, args.threads);
    let ready_barrier = Arc::new(Barrier::new(args.threads + 1));
    let measure_barrier = Arc::new(Barrier::new(args.threads + 1));
    let measure_start = Arc::new(Mutex::new(None::<Instant>));

    let handles: Vec<JoinHandle<ThreadStats>> = (0..args.threads)
        .map(|tid| {
            let tree = Arc::clone(&tree);
            let v = value.clone();
            let spec = workload.clone();
            let ready = Arc::clone(&ready_barrier);
            let measure = Arc::clone(&measure_barrier);
            let measure_start_slot = Arc::clone(&measure_start);
            let key_size = args.key_size;
            let read_path_mode = read_path;
            let durability = durability_mode;
            let warmup_secs = args.warmup_secs;
            let measure_secs = args.measure_secs;
            let distribution = spec.distribution;
            let zipf_theta = args.zipf_theta;
            let scan_len = spec.scan_len;
            let shared = shared_keyspace;
            let prefill_key_count = prefill_keys;
            let local_key_len = thread_prefill_ranges[tid].len;
            let local_op_count = op_counts[tid].len;

            std::thread::spawn(move || {
                coreid::bind_core(tid);
                let rt = worker_runtime();
                rt.block_on(async move {
                    let seed = (now_epoch_ms() as u64)
                        ^ (tid as u64)
                            .wrapping_add(1)
                            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                        ^ (prefill_key_count as u64).wrapping_shl(7);
                    let mut rng = StdRng::seed_from_u64(seed);
                    let mut stats = ThreadStats::default();

                    let count_indices: Vec<usize> = (0..local_op_count).collect();

                    ready.wait();

                    if warmup_secs > 0 {
                        let deadline = Instant::now() + Duration::from_secs(warmup_secs);
                        while Instant::now() < deadline {
                            let op = pick_op_kind(&mut rng, &spec);
                            run_one_op(
                                op,
                                &tree,
                                &v,
                                &mut rng,
                                distribution,
                                zipf_theta,
                                read_path_mode,
                                durability,
                                key_size,
                                scan_len,
                                shared,
                                prefill_key_count,
                                local_key_len,
                                tid,
                                None,
                            )
                            .await;
                        }
                    }

                    measure.wait();
                    {
                        let now = Instant::now();
                        let mut slot = measure_start_slot.lock();
                        if slot.map_or_else(|| true, |prev| now < prev) {
                            *slot = Some(now);
                        }
                    }

                    if measure_secs > 0 {
                        let deadline = Instant::now() + Duration::from_secs(measure_secs);
                        while Instant::now() < deadline {
                            let op = pick_op_kind(&mut rng, &spec);
                            run_one_op(
                                op,
                                &tree,
                                &v,
                                &mut rng,
                                distribution,
                                zipf_theta,
                                read_path_mode,
                                durability,
                                key_size,
                                scan_len,
                                shared,
                                prefill_key_count,
                                local_key_len,
                                tid,
                                Some(&mut stats),
                            )
                            .await;
                        }
                    } else {
                        for _ in count_indices {
                            let op = pick_op_kind(&mut rng, &spec);
                            run_one_op(
                                op,
                                &tree,
                                &v,
                                &mut rng,
                                distribution,
                                zipf_theta,
                                read_path_mode,
                                durability,
                                key_size,
                                scan_len,
                                shared,
                                prefill_key_count,
                                local_key_len,
                                tid,
                                Some(&mut stats),
                            )
                            .await;
                        }
                    }

                    stats
                })
            })
        })
        .collect();

    ready_barrier.wait();
    measure_barrier.wait();
    {
        let now = Instant::now();
        let mut slot = measure_start.lock();
        if slot.map_or_else(|| true, |prev| now < prev) {
            *slot = Some(now);
        }
    }

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

    let measure_end = Instant::now();
    let measure_started = (*measure_start.lock()).unwrap_or(measure_end);
    let elapsed_us = measure_end.duration_since(measure_started).as_micros() as u64;
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

    let row = ResultRow {
        engine: "surrealkv",
        workload_id: workload.id.clone(),
        mode: workload.mode_label.clone(),
        durability_mode,
        threads: args.threads,
        key_size: args.key_size,
        value_size: args.value_size,
        prefill_keys,
        shared_keyspace,
        distribution: workload.distribution,
        zipf_theta: args.zipf_theta,
        read_pct: workload.read_pct,
        update_pct: workload.update_pct,
        scan_pct: workload.scan_pct,
        scan_len: workload.scan_len,
        read_path,
        warmup_secs: args.warmup_secs,
        measure_secs: args.measure_secs,
        total_op,
        ok_op,
        err_op,
        ops,
        quantiles,
        elapsed_us,
    };

    if let Err(e) = append_result_row(&args.result_file, &row) {
        eprintln!("failed to write result file {}: {}", args.result_file, e);
        exit(1);
    }

    println!(
        "engine=surrealkv workload={} mode={} durability={} threads={} total_op={} ok_op={} err_op={} ops={:.2} p99_us={} gc_settle_secs={} result_file={}",
        row.workload_id,
        row.mode,
        row.durability_mode.as_str(),
        row.threads,
        row.total_op,
        row.ok_op,
        row.err_op,
        row.ops,
        row.quantiles.p99_us,
        args.gc_settle_secs,
        args.result_file
    );

    if let Err(e) = rt.block_on(async { tree.close().await }) {
        eprintln!("surrealkv close failed: {e}");
    }
    drop(tree);
    drop(rt);
    if cleanup {
        let _ = std::fs::remove_dir_all(&args.path);
    }
}

/// Scans up to `limit` keys of `[start, end)`, the same unit mace's
/// `seek(prefix)` and fjall's `prefix()` count (one scan request, not scanned
/// keys). A scan that finds nothing still counts as one successful request.
fn scan_prefix(tx: &Transaction, start: &[u8], end: &[u8], limit: usize) -> bool {
    let mut iter = match tx.range(start, end) {
        Ok(i) => i,
        Err(_) => return false,
    };
    match iter.seek_first() {
        Ok(true) => {}
        Ok(false) => return true,
        Err(_) => return false,
    }

    let mut seen = 0usize;
    loop {
        if !iter.valid() {
            break;
        }
        std::hint::black_box(iter.key().user_key());
        match iter.value() {
            Ok(v) => {
                std::hint::black_box(v);
            }
            Err(_) => return false,
        }
        seen += 1;
        if seen >= limit {
            break;
        }
        match iter.next() {
            Ok(true) => {}
            Ok(false) => break,
            Err(_) => return false,
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn run_one_op(
    op: OpKind,
    tree: &Tree,
    value: &Arc<Vec<u8>>,
    rng: &mut StdRng,
    distribution: Distribution,
    zipf_theta: f64,
    read_path: ReadPath,
    durability: DurabilityMode,
    key_size: usize,
    scan_len: usize,
    shared_keyspace: bool,
    prefill_keys: usize,
    local_key_len: usize,
    tid: usize,
    stats: Option<&mut ThreadStats>,
) {
    let start = stats.as_ref().map(|_| Instant::now());

    let ok = match op {
        OpKind::Read => {
            let maybe_id = pick_key_id(
                rng,
                distribution,
                zipf_theta,
                shared_keyspace,
                prefill_keys,
                local_key_len,
            );
            if let Some(id) = maybe_id {
                let key = if shared_keyspace {
                    make_shared_key(id, key_size)
                } else {
                    make_thread_key(tid, id, key_size)
                };
                match read_path {
                    // read-only txn = the snapshot read path of the other
                    // harnesses (mace view / fjall read_tx)
                    ReadPath::Snapshot => match tree.begin_with_mode(Mode::ReadOnly) {
                        Ok(tx) => matches!(tx.get(key.as_slice()), Ok(Some(_))),
                        Err(_) => false,
                    },
                    // rw txn that reads and commits, mirrors mace's
                    // begin + get + commit and fjall's write_tx + get + commit
                    ReadPath::RwTxn => match tree.begin() {
                        Ok(mut tx) => {
                            let get_ok = matches!(tx.get(key.as_slice()), Ok(Some(_)));
                            let commit_ok = tx.commit().await.is_ok();
                            get_ok && commit_ok
                        }
                        Err(_) => false,
                    },
                }
            } else {
                false
            }
        }
        OpKind::Update => {
            let maybe_id = pick_key_id(
                rng,
                distribution,
                zipf_theta,
                shared_keyspace,
                prefill_keys,
                local_key_len,
            );
            if let Some(id) = maybe_id {
                let key = if shared_keyspace {
                    make_shared_key(id, key_size)
                } else {
                    make_thread_key(tid, id, key_size)
                };
                match tree.begin() {
                    Ok(mut tx) => {
                        if durability == DurabilityMode::Durable {
                            tx.set_durability(durability.engine());
                        }
                        // read-then-write in one SI/OCC txn, mirrors rocksdb
                        // GetForUpdate + Put + Commit and fjall's read-then-write
                        // SSI txn; the read registers the key for commit-time
                        // conflict validation
                        let read_ok = matches!(tx.get_for_update(key.as_slice()), Ok(Some(_)));
                        if !read_ok {
                            false
                        } else if tx.set(key.as_slice(), &value[..]).is_err() {
                            false
                        } else {
                            tx.commit().await.is_ok()
                        }
                    }
                    Err(_) => false,
                }
            } else {
                false
            }
        }
        OpKind::Scan => {
            let bounds = if shared_keyspace {
                pick_key_id(
                    rng,
                    distribution,
                    zipf_theta,
                    true,
                    prefill_keys,
                    local_key_len,
                )
                .map(|id| {
                    let prefix = make_shared_prefix(id);
                    let upper = prefix_upper(&prefix);
                    (prefix, upper)
                })
            } else {
                let prefix = make_thread_prefix(tid);
                let upper = prefix_upper(&prefix);
                Some((prefix, upper))
            };

            if let Some((start_key, end_key)) = bounds {
                let limit = scan_len.max(1);
                match read_path {
                    ReadPath::Snapshot => match tree.begin_with_mode(Mode::ReadOnly) {
                        Ok(tx) => scan_prefix(&tx, &start_key, &end_key, limit),
                        Err(_) => false,
                    },
                    ReadPath::RwTxn => match tree.begin() {
                        Ok(mut tx) => {
                            if durability == DurabilityMode::Durable {
                                tx.set_durability(durability.engine());
                            }
                            let scan_ok = scan_prefix(&tx, &start_key, &end_key, limit);
                            scan_ok && tx.commit().await.is_ok()
                        }
                        Err(_) => false,
                    },
                }
            } else {
                false
            }
        }
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
