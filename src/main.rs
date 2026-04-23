use clap::{ArgAction, Parser};
#[cfg(target_os = "linux")]
use logger::Logger;
use mace::{Mace, Options};
#[cfg(feature = "custom_alloc")]
use myalloc::{MyAlloc, print_filtered_trace};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::exit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "custom_alloc")]
#[global_allocator]
static GLOBAL: MyAlloc = MyAlloc;

const LAT_BUCKETS: usize = 64;
const PREFIX_GROUPS: usize = 1024;
const PREFILL_BATCH: usize = 1024;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'p', long)]
    path: String,

    #[arg(short = 'm', long, default_value = "insert")]
    mode: String,

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

    #[arg(long, default_value_t = false)]
    random: bool,

    #[arg(long, default_value_t = 8192)]
    blob_size: usize,

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
    insert_only: bool,
}

#[derive(Clone, Copy, Debug)]
struct ThreadRange {
    start: usize,
    len: usize,
}

#[derive(Clone, Debug)]
struct MachineMeta {
    host: String,
    os: String,
    arch: String,
    kernel: String,
    cpu_cores: usize,
    mem_total_kb: u64,
    mem_available_kb: u64,
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
    ts_epoch_ms: u128,
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
    meta: MachineMeta,
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
    if let Some(w) = args.workload.as_ref() {
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
                insert_only: false,
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
                insert_only: false,
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
                insert_only: false,
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
                insert_only: false,
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
                insert_only: false,
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
                insert_only: false,
            },
            _ => {
                return Err(format!(
                    "invalid workload `{}` (supported: W1, W2, W3, W4, W5, W6)",
                    w
                ));
            }
        };
        return Ok(spec);
    }

    let mode = args.mode.trim().to_ascii_lowercase();
    match mode.as_str() {
        "insert" => Ok(WorkloadSpec {
            id: "LEGACY_INSERT".into(),
            mode_label: "insert".into(),
            distribution: Distribution::Uniform,
            read_pct: 0,
            update_pct: 100,
            scan_pct: 0,
            scan_len: args.scan_len,
            requires_prefill: false,
            insert_only: true,
        }),
        "get" => Ok(WorkloadSpec {
            id: "LEGACY_GET".into(),
            mode_label: "get".into(),
            distribution: Distribution::Uniform,
            read_pct: 100,
            update_pct: 0,
            scan_pct: 0,
            scan_len: args.scan_len,
            requires_prefill: true,
            insert_only: false,
        }),
        "scan" => Ok(WorkloadSpec {
            id: "LEGACY_SCAN".into(),
            mode_label: "scan".into(),
            distribution: Distribution::Uniform,
            read_pct: 0,
            update_pct: 0,
            scan_pct: 100,
            scan_len: args.scan_len,
            requires_prefill: true,
            insert_only: false,
        }),
        _ => Err(format!(
            "invalid mode `{}` (supported: insert, get, scan)",
            args.mode
        )),
    }
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

fn read_proc_value_kb(key: &str) -> u64 {
    let Ok(content) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            let num = rest
                .split_whitespace()
                .next()
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);
            return num;
        }
    }
    0
}

fn gather_machine_meta() -> MachineMeta {
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".to_string());

    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    MachineMeta {
        host,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        kernel,
        cpu_cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        mem_total_kb: read_proc_value_kb("MemTotal:"),
        mem_available_kb: read_proc_value_kb("MemAvailable:"),
    }
}

fn csv_escape(raw: &str) -> String {
    raw.replace([',', '\n', '\r'], " ")
}

fn result_header() -> &'static str {
    "schema_version,ts_epoch_ms,engine,workload_id,mode,durability_mode,threads,key_size,value_size,prefill_keys,shared_keyspace,distribution,zipf_theta,read_pct,update_pct,scan_pct,scan_len,read_path,warmup_secs,measure_secs,total_op,ok_op,err_op,ops,p50_us,p95_us,p99_us,p999_us,elapsed_us,host,os,arch,kernel,cpu_cores,mem_total_kb,mem_available_kb"
}

fn result_row_csv(row: &ResultRow) -> String {
    format!(
        "v2,{},{},{},{},{},{},{},{},{},{},{},{:.4},{},{},{},{},{},{},{},{},{},{},{:.3},{},{},{},{},{},{},{},{},{},{},{},{}",
        row.ts_epoch_ms,
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
        row.ops,
        row.quantiles.p50_us,
        row.quantiles.p95_us,
        row.quantiles.p99_us,
        row.quantiles.p999_us,
        row.elapsed_us,
        csv_escape(&row.meta.host),
        csv_escape(&row.meta.os),
        csv_escape(&row.meta.arch),
        csv_escape(&row.meta.kernel),
        row.meta.cpu_cores,
        row.meta.mem_total_kb,
        row.meta.mem_available_kb,
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
    if spec.insert_only {
        return OpKind::Update;
    }
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

fn main() {
    #[cfg(target_os = "linux")]
    {
        Logger::init().add_file("kv_bench.log", true);
        log::set_max_level(log::LevelFilter::Error);
    }

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
    let legacy_mode = workload.id.starts_with("LEGACY_");
    let effective_warmup_secs = if legacy_mode { 0 } else { args.warmup_secs };
    let effective_measure_secs = if legacy_mode { 0 } else { args.measure_secs };

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

    let mut opt = Options::new(path);
    opt.sync_on_write = durability_mode == DurabilityMode::Durable;
    opt.inline_size = args.blob_size;
    opt.checkpoint_size = 128 << 20;
    opt.cache_capacity = 1 << 30;
    opt.lru_capacity = 1 << 30;
    opt.pool_capacity = 1 << 30;
    opt.enable_backpressure = true;
    opt.gc_timeout = 5 * 1000;
    opt.gc_eager = false;
    opt.data_garbage_ratio = 50;
    opt.tmp_store = cleanup;

    let db = Mace::new(opt.validate().unwrap()).unwrap();
    db.disable_gc();
    let bkt = if args.reuse_path {
        db.get_bucket("default")
            .or_else(|_| db.new_bucket("default"))
            .unwrap()
    } else {
        db.new_bucket("default").unwrap()
    };
    let value = Arc::new(vec![b'0'; args.value_size]);

    if workload.requires_prefill && !args.skip_prefill {
        let mut fill_handles = Vec::with_capacity(thread_prefill_ranges.len());
        for (tid, tr) in thread_prefill_ranges.iter().copied().enumerate() {
            let bucket = bkt.clone();
            let v = value.clone();
            let key_size = args.key_size;
            let shared = shared_keyspace;
            fill_handles.push(std::thread::spawn(move || {
                coreid::bind_core(tid);
                let mut in_batch = 0usize;
                let mut tx = bucket.begin().unwrap();
                for i in 0..tr.len {
                    let key = if shared {
                        make_shared_key(tr.start + i, key_size)
                    } else {
                        make_thread_key(tid, i, key_size)
                    };
                    tx.upsert(key.as_slice(), v.as_slice()).unwrap();
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

    if workload_runs_gc(&workload) {
        db.enable_gc();
    }

    let op_counts = split_ranges(args.iterations, args.threads);
    let ready_barrier = Arc::new(Barrier::new(args.threads + 1));
    let measure_barrier = Arc::new(Barrier::new(args.threads + 1));
    let measure_start = Arc::new(Mutex::new(None::<Instant>));
    let insert_counter = Arc::new(AtomicUsize::new(0));

    let handles: Vec<JoinHandle<ThreadStats>> = (0..args.threads)
        .map(|tid| {
            let bucket = bkt.clone();
            let v = value.clone();
            let spec = workload.clone();
            let ready = Arc::clone(&ready_barrier);
            let measure = Arc::clone(&measure_barrier);
            let measure_start_slot = Arc::clone(&measure_start);
            let ins_ctr = Arc::clone(&insert_counter);
            let key_size = args.key_size;
            let random_insert = args.random;
            let read_path_mode = read_path;
            let warmup_secs = effective_warmup_secs;
            let measure_secs = effective_measure_secs;
            let distribution = spec.distribution;
            let zipf_theta = args.zipf_theta;
            let scan_len = spec.scan_len;
            let shared = shared_keyspace;
            let prefill_key_count = prefill_keys;
            let local_key_len = thread_prefill_ranges[tid].len;
            let local_op_start = op_counts[tid].start;
            let local_op_count = op_counts[tid].len;

            std::thread::spawn(move || {
                coreid::bind_core(tid);
                let seed = (now_epoch_ms() as u64)
                    ^ (tid as u64)
                        .wrapping_add(1)
                        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    ^ (prefill_key_count as u64).wrapping_shl(7);
                let mut rng = StdRng::seed_from_u64(seed);
                let mut stats = ThreadStats::default();
                let mut local_insert_idx = 0usize;

                let mut count_indices: Vec<usize> = (0..local_op_count).collect();
                if random_insert && spec.insert_only {
                    count_indices.shuffle(&mut rng);
                }

                ready.wait();

                if warmup_secs > 0 {
                    let deadline = Instant::now() + Duration::from_secs(warmup_secs);
                    while Instant::now() < deadline {
                        let op = pick_op_kind(&mut rng, &spec);
                        run_one_op(
                            op,
                            &bucket,
                            &v,
                            &mut rng,
                            &spec,
                            distribution,
                            zipf_theta,
                            read_path_mode,
                            key_size,
                            scan_len,
                            shared,
                            prefill_key_count,
                            local_key_len,
                            tid,
                            &ins_ctr,
                            &mut local_insert_idx,
                            None,
                            None,
                        );
                    }
                }

                measure.wait();
                {
                    let now = Instant::now();
                    let mut slot = measure_start_slot.lock().unwrap();
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
                            &bucket,
                            &v,
                            &mut rng,
                            &spec,
                            distribution,
                            zipf_theta,
                            read_path_mode,
                            key_size,
                            scan_len,
                            shared,
                            prefill_key_count,
                            local_key_len,
                            tid,
                            &ins_ctr,
                            &mut local_insert_idx,
                            None,
                            Some(&mut stats),
                        );
                    }
                } else {
                    for idx in count_indices {
                        let fixed_insert_id = if spec.insert_only {
                            Some(if shared { local_op_start + idx } else { idx })
                        } else {
                            None
                        };
                        let op = if spec.insert_only {
                            OpKind::Update
                        } else {
                            pick_op_kind(&mut rng, &spec)
                        };
                        run_one_op(
                            op,
                            &bucket,
                            &v,
                            &mut rng,
                            &spec,
                            distribution,
                            zipf_theta,
                            read_path_mode,
                            key_size,
                            scan_len,
                            shared,
                            prefill_key_count,
                            local_key_len,
                            tid,
                            &ins_ctr,
                            &mut local_insert_idx,
                            fixed_insert_id,
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
    {
        let now = Instant::now();
        let mut slot = measure_start.lock().unwrap();
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
    let measure_started = (*measure_start.lock().unwrap()).unwrap_or(measure_end);
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
        ts_epoch_ms: now_epoch_ms(),
        engine: "mace",
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
        warmup_secs: effective_warmup_secs,
        measure_secs: effective_measure_secs,
        total_op,
        ok_op,
        err_op,
        ops,
        quantiles,
        elapsed_us,
        meta: gather_machine_meta(),
    };

    if let Err(e) = append_result_row(&args.result_file, &row) {
        eprintln!("failed to write result file {}: {}", args.result_file, e);
        exit(1);
    }

    println!(
        "engine=mace workload={} mode={} durability={} threads={} total_op={} ok_op={} err_op={} ops={:.2} p99_us={} result_file={}",
        row.workload_id,
        row.mode,
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
    #[cfg(feature = "custom_alloc")]
    print_filtered_trace(|x, y| log::info!("{}{}", x, y));
}

#[allow(clippy::too_many_arguments)]
fn run_one_op(
    op: OpKind,
    bucket: &mace::Bucket,
    value: &Arc<Vec<u8>>,
    rng: &mut StdRng,
    spec: &WorkloadSpec,
    distribution: Distribution,
    zipf_theta: f64,
    read_path: ReadPath,
    key_size: usize,
    scan_len: usize,
    shared_keyspace: bool,
    prefill_keys: usize,
    local_key_len: usize,
    tid: usize,
    insert_counter: &AtomicUsize,
    local_insert_idx: &mut usize,
    fixed_insert_id: Option<usize>,
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
                    ReadPath::Snapshot => {
                        bucket.view().ok().and_then(|tx| tx.get(key).ok()).is_some()
                    }
                    ReadPath::RwTxn => {
                        if let Ok(tx) = bucket.begin() {
                            let get_ok = tx.get(key).is_ok();
                            let commit_ok = tx.commit().is_ok();
                            get_ok && commit_ok
                        } else {
                            false
                        }
                    }
                }
            } else {
                false
            }
        }
        OpKind::Update => {
            let key_opt = if spec.insert_only {
                if let Some(id) = fixed_insert_id {
                    if shared_keyspace {
                        Some(make_shared_key(id, key_size))
                    } else {
                        Some(make_thread_key(tid, id, key_size))
                    }
                } else if shared_keyspace {
                    let id = insert_counter.fetch_add(1, Ordering::Relaxed);
                    Some(make_shared_key(id, key_size))
                } else {
                    let id = *local_insert_idx;
                    *local_insert_idx += 1;
                    Some(make_thread_key(tid, id, key_size))
                }
            } else {
                let maybe_id = pick_key_id(
                    rng,
                    distribution,
                    zipf_theta,
                    shared_keyspace,
                    prefill_keys,
                    local_key_len,
                );
                if let Some(id) = maybe_id {
                    if shared_keyspace {
                        Some(make_shared_key(id, key_size))
                    } else {
                        Some(make_thread_key(tid, id, key_size))
                    }
                } else {
                    None
                }
            };

            if let Some(key) = key_opt {
                if let Ok(tx) = bucket.begin() {
                    let write_ok = if spec.insert_only {
                        tx.upsert(key.as_slice(), value.as_slice()).is_ok()
                    } else {
                        tx.update(key.as_slice(), value.as_slice()).is_ok()
                    };
                    if !write_ok {
                        false
                    } else {
                        tx.commit().is_ok()
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }
        OpKind::Scan => {
            let prefix_opt = if shared_keyspace {
                let maybe_id = pick_key_id(
                    rng,
                    distribution,
                    zipf_theta,
                    true,
                    prefill_keys,
                    local_key_len,
                );
                maybe_id.map(make_shared_prefix)
            } else {
                Some(make_thread_prefix(tid))
            };

            if let Some(prefix) = prefix_opt {
                match read_path {
                    ReadPath::Snapshot => {
                        if let Ok(view) = bucket.view() {
                            for item in view.seek(prefix).take(scan_len.max(1)) {
                                std::hint::black_box(item);
                            }
                            true
                        } else {
                            false
                        }
                    }
                    ReadPath::RwTxn => {
                        if let Ok(tx) = bucket.begin() {
                            for item in tx.seek(prefix).take(scan_len.max(1)) {
                                std::hint::black_box(item);
                            }
                            tx.commit().is_ok()
                        } else {
                            false
                        }
                    }
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
