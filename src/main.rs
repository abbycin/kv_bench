use clap::Parser;
#[cfg(target_os = "linux")]
use logger::Logger;
use mace::{Mace, Options};
#[cfg(feature = "custom_alloc")]
use myalloc::{MyAlloc, print_filtered_trace};
use std::path::Path;
use std::process::exit;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

#[cfg(feature = "custom_alloc")]
#[global_allocator]
static GLOBAL: MyAlloc = MyAlloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'p', long, default_value = "/tmp/mace")]
    path: String,

    #[arg(short = 'm', long, default_value = "insert")]
    mode: String,

    #[arg(short = 'k', long, default_value = "16")]
    key_size: usize,

    #[arg(short = 'v', long, default_value = "1024")]
    value_size: usize,

    #[arg(short = 't', long, default_value = "4")]
    threads: usize,

    #[arg(short = 'i', long, default_value = "10000")]
    iterations: usize,

    #[arg(short = 'r', long, default_value = "30")]
    insert_ratio: u8,

    #[arg(long, default_value = "false")]
    random: bool,

    #[arg(long, default_value = "8192")]
    blob_size: usize,
}

fn main() {
    #[cfg(target_os = "linux")]
    {
        Logger::init().add_file("/tmp/x.log", true);
        log::set_max_level(log::LevelFilter::Info);
    }
    let args = Args::parse();

    let path = Path::new(&args.path);

    if args.path.is_empty() {
        eprintln!("path is empty");
        exit(1);
    }

    if path.exists() {
        eprintln!("path {:?} already exists", args.path);
        exit(1);
    }

    if args.threads == 0 {
        eprintln!("Error: threads must be greater than 0");
        exit(1);
    }

    if !matches!(args.mode.as_str(), "insert" | "get" | "mixed" | "scan") {
        eprintln!("Error: Invalid mode");
        exit(1);
    }

    if args.key_size < 16 || args.value_size < 16 {
        eprintln!("Error: key_size or value_size too small, must >= 16");
        exit(1);
    }

    if args.insert_ratio > 100 {
        eprintln!("Error: Insert ratio must be between 0 and 100");
        exit(1);
    }
    let mut opt = Options::new(path);
    opt.sync_on_write = false;
    opt.inline_size = args.blob_size;
    opt.tmp_store = args.mode != "get" && args.mode != "scan";
    opt.cache_capacity = 3 << 30;
    opt.data_file_size = 64 << 20;
    opt.max_log_size = 1 << 30;
    opt.default_arenas = 128;
    let mut saved = opt.clone();

    saved.tmp_store = true;
    let mut db = Mace::new(opt.validate().unwrap()).unwrap();
    db.disable_gc();
    let mut bkt = db.new_bucket("default").unwrap();

    let value = Arc::new(vec![b'0'; args.value_size]);

    if args.mode == "get" || args.mode == "scan" {
        let mut fill_handles = vec![];
        for tid in 0..args.threads {
            let bkt_clone = bkt.clone();
            let val_clone = value.clone();
            let key_count = args.iterations / args.threads
                + if tid < args.iterations % args.threads {
                    1
                } else {
                    0
                };
            let key_size = args.key_size;
            fill_handles.push(std::thread::spawn(move || {
                coreid::bind_core(tid);
                const BATCH_SIZE: usize = 10000;
                for i in (0..key_count).step_by(BATCH_SIZE) {
                    let tx = bkt_clone.begin().unwrap();
                    for j in 0..BATCH_SIZE {
                        if i + j >= key_count {
                            break;
                        }
                        let mut key = format!("key_{tid}_{}", i + j).into_bytes();
                        key.resize(key_size, b'x');
                        tx.put(&key, &*val_clone).unwrap();
                    }
                    tx.commit().unwrap();
                }
            }));
        }
        for h in fill_handles {
            h.join().unwrap();
        }
        drop(bkt);
        drop(db);
        db = Mace::new(saved.validate().unwrap()).unwrap();
        bkt = db.get_bucket("default").unwrap();
    }

    let mut key_counts = vec![args.iterations / args.threads; args.threads];
    for cnt in key_counts.iter_mut().take(args.iterations % args.threads) {
        *cnt += 1;
    }

    let ready_barrier = Arc::new(std::sync::Barrier::new(args.threads + 1));
    let start_barrier = Arc::new(std::sync::Barrier::new(args.threads + 1));
    let total_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let h: Vec<JoinHandle<()>> = (0..args.threads)
        .map(|tid| {
            let db = bkt.clone();
            let total_ops = total_ops.clone();
            let ready_barrier = Arc::clone(&ready_barrier);
            let start_barrier = Arc::clone(&start_barrier);
            let mode = args.mode.clone();
            let insert_ratio = args.insert_ratio;
            let val = value.clone();
            let key_count = key_counts[tid];
            let key_size = args.key_size;
            let prefix = format!("key_{tid}_");
            let is_random = args.random;

            std::thread::spawn(move || {
                coreid::bind_core(tid);
                let mut round = 0;
                let mut indices: Vec<usize> = (0..key_count).collect();
                if is_random {
                    use rand::seq::SliceRandom;
                    indices.shuffle(&mut rand::rng());
                }

                ready_barrier.wait();
                start_barrier.wait();
                match mode.as_str() {
                    "insert" => {
                        for i in indices {
                            let mut key = format!("key_{tid}_{i}").into_bytes();
                            key.resize(key_size, b'x');
                            round += 1;
                            let tx = db.begin().unwrap();
                            tx.put(key.as_slice(), val.as_slice()).unwrap();
                            tx.commit().unwrap();
                        }
                    }
                    "get" => {
                        for i in indices {
                            let mut key = format!("key_{tid}_{i}").into_bytes();
                            key.resize(key_size, b'x');
                            round += 1;
                            let tx = db.view().unwrap();
                            let x = tx.get(key).unwrap();
                            std::hint::black_box(x);
                        }
                    }
                    "mixed" => {
                        for i in indices {
                            let mut key = format!("key_{tid}_{i}").into_bytes();
                            key.resize(key_size, b'x');
                            let is_insert = rand::random_range(0..100) < insert_ratio;
                            round += 1;

                            if is_insert {
                                let tx = db.begin().unwrap();
                                tx.put(key, &*val).unwrap();
                                tx.commit().unwrap();
                            } else {
                                let tx = db.view().unwrap();
                                let x = tx.get(key);
                                let _ = std::hint::black_box(x);
                            }
                        }
                    }
                    "scan" => {
                        let view = db.view().unwrap();
                        let iter = view.seek(prefix);
                        for x in iter {
                            round += 1;
                            std::hint::black_box(x);
                        }
                    }
                    _ => panic!("Invalid mode"),
                }

                total_ops.fetch_add(round, std::sync::atomic::Ordering::Relaxed);
            })
        })
        .collect();

    ready_barrier.wait();
    let start_time = Instant::now();
    start_barrier.wait();

    for x in h {
        x.join().unwrap();
    }

    let elapsed_us = start_time.elapsed().as_micros() as u64;
    let total = total_ops.load(std::sync::atomic::Ordering::Relaxed);
    let ops = if elapsed_us == 0 {
        0
    } else {
        ((total as u128 * 1_000_000u128) / elapsed_us as u128) as usize
    };

    let ratio = if args.mode == "mixed" {
        args.insert_ratio
    } else if args.mode == "insert" {
        100
    } else {
        0
    };
    let mut mode = args.mode.clone();
    if mode == "insert" {
        if args.random {
            mode = "random_insert".into();
        } else {
            mode = "sequential_insert".into();
        }
    }
    println!(
        "{},{},{},{},{},{},{}",
        mode, args.threads, args.key_size, args.value_size, ratio, ops, elapsed_us
    );
    drop(db);
    #[cfg(feature = "custom_alloc")]
    print_filtered_trace(|x, y| log::info!("{}{}", x, y));
}
