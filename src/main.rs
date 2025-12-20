use clap::Parser;
#[cfg(target_os = "linux")]
use logger::Logger;
use mace::{Mace, Options};
#[cfg(feature = "custom_alloc")]
use myalloc::{MyAlloc, print_filtered_trace};
use rand::prelude::*;
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

    if args.key_size < 16 || args.value_size < 16 {
        eprintln!("Error: key_size or value_size too small, must >= 16");
        exit(1);
    }

    if args.insert_ratio > 100 {
        eprintln!("Error: Insert ratio must be between 0 and 100");
        exit(1);
    }

    let mut keys: Vec<Vec<Vec<u8>>> = Vec::with_capacity(args.threads);
    let mut opt = Options::new(path);
    opt.sync_on_write = false;
    opt.over_provision = true; // large value will use lots of memeory
    opt.inline_size = args.blob_size;
    opt.tmp_store = args.mode != "get" && args.mode != "scan";
    let mut saved = opt.clone();
    saved.tmp_store = false;
    let mut db = Mace::new(opt.validate().unwrap()).unwrap();
    db.disable_gc();

    let mut rng = rand::rng();
    let value = Arc::new(vec![b'0'; args.value_size]);
    let keys_per_thread = args.iterations / args.threads;
    for tid in 0..args.threads {
        let mut tk = Vec::with_capacity(keys_per_thread);
        for i in 0..keys_per_thread {
            let mut key = format!("key_{tid}_{i}").into_bytes();
            key.resize(args.key_size, b'x');
            tk.push(key);
        }
        if args.random || args.mode == "get" {
            tk.shuffle(&mut rng);
        }
        keys.push(tk);
    }

    if args.mode == "get" || args.mode == "scan" {
        let pre_tx = db.begin().unwrap();
        (0..args.threads).for_each(|tid| {
            for k in &keys[tid] {
                pre_tx.put(k, &*value).unwrap();
            }
        });
        pre_tx.commit().unwrap();
        drop(db);
        // re-open db
        saved.tmp_store = true;
        db = Mace::new(saved.validate().unwrap()).unwrap();

        // simulate common use cases
        for i in 0..keys_per_thread {
            let tid = rng.random_range(0..args.threads);
            let mut k = format!("key_{tid}_{i}").into_bytes();
            k.resize(args.key_size, b'x');
            let view = db.view().unwrap();
            view.get(&k).unwrap();
        }
    }

    let barrier = Arc::new(std::sync::Barrier::new(args.threads));
    let total_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let start_time = Arc::new(std::sync::Mutex::new(Instant::now()));

    let h: Vec<JoinHandle<()>> = (0..args.threads)
        .map(|tid| {
            let db = db.clone();
            let tk: &Vec<Vec<u8>> = unsafe { std::mem::transmute(&keys[tid]) };
            let total_ops = total_ops.clone();
            let barrier = Arc::clone(&barrier);
            let mode = args.mode.clone();
            let insert_ratio = args.insert_ratio;
            let st = start_time.clone();
            let val = value.clone();
            let prefix = format!("key_{tid}");

            std::thread::spawn(move || {
                // coreid::bind_core(tid);
                let mut round = 0;
                barrier.wait();

                {
                    if let Ok(mut guard) = st.try_lock() {
                        *guard = Instant::now();
                    }
                }
                match mode.as_str() {
                    "insert" => {
                        for key in tk {
                            round += 1;
                            let tx = db.begin().unwrap();
                            tx.put(key.as_slice(), val.as_slice()).unwrap();
                            tx.commit().unwrap();
                        }
                    }
                    "get" => {
                        for key in tk {
                            round += 1;
                            let tx = db.view().unwrap();
                            let x = tx.get(key).unwrap();
                            std::hint::black_box(x);
                        }
                    }
                    "mixed" => {
                        for key in tk {
                            let is_insert = rand::random_range(0..100) < insert_ratio;
                            round += 1;

                            if is_insert {
                                let tx = db.begin().unwrap();
                                tx.put(key, &*val).unwrap();
                                tx.commit().unwrap();
                            } else {
                                let tx = db.view().unwrap();
                                let x = tx.get(key); // not found
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

    for x in h {
        x.join().unwrap();
    }

    let test_start = start_time.lock().unwrap();
    let duration = test_start.elapsed();
    let total = total_ops.load(std::sync::atomic::Ordering::Relaxed);
    let ops = (total as f64 / duration.as_secs_f64()) as usize;

    let ratio = if args.mode == "mixed" {
        args.insert_ratio
    } else if args.mode == "insert" {
        100
    } else {
        0
    };
    eprintln!(
        "{},{},{},{},{},{},{}",
        args.mode,
        args.threads,
        args.key_size,
        args.value_size,
        ratio,
        ops,
        duration.as_millis()
    );
    drop(db);
    #[cfg(feature = "custom_alloc")]
    print_filtered_trace(|x, y| log::info!("{}{}", x, y));
}
