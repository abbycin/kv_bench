use clap::Parser;
use mace::{Mace, Options};
use rand::prelude::*;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

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

    #[arg(short = 'r', long, default_value = "50")]
    insert_ratio: u8,

    #[arg(long, default_value = "false")]
    random: bool,
}

fn main() {
    let args = Args::parse();

    let path = Path::new(&args.path);

    if args.path.is_empty() {
        eprintln!("path is empty");
        return;
    }

    if path.exists() {
        eprintln!("path {:?} already exists", args.path);
        return;
    }

    if args.key_size < 16 || args.value_size < 16 {
        eprintln!("Error: key_size or value_size too small, must >= 16");
        return;
    }

    if args.insert_ratio > 100 {
        eprintln!("Error: Insert ratio must be between 0 and 100");
        return;
    }

    let mut opt = Options::new(path);
    opt.sync_on_write = false;
    opt.tmp_store = true;
    // currently we don't have prefix encode, so enlarge the inline size to avoid indirection
    opt.max_inline_size = 4096;
    let db = Mace::new(opt.validate().unwrap()).unwrap();

    let value = Arc::new(vec![b'0'; args.value_size]);
    if args.mode == "get" {
        for tid in 0..args.threads {
            for i in 0..args.iterations {
                let key = format!("key_{tid}_{i}");
                let mut tmp = key.into_bytes();
                tmp.resize(args.key_size, b'x');
                let pre_tx = db.begin().unwrap();
                pre_tx.put(&tmp, &*value).unwrap();
                pre_tx.commit().unwrap();
            }
        }
    }

    let barrier = Arc::new(std::sync::Barrier::new(args.threads));
    let total_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let start_time = Arc::new(std::sync::Mutex::new(Instant::now()));

    let h: Vec<JoinHandle<()>> = (0..args.threads)
        .map(|tid| {
            let db = db.clone();
            let total_ops = total_ops.clone();
            let barrier = Arc::clone(&barrier);
            let mode = args.mode.clone();
            let insert_ratio = args.insert_ratio;
            let st = start_time.clone();
            let val = value.clone();

            std::thread::spawn(move || {
                let mut keys: Vec<Vec<u8>> = Vec::with_capacity(args.iterations);
                for i in 0..args.iterations {
                    let key = format!("key_{tid}_{i}");
                    let mut tmp = key.into_bytes();
                    tmp.resize(args.key_size, b'x');
                    keys.push(tmp);
                }

                let mut rng = rand::rng();
                if args.random {
                    keys.shuffle(&mut rng);
                }
                barrier.wait();

                {
                    if let Ok(mut guard) = st.try_lock() {
                        *guard = Instant::now();
                    }
                }

                match mode.as_str() {
                    "insert" => {
                        for key in &keys {
                            let tx = db.begin().unwrap();
                            tx.put(key.as_slice(), val.as_slice()).unwrap();
                            tx.commit().unwrap();
                        }
                    }
                    "get" => {
                        for key in &keys {
                            let tx = db.view().unwrap();
                            tx.get(key).unwrap();
                        }
                    }
                    "mixed" => {
                        for key in &keys {
                            let is_insert = rng.random_range(0..100) < insert_ratio;

                            if is_insert {
                                let tx = db.begin().unwrap();
                                tx.put(key, &*val).unwrap();
                                tx.commit().unwrap();
                            } else {
                                let tx = db.view().unwrap();
                                let _ = tx.get(key); // may not insert
                            }
                        }
                    }
                    _ => panic!("Invalid mode"),
                }

                total_ops.fetch_add(args.iterations, std::sync::atomic::Ordering::Relaxed);
            })
        })
        .collect();

    for x in h {
        x.join().unwrap();
    }

    let test_start = start_time.lock().unwrap();
    let duration = test_start.elapsed();
    let total = total_ops.load(std::sync::atomic::Ordering::Relaxed);
    let ops = total as f64 / duration.as_secs_f64();

    // println!("{:<20} {}", "Test Mode:", args.mode);
    // println!("{:<20} {}", "Threads:", args.threads);
    // println!("{:<20} {total}", "Total Ops:");
    // println!("{:<20} {:.2}s", "Duration:", duration.as_secs_f64());
    // println!("{:<20} {ops:.2}", "OPS:");
    // println!("{:<20} {}B", "Key Size:", args.key_size);
    // println!("{:<20} {}B", "Value Size:", args.value_size);

    // if args.mode == "mixed" {
    //     println!("{:<20} {}%", "Insert Ratio:", args.insert_ratio);
    // }

    let ratio = if args.mode == "mixed" {
        args.insert_ratio
    } else if args.mode == "insert" {
        100
    } else {
        0
    };
    // eprintln!("mode,threads,key_size,value_size,insert_ratio,ops");
    eprintln!(
        "{},{},{},{},{},{:.2}",
        args.mode, args.threads, args.key_size, args.value_size, ratio, ops
    );
}
