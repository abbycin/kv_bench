use std::{
    alloc::{GlobalAlloc, System},
    cell::Cell,
    collections::{HashMap, hash_map::Entry},
    fmt::Display,
    hash::{DefaultHasher, Hash, Hasher},
    ptr,
    sync::{LazyLock, Mutex, atomic::AtomicBool},
};

pub struct MyAlloc;

fn trace(size: usize, is_alloc: bool) -> Option<String> {
    let mut key = String::new();
    backtrace::trace(|f| {
        backtrace::resolve_frame(f, |sym| {
            if let Some(filename) = sym.filename()
                && let Some(line) = sym.lineno()
            {
                if let Some(name) = filename.to_str()
                    && name.contains("mace")
                {
                    if name.len() > 10 {
                        // sometime name maybe empty
                        let x = format!("{}:{}\n", name, line);
                        key.extend(x.chars().into_iter());
                    }
                }
            }
        });
        true
    });
    if !key.is_empty() {
        let mut lk = G_MAP.lock().unwrap();
        let tmp = key.clone();
        match lk.entry(tmp) {
            Entry::Vacant(v) => {
                if is_alloc {
                    v.insert(Status {
                        nr_alloc: 1,
                        alloc_size: size,
                        nr_free: 0,
                        free_size: 0,
                    });
                } else {
                    v.insert(Status {
                        nr_alloc: 0,
                        alloc_size: 0,
                        nr_free: 1,
                        free_size: size,
                    });
                }
            }
            Entry::Occupied(mut o) => {
                let s = o.get_mut();
                if is_alloc {
                    s.nr_alloc += 1;
                    s.alloc_size += size;
                } else {
                    s.nr_free += 1;
                    s.free_size += size;
                }
            }
        }
        Some(key)
    } else {
        None
    }
}

#[derive(Debug)]
pub struct Status {
    nr_alloc: usize,
    alloc_size: usize,
    nr_free: usize,
    free_size: usize,
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

static G_STOP: AtomicBool = AtomicBool::new(false);

static G_MAP: LazyLock<Mutex<HashMap<String, Status>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static G_TRACE: LazyLock<Mutex<HashMap<u64, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const META_LEN: usize = 8;

thread_local! {
    static G_SELF: Cell<bool> = const { Cell::new(false) };
}

const fn real_size(layout: &std::alloc::Layout) -> usize {
    if META_LEN > layout.align() {
        META_LEN.checked_add(layout.size()).unwrap()
    } else {
        layout.align().checked_add(layout.size()).unwrap()
    }
}

fn new_layout(layout: std::alloc::Layout) -> std::alloc::Layout {
    let align = layout.align().max(align_of::<u64>());
    let sz = real_size(&layout);
    std::alloc::Layout::from_size_align(sz, align).unwrap()
}

fn write_hash(x: *mut u8, align: usize, s: Option<String>) -> *mut u8 {
    let r = unsafe { x.add(META_LEN.max(align)) };
    if !G_SELF.with(|x| x.get()) {
        G_SELF.with(|x| x.set(true));
        let p = x.cast::<u64>();
        if let Some(s) = s {
            let mut stat = DefaultHasher::new();
            s.hash(&mut stat);
            let h = stat.finish();
            unsafe { p.write_unaligned(h) };
            let mut lk = G_TRACE.lock().unwrap();
            lk.insert(h, s);
        } else {
            unsafe { p.write_unaligned(u64::MAX) };
        }
        G_SELF.with(|x| x.set(false));
    }
    r
}

fn read_hash(x: *mut u8, align: usize) -> *mut u8 {
    let (h, p) = unsafe {
        let p = x.sub(META_LEN.max(align)).cast::<u64>();
        (p.read_unaligned(), p.cast::<u8>())
    };

    if h == u64::MAX {
        return p;
    }
    if !G_SELF.with(|x| x.get()) {
        G_SELF.with(|x| x.set(true));
        let mut lk = G_TRACE.lock().unwrap();
        lk.remove(&h);
        G_SELF.with(|x| x.set(false));
    }
    p
}

unsafe impl GlobalAlloc for MyAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let s = if !G_SELF.with(|x| x.get()) && !G_STOP.load(std::sync::atomic::Ordering::Acquire) {
            G_SELF.with(|x| x.set(true));
            let x = trace(layout.size(), true);
            G_SELF.with(|x| x.set(false));
            x
        } else {
            None
        };

        let new = new_layout(layout);
        let x = unsafe { System.alloc(new) };
        write_hash(x, new.align(), s)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        if !G_SELF.with(|x| x.get()) && !G_STOP.load(std::sync::atomic::Ordering::Acquire) {
            G_SELF.with(|x| x.set(true));
            trace(layout.size(), false);
            G_SELF.with(|x| x.set(false));
        }
        let new = new_layout(layout);
        let p = read_hash(ptr, new.align());
        unsafe { System.dealloc(p, new) };
    }

    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        let p = unsafe { self.alloc(layout) };
        if !p.is_null() {
            unsafe { ptr::write_bytes(p, 0, layout.size()) };
        }
        p
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        let s = if !G_SELF.with(|x| x.get()) && !G_STOP.load(std::sync::atomic::Ordering::Acquire) {
            G_SELF.with(|x| x.set(true));
            let x = trace(layout.size(), true);
            G_SELF.with(|x| x.set(false));
            x
        } else {
            None
        };

        unsafe {
            let old_layout = new_layout(layout);
            let raw = ptr.sub(META_LEN.max(old_layout.align()));
            let new_total_size = META_LEN + new_size;

            let new_raw = System.realloc(raw, old_layout, new_total_size);
            if new_raw.is_null() {
                return new_raw;
            }
            write_hash(new_raw, old_layout.align(), s)
        }
    }
}

pub fn print_filtered_trace<F>(f: F)
where
    F: Fn(&str, &Status),
{
    G_STOP.store(true, std::sync::atomic::Ordering::Release);
    let lk = G_MAP.lock().unwrap();
    let t = G_TRACE.lock().unwrap();

    for (_, v) in t.iter() {
        if let Some(s) = lk.get(v) {
            f(v, s);
        }
    }
}

pub fn print_all_trace<F>(f: F)
where
    F: Fn(&str, &Status),
{
    G_STOP.store(true, std::sync::atomic::Ordering::Release);
    let lk = G_MAP.lock().unwrap();

    lk.iter().for_each(|(k, v)| f(k, v));
}
