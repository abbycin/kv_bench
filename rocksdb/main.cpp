#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>

#include <pthread.h>
#include <unistd.h>

#include "CLI/CLI.hpp"

template<class T>
static void black_box(const T &t) {
    asm volatile("" : : "m"(t) : "memory");
}

static size_t cores_online() {
    auto n = ::sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? static_cast<size_t>(n) : 1;
}

static void bind_core(size_t tid) {
    cpu_set_t set;
    CPU_ZERO(&set);
    auto core = static_cast<int>(tid % cores_online());
    CPU_SET(core, &set);
    (void) pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &set);
}

static void require_ok(const rocksdb::Status &st, const char *what) {
    if (!st.ok()) {
        fmt::println(stderr, "{} failed: {}", what, st.ToString());
        std::abort();
    }
}

constexpr size_t kLatencyBuckets = 64;
constexpr size_t kPrefixGroups = 1024;
constexpr size_t kPrefillBatch = 1024;

struct Args {
    size_t threads = 4;
    size_t iterations = 10000;
    size_t key_size = 16;
    size_t value_size = 1024;
    size_t blob_size = 8192;
    bool random = false;
    std::string mode = "insert";
    std::optional<std::string> workload;
    std::string path;
    bool shared_keyspace = true;
    size_t prefill_keys = 0;
    uint64_t warmup_secs = 0;
    uint64_t measure_secs = 0;
    size_t scan_len = 100;
    double zipf_theta = 0.99;
    std::string read_path = "snapshot";
    std::string durability = "relaxed";
    std::string result_file = "benchmark_results.csv";
    bool cleanup = true;
    bool skip_prefill = false;
    bool reuse_path = false;
};

enum class Distribution {
    Uniform,
    Zipf,
};

enum class ReadPath {
    Snapshot,
    RwTxn,
};

enum class DurabilityMode {
    Relaxed,
    Durable,
};

struct WorkloadSpec {
    std::string id;
    std::string mode_label;
    Distribution distribution;
    uint8_t read_pct;
    uint8_t update_pct;
    uint8_t scan_pct;
    size_t scan_len;
    bool requires_prefill;
    bool insert_only;
};

struct ThreadRange {
    size_t start;
    size_t len;
};

struct Quantiles {
    uint64_t p50_us = 0;
    uint64_t p95_us = 0;
    uint64_t p99_us = 0;
    uint64_t p999_us = 0;
};

struct ThreadStats {
    uint64_t total_op = 0;
    uint64_t err_op = 0;
    std::array<uint64_t, kLatencyBuckets> hist{};
};

struct ResultRow {
    std::string workload_id;
    std::string mode;
    DurabilityMode durability_mode;
    size_t threads;
    size_t key_size;
    size_t value_size;
    size_t prefill_keys;
    bool shared_keyspace;
    Distribution distribution;
    double zipf_theta;
    uint8_t read_pct;
    uint8_t update_pct;
    uint8_t scan_pct;
    size_t scan_len;
    ReadPath read_path;
    uint64_t warmup_secs;
    uint64_t measure_secs;
    uint64_t total_op;
    uint64_t ok_op;
    uint64_t err_op;
    double ops;
    Quantiles quantiles;
    uint64_t elapsed_us;
};

enum class OpKind {
    Read,
    Update,
    Scan,
};

static std::string to_upper(std::string v) {
    for (auto &c: v) {
        if (c >= 'a' && c <= 'z') {
            c = static_cast<char>(c - 'a' + 'A');
        }
    }
    return v;
}

static std::string to_lower(std::string v) {
    for (auto &c: v) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c - 'A' + 'a');
        }
    }
    return v;
}

static std::optional<ReadPath> parse_read_path(const std::string &raw) {
    auto v = to_lower(raw);
    if (v == "snapshot") {
        return ReadPath::Snapshot;
    }
    if (v == "rw_txn" || v == "rwtxn" || v == "txn") {
        return ReadPath::RwTxn;
    }
    return std::nullopt;
}

static const char *read_path_str(ReadPath p) {
    switch (p) {
        case ReadPath::Snapshot:
            return "snapshot";
        case ReadPath::RwTxn:
            return "rw_txn";
    }
    return "snapshot";
}

static std::optional<DurabilityMode> parse_durability(const std::string &raw) {
    auto v = to_lower(raw);
    if (v == "relaxed") {
        return DurabilityMode::Relaxed;
    }
    if (v == "durable") {
        return DurabilityMode::Durable;
    }
    return std::nullopt;
}

static const char *durability_str(DurabilityMode mode) {
    switch (mode) {
        case DurabilityMode::Relaxed:
            return "relaxed";
        case DurabilityMode::Durable:
            return "durable";
    }
    return "relaxed";
}

static const char *distribution_str(Distribution d) {
    switch (d) {
        case Distribution::Uniform:
            return "uniform";
        case Distribution::Zipf:
            return "zipf";
    }
    return "uniform";
}

static std::optional<WorkloadSpec> parse_workload(const Args &args, std::string &err) {
    if (args.workload.has_value()) {
        auto id = to_upper(args.workload.value());
        if (id == "W1") {
            return WorkloadSpec{id, "mixed", Distribution::Uniform, 95, 5, 0, args.scan_len, true, false};
        }
        if (id == "W2") {
            return WorkloadSpec{id, "mixed", Distribution::Zipf, 95, 5, 0, args.scan_len, true, false};
        }
        if (id == "W3") {
            return WorkloadSpec{id, "mixed", Distribution::Uniform, 50, 50, 0, args.scan_len, true, false};
        }
        if (id == "W4") {
            return WorkloadSpec{id, "mixed", Distribution::Uniform, 5, 95, 0, args.scan_len, true, false};
        }
        if (id == "W5") {
            return WorkloadSpec{id, "mixed", Distribution::Uniform, 70, 25, 5, args.scan_len, true, false};
        }
        if (id == "W6") {
            return WorkloadSpec{id, "scan", Distribution::Uniform, 0, 0, 100, args.scan_len, true, false};
        }
        err = fmt::format("invalid workload `{}` (supported: W1..W6)", args.workload.value());
        return std::nullopt;
    }

    auto mode = to_lower(args.mode);
    if (mode == "insert") {
        return WorkloadSpec{"LEGACY_INSERT", "insert", Distribution::Uniform, 0, 100, 0, args.scan_len, false, true};
    }
    if (mode == "get") {
        return WorkloadSpec{"LEGACY_GET", "get", Distribution::Uniform, 100, 0, 0, args.scan_len, true, false};
    }
    if (mode == "scan") {
        return WorkloadSpec{"LEGACY_SCAN", "scan", Distribution::Uniform, 0, 0, 100, args.scan_len, true, false};
    }
    err = fmt::format("invalid mode `{}` (supported: insert/get/scan)", args.mode);
    return std::nullopt;
}

static bool workload_runs_gc(const WorkloadSpec &spec) { return spec.requires_prefill; }

static void run_prefill_gc(rocksdb::OptimisticTransactionDB *db, rocksdb::ColumnFamilyHandle *handle) {
    require_ok(db->EnableAutoCompaction({handle}), "enable auto compaction");
}

static std::vector<ThreadRange> split_ranges(size_t total, size_t n) {
    std::vector<ThreadRange> out;
    out.reserve(n);
    if (n == 0) {
        return out;
    }
    auto base = total / n;
    auto rem = total % n;
    size_t start = 0;
    for (size_t tid = 0; tid < n; ++tid) {
        size_t len = base + (tid < rem ? 1 : 0);
        out.push_back(ThreadRange{start, len});
        start += len;
    }
    return out;
}

static std::string make_shared_key(size_t id, size_t key_size) {
    auto group = id % kPrefixGroups;
    auto key = fmt::format("s{:03x}_{:010x}", group, id);
    if (key.size() < key_size) {
        key.resize(key_size, 'x');
    }
    return key;
}

static std::string make_thread_key(size_t tid, size_t local_id, size_t key_size) {
    auto key = fmt::format("t{:03x}_{:010x}", tid % kPrefixGroups, local_id);
    if (key.size() < key_size) {
        key.resize(key_size, 'x');
    }
    return key;
}

static std::string make_shared_prefix(size_t id) { return fmt::format("s{:03x}_", id % kPrefixGroups); }

static std::string make_thread_prefix(size_t tid) { return fmt::format("t{:03x}_", tid % kPrefixGroups); }

static size_t latency_bucket(uint64_t us) {
    auto v = std::max<uint64_t>(us, 1);
    size_t idx = 0;
    while (v >>= 1) {
        idx += 1;
        if (idx + 1 >= kLatencyBuckets) {
            break;
        }
    }
    return std::min(idx, kLatencyBuckets - 1);
}

static uint64_t histogram_quantile_us(const std::array<uint64_t, kLatencyBuckets> &hist, double q) {
    uint64_t total = 0;
    for (auto v: hist) {
        total += v;
    }
    if (total == 0) {
        return 0;
    }
    auto target = static_cast<uint64_t>(std::ceil(static_cast<double>(total) * q));
    uint64_t acc = 0;
    for (size_t i = 0; i < hist.size(); ++i) {
        acc += hist[i];
        if (acc >= target) {
            if (i == 0) {
                return 1;
            }
            if (i >= 63) {
                return (1ULL << 63);
            }
            return (1ULL << i);
        }
    }
    return (1ULL << (kLatencyBuckets - 1));
}

static uint64_t now_epoch_ms() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    return static_cast<uint64_t>(ms.count());
}

static uint64_t steady_now_ns() {
    auto now = std::chrono::steady_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    return static_cast<uint64_t>(ns.count());
}

static std::string csv_escape(const std::string &v) {
    std::string out = v;
    for (auto &c: out) {
        if (c == ',' || c == '\n' || c == '\r') {
            c = ' ';
        }
    }
    return out;
}

static const char *result_header() {
    return "engine,workload_id,mode,durability_mode,threads,key_size,value_size,prefill_"
           "keys,shared_keyspace,distribution,zipf_theta,read_pct,update_pct,scan_pct,scan_len,read_path,warmup_secs,"
           "measure_secs,total_op,ok_op,err_op,ops,p50_us,p95_us,p99_us,p999_us,elapsed_us";
}

static std::string result_row_csv(const ResultRow &r) {
    return fmt::format("{},{},{},{},{},{},{},{},{},{},{:.4},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}", "rocksdb",
                       csv_escape(r.workload_id), csv_escape(r.mode), durability_str(r.durability_mode), r.threads,
                       r.key_size, r.value_size, r.prefill_keys, r.shared_keyspace, distribution_str(r.distribution),
                       r.zipf_theta, r.read_pct, r.update_pct, r.scan_pct, r.scan_len, read_path_str(r.read_path),
                       r.warmup_secs, r.measure_secs, r.total_op, r.ok_op, r.err_op, static_cast<uint64_t>(r.ops),
                       r.quantiles.p50_us, r.quantiles.p95_us, r.quantiles.p99_us, r.quantiles.p999_us, r.elapsed_us);
}

static bool append_result_row(const std::string &path, const ResultRow &row) {
    auto exists = std::filesystem::exists(path);
    std::ofstream out(path, std::ios::out | std::ios::app);
    if (!out.is_open()) {
        return false;
    }
    if (!exists) {
        out << result_header() << "\n";
    }
    out << result_row_csv(row) << "\n";
    return true;
}

static size_t sample_zipf_like(std::mt19937_64 &rng, size_t n, double theta) {
    if (n <= 1) {
        return 0;
    }
    auto t = std::clamp(theta, 0.0001, 0.9999);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    auto u = dist(rng);
    auto scaled = std::pow(u, 1.0 / (1.0 - t));
    auto idx = static_cast<size_t>(scaled * static_cast<double>(n));
    return std::min(idx, n - 1);
}

static std::optional<size_t> pick_key_id(std::mt19937_64 &rng, Distribution distribution, double zipf_theta,
                                         bool shared_keyspace, size_t prefill_keys, size_t local_key_len) {
    if (shared_keyspace) {
        if (prefill_keys == 0) {
            return std::nullopt;
        }
        if (distribution == Distribution::Uniform) {
            std::uniform_int_distribution<size_t> dist(0, prefill_keys - 1);
            return dist(rng);
        }
        return sample_zipf_like(rng, prefill_keys, zipf_theta);
    }

    if (local_key_len == 0) {
        return std::nullopt;
    }

    if (distribution == Distribution::Uniform) {
        std::uniform_int_distribution<size_t> dist(0, local_key_len - 1);
        return dist(rng);
    }
    return sample_zipf_like(rng, local_key_len, zipf_theta);
}

static OpKind pick_op_kind(std::mt19937_64 &rng, const WorkloadSpec &spec) {
    if (spec.insert_only) {
        return OpKind::Update;
    }
    if (spec.scan_pct == 100) {
        return OpKind::Scan;
    }
    std::uniform_int_distribution<int> dist(0, 99);
    auto roll = static_cast<uint8_t>(dist(rng));
    if (roll < spec.read_pct) {
        return OpKind::Read;
    }
    if (roll < static_cast<uint8_t>(spec.read_pct + spec.update_pct)) {
        return OpKind::Update;
    }
    return OpKind::Scan;
}

static std::string find_upper_bound(const std::string &prefix) {
    std::string upper = prefix;
    for (int i = static_cast<int>(upper.size()) - 1; i >= 0; --i) {
        if (static_cast<unsigned char>(upper[i]) != 0xff) {
            upper[i] = static_cast<char>(static_cast<unsigned char>(upper[i]) + 1);
            upper.resize(static_cast<size_t>(i + 1));
            return upper;
        }
    }
    return "";
}

static bool run_one_op(OpKind op, rocksdb::OptimisticTransactionDB *db, rocksdb::ColumnFamilyHandle *handle,
                       const rocksdb::WriteOptions &wopt, const std::string &value, std::mt19937_64 &rng,
                       const WorkloadSpec &spec, Distribution distribution, double zipf_theta, ReadPath read_path,
                       size_t key_size, size_t scan_len, bool shared_keyspace, size_t prefill_keys,
                       size_t local_key_len, size_t tid, std::atomic<size_t> &insert_counter, size_t &local_insert_idx,
                       std::optional<size_t> fixed_insert_id) {
    if (op == OpKind::Read) {
        auto maybe_id = pick_key_id(rng, distribution, zipf_theta, shared_keyspace, prefill_keys, local_key_len);
        if (!maybe_id.has_value()) {
            return false;
        }

        auto key = shared_keyspace ? make_shared_key(maybe_id.value(), key_size)
                                   : make_thread_key(tid, maybe_id.value(), key_size);

        auto ropt = rocksdb::ReadOptions();
        std::string out;

        if (read_path == ReadPath::Snapshot) {
            auto *snapshot = db->GetSnapshot();
            ropt.snapshot = snapshot;
            auto st = db->Get(ropt, handle, key, &out);
            db->ReleaseSnapshot(snapshot);
            return st.ok();
        }

        auto *txn = db->BeginTransaction(wopt);
        txn->SetSnapshot();
        ropt.snapshot = txn->GetSnapshot();
        auto st = txn->Get(ropt, handle, key, &out);
        auto cst = txn->Commit();
        delete txn;
        return st.ok() && cst.ok();
    }

    if (op == OpKind::Update) {
        std::optional<std::string> key;
        if (spec.insert_only) {
            if (fixed_insert_id.has_value()) {
                auto id = fixed_insert_id.value();
                if (shared_keyspace) {
                    key = make_shared_key(id, key_size);
                } else {
                    key = make_thread_key(tid, id, key_size);
                }
            } else if (shared_keyspace) {
                auto id = insert_counter.fetch_add(1, std::memory_order_relaxed);
                key = make_shared_key(id, key_size);
            } else {
                auto id = local_insert_idx;
                local_insert_idx += 1;
                key = make_thread_key(tid, id, key_size);
            }
        } else {
            auto maybe_id = pick_key_id(rng, distribution, zipf_theta, shared_keyspace, prefill_keys, local_key_len);
            if (!maybe_id.has_value()) {
                return false;
            }
            if (shared_keyspace) {
                key = make_shared_key(maybe_id.value(), key_size);
            } else {
                key = make_thread_key(tid, maybe_id.value(), key_size);
            }
        }

        auto *txn = db->BeginTransaction(wopt);
        if (spec.insert_only) {
            auto pst = txn->Put(handle, key.value(), value);
            auto cst = txn->Commit();
            delete txn;
            return pst.ok() && cst.ok();
        }

        rocksdb::ReadOptions update_ropt;
        txn->SetSnapshot();
        update_ropt.snapshot = txn->GetSnapshot();
        auto gst = txn->GetForUpdate(update_ropt, handle, key.value(), static_cast<std::string *>(nullptr));
        if (!gst.ok()) {
            delete txn;
            return false;
        }
        auto pst = txn->Put(handle, key.value(), value);
        auto cst = txn->Commit();
        delete txn;
        return pst.ok() && cst.ok();
    }

    // scan
    std::optional<std::string> prefix;
    if (shared_keyspace) {
        auto maybe_id = pick_key_id(rng, distribution, zipf_theta, true, prefill_keys, local_key_len);
        if (!maybe_id.has_value()) {
            return false;
        }
        prefix = make_shared_prefix(maybe_id.value());
    } else {
        prefix = make_thread_prefix(tid);
    }

    auto ropt = rocksdb::ReadOptions();
    auto upper = find_upper_bound(prefix.value());
    rocksdb::Slice upper_slice(upper);
    if (!upper.empty()) {
        ropt.iterate_upper_bound = &upper_slice;
    }
    ropt.prefix_same_as_start = true;

    auto scan_limit = std::max<size_t>(scan_len, 1);

    if (read_path == ReadPath::Snapshot) {
        auto *snapshot = db->GetSnapshot();
        ropt.snapshot = snapshot;
        auto *iter = db->NewIterator(ropt, handle);
        iter->Seek(prefix.value());
        size_t scanned = 0;
        while (iter->Valid() && scanned < scan_limit) {
            black_box(iter->key());
            black_box(iter->value());
            iter->Next();
            scanned += 1;
        }
        auto st = iter->status();
        delete iter;
        db->ReleaseSnapshot(snapshot);
        return st.ok();
    }

    auto *txn = db->BeginTransaction(wopt);
    txn->SetSnapshot();
    ropt.snapshot = txn->GetSnapshot();
    auto *iter = txn->GetIterator(ropt);
    iter->Seek(prefix.value());
    size_t scanned = 0;
    while (iter->Valid() && scanned < scan_limit) {
        black_box(iter->key());
        black_box(iter->value());
        iter->Next();
        scanned += 1;
    }
    auto ist = iter->status();
    auto cst = txn->Commit();
    delete iter;
    delete txn;
    return ist.ok() && cst.ok();
}

int main(int argc, char *argv[]) {
    CLI::App app{"rocksdb bench"};
    Args args;

    bool disable_shared = false;
    bool disable_cleanup = false;
    std::string workload;

    app.add_option("-m,--mode", args.mode, "Mode: insert, get, scan");
    app.add_option("--workload", workload, "Workload preset: W1..W6");
    app.add_option("-t,--threads", args.threads, "Threads");
    app.add_option("-k,--key-size", args.key_size, "Key Size");
    app.add_option("-v,--value-size", args.value_size, "Value Size");
    app.add_option("-b,--blob-size", args.blob_size, "Blob Size");
    app.add_option("-i,--iterations", args.iterations, "Iterations");
    app.add_option("-p,--path", args.path, "Database path");
    app.add_option("--prefill-keys", args.prefill_keys, "Prefill key count");
    app.add_option("--warmup-secs", args.warmup_secs, "Warmup duration seconds");
    app.add_option("--measure-secs", args.measure_secs, "Measure duration seconds");
    app.add_option("--scan-len", args.scan_len, "Scan length per scan op");
    app.add_option("--zipf-theta", args.zipf_theta, "Zipf theta");
    app.add_option("--read-path", args.read_path, "snapshot|rw_txn");
    app.add_option("--durability", args.durability, "relaxed|durable");
    app.add_option("--result-file", args.result_file, "Unified result csv");
    app.add_flag("--random", args.random, "Shuffle insert keys (legacy insert)");
    app.add_flag("--no-shared-keyspace", disable_shared, "Use per-thread keyspace");
    app.add_flag("--no-cleanup", disable_cleanup, "Keep db directory after run");
    app.add_flag("--skip-prefill", args.skip_prefill, "Skip prefill and use existing dataset");
    app.add_flag("--reuse-path", args.reuse_path, "Allow opening existing db path");

    CLI11_PARSE(app, argc, argv);

    if (!workload.empty()) {
        args.workload = workload;
    }
    args.shared_keyspace = !disable_shared;
    args.cleanup = !disable_cleanup;

    if (args.path.empty()) {
        fmt::println(stderr, "path is empty");
        return 1;
    }
    if (std::filesystem::exists(args.path) && !args.reuse_path) {
        fmt::println(stderr, "path `{}` already exists", args.path);
        return 1;
    }
    if (args.threads == 0) {
        fmt::println(stderr, "threads must be greater than 0");
        return 1;
    }
    if (args.key_size < 16 || args.value_size < 16) {
        fmt::println(stderr, "key_size and value_size must be >= 16");
        return 1;
    }
    if (!(args.zipf_theta > 0.0 && args.zipf_theta < 1.0)) {
        fmt::println(stderr, "zipf_theta must be in (0,1)");
        return 1;
    }

    auto read_path = parse_read_path(args.read_path);
    if (!read_path.has_value()) {
        fmt::println(stderr, "invalid read_path `{}` (supported: snapshot, rw_txn)", args.read_path);
        return 1;
    }
    auto durability = parse_durability(args.durability);
    if (!durability.has_value()) {
        fmt::println(stderr, "invalid durability `{}` (supported: relaxed, durable)", args.durability);
        return 1;
    }

    std::string workload_err;
    auto workload_spec_opt = parse_workload(args, workload_err);
    if (!workload_spec_opt.has_value()) {
        fmt::println(stderr, "{}", workload_err);
        return 1;
    }
    auto workload_spec = workload_spec_opt.value();
    auto legacy_mode = workload_spec.id.rfind("LEGACY_", 0) == 0;
    auto effective_warmup_secs = legacy_mode ? 0ULL : args.warmup_secs;
    auto effective_measure_secs = legacy_mode ? 0ULL : args.measure_secs;
    auto mixed_workload = workload_spec.read_pct > 0 && workload_spec.update_pct > 0;
    if (mixed_workload && !args.shared_keyspace) {
        fmt::println(stderr, "mixed workloads require shared keyspace");
        return 1;
    }

    auto prefill_keys = workload_spec.requires_prefill
                                ? (args.prefill_keys > 0 ? args.prefill_keys : std::max<size_t>(args.iterations, 1))
                                : args.prefill_keys;

    if (workload_spec.requires_prefill && prefill_keys == 0) {
        fmt::println(stderr, "prefill_keys must be > 0 for read/mixed/scan workloads");
        return 1;
    }

    auto prefill_ranges = split_ranges(prefill_keys, args.threads);
    auto op_ranges = split_ranges(args.iterations, args.threads);

    rocksdb::ColumnFamilyOptions cfo{};
    cfo.enable_blob_files = true;
    cfo.min_blob_size = args.blob_size;
    cfo.write_buffer_size = 64 << 20;
    cfo.max_write_buffer_number = 16;

    auto cache = rocksdb::NewLRUCache(5 << 30);
    rocksdb::BlockBasedTableOptions table_options{};
    table_options.block_cache = cache;
    cfo.table_factory.reset(NewBlockBasedTableFactory(table_options));

    std::vector<rocksdb::ColumnFamilyDescriptor> cfd{};
    cfd.emplace_back("default", cfo);

    rocksdb::DBOptions options;
    options.create_if_missing = true;
    options.allow_concurrent_memtable_write = true;
    options.enable_pipelined_write = true;
    options.use_fsync = (durability.value() == DurabilityMode::Durable);

    auto wopt = rocksdb::WriteOptions();
    wopt.no_slowdown = false;
    wopt.sync = (durability.value() == DurabilityMode::Durable);

    rocksdb::OptimisticTransactionDB *db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle *> handles{};
    auto st = rocksdb::OptimisticTransactionDB::Open(options, args.path, cfd, &handles, &db);
    require_ok(st, "open db");
    auto *handle = handles[0];

    std::string value(args.value_size, '0');

    if (workload_spec.requires_prefill && !args.skip_prefill) {
        std::vector<std::thread> fill_threads;
        fill_threads.reserve(args.threads);
        for (size_t tid = 0; tid < args.threads; ++tid) {
            fill_threads.emplace_back([&, tid] {
                bind_core(tid);
                auto tr = prefill_ranges[tid];
                auto *txn = db->BeginTransaction(wopt);
                size_t in_batch = 0;
                for (size_t i = 0; i < tr.len; ++i) {
                    std::string key;
                    if (args.shared_keyspace) {
                        key = make_shared_key(tr.start + i, args.key_size);
                    } else {
                        key = make_thread_key(tid, i, args.key_size);
                    }
                    require_ok(txn->Put(handle, key, value), "prefill put");
                    in_batch += 1;
                    if (in_batch >= kPrefillBatch) {
                        require_ok(txn->Commit(), "prefill commit");
                        delete txn;
                        txn = db->BeginTransaction(wopt);
                        in_batch = 0;
                    }
                }
                if (in_batch > 0) {
                    require_ok(txn->Commit(), "prefill tail commit");
                }
                delete txn;
            });
        }
        for (auto &t: fill_threads) {
            t.join();
        }
    }

    if (workload_runs_gc(workload_spec)) {
        run_prefill_gc(db, handle);
    }

    std::barrier ready_barrier(static_cast<ptrdiff_t>(args.threads + 1));
    std::barrier measure_barrier(static_cast<ptrdiff_t>(args.threads + 1));

    std::atomic<size_t> insert_counter{0};
    std::atomic<uint64_t> measure_start_ns{0};
    std::vector<std::thread> workers;
    workers.reserve(args.threads);
    std::vector<ThreadStats> thread_stats(args.threads);

    auto seed_base = now_epoch_ms();
    auto mark_measure_start = [&measure_start_ns]() {
        uint64_t expected = 0;
        auto now_ns = steady_now_ns();
        (void) measure_start_ns.compare_exchange_strong(expected, now_ns, std::memory_order_relaxed);
    };

    for (size_t tid = 0; tid < args.threads; ++tid) {
        workers.emplace_back([&, tid] {
            bind_core(tid);
            auto &stats = thread_stats[tid];
            std::mt19937_64 rng(seed_base ^ ((tid + 1) * 0x9E3779B97F4A7C15ULL));
            auto local_key_len = prefill_ranges[tid].len;
            auto local_op_start = op_ranges[tid].start;
            auto local_op_len = op_ranges[tid].len;
            size_t local_insert_idx = 0;

            std::vector<size_t> count_indices(local_op_len);
            std::iota(count_indices.begin(), count_indices.end(), 0);
            if (args.random && workload_spec.insert_only) {
                std::shuffle(count_indices.begin(), count_indices.end(), rng);
            }

            ready_barrier.arrive_and_wait();

            if (effective_warmup_secs > 0) {
                auto warmup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(effective_warmup_secs);
                while (std::chrono::steady_clock::now() < warmup_deadline) {
                    auto op = pick_op_kind(rng, workload_spec);
                    (void) run_one_op(op, db, handle, wopt, value, rng, workload_spec, workload_spec.distribution,
                                      args.zipf_theta, read_path.value(), args.key_size, workload_spec.scan_len,
                                      args.shared_keyspace, prefill_keys, local_key_len, tid, insert_counter,
                                      local_insert_idx, std::nullopt);
                }
            }

            measure_barrier.arrive_and_wait();
            mark_measure_start();

            auto record = [&](bool ok, uint64_t us) {
                stats.total_op += 1;
                if (!ok) {
                    stats.err_op += 1;
                }
                auto b = latency_bucket(us);
                stats.hist[b] += 1;
            };

            if (effective_measure_secs > 0) {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(effective_measure_secs);
                while (std::chrono::steady_clock::now() < deadline) {
                    auto op = pick_op_kind(rng, workload_spec);
                    auto started = std::chrono::steady_clock::now();
                    auto ok = run_one_op(op, db, handle, wopt, value, rng, workload_spec, workload_spec.distribution,
                                         args.zipf_theta, read_path.value(), args.key_size, workload_spec.scan_len,
                                         args.shared_keyspace, prefill_keys, local_key_len, tid, insert_counter,
                                         local_insert_idx, std::nullopt);
                    auto us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                            std::chrono::steady_clock::now() - started)
                                                            .count());
                    record(ok, us);
                }
            } else {
                for (size_t i: count_indices) {
                    auto op = workload_spec.insert_only ? OpKind::Update : pick_op_kind(rng, workload_spec);
                    std::optional<size_t> fixed_insert_id = std::nullopt;
                    if (workload_spec.insert_only) {
                        fixed_insert_id = args.shared_keyspace ? (local_op_start + i) : i;
                    }
                    auto started = std::chrono::steady_clock::now();
                    auto ok = run_one_op(op, db, handle, wopt, value, rng, workload_spec, workload_spec.distribution,
                                         args.zipf_theta, read_path.value(), args.key_size, workload_spec.scan_len,
                                         args.shared_keyspace, prefill_keys, local_key_len, tid, insert_counter,
                                         local_insert_idx, fixed_insert_id);
                    auto us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                            std::chrono::steady_clock::now() - started)
                                                            .count());
                    record(ok, us);
                }
            }
        });
    }

    ready_barrier.arrive_and_wait();
    measure_barrier.arrive_and_wait();
    mark_measure_start();

    for (auto &w: workers) {
        w.join();
    }

    auto measure_end_ns = steady_now_ns();
    auto measure_begin_ns = measure_start_ns.load(std::memory_order_relaxed);
    if (measure_begin_ns == 0 || measure_end_ns < measure_begin_ns) {
        measure_begin_ns = measure_end_ns;
    }
    uint64_t elapsed_us = (measure_end_ns - measure_begin_ns) / 1000;
    uint64_t total_op = 0;
    uint64_t err_op = 0;
    std::array<uint64_t, kLatencyBuckets> merged_hist{};

    for (const auto &s: thread_stats) {
        total_op += s.total_op;
        err_op += s.err_op;
        for (size_t i = 0; i < merged_hist.size(); ++i) {
            merged_hist[i] += s.hist[i];
        }
    }

    auto ops = elapsed_us == 0 ? 0.0 : (static_cast<double>(total_op) * 1'000'000.0 / static_cast<double>(elapsed_us));

    uint64_t ok_op = total_op >= err_op ? (total_op - err_op) : 0;

    auto row = ResultRow{
            .workload_id = workload_spec.id,
            .mode = workload_spec.mode_label,
            .durability_mode = durability.value(),
            .threads = args.threads,
            .key_size = args.key_size,
            .value_size = args.value_size,
            .prefill_keys = prefill_keys,
            .shared_keyspace = args.shared_keyspace,
            .distribution = workload_spec.distribution,
            .zipf_theta = args.zipf_theta,
            .read_pct = workload_spec.read_pct,
            .update_pct = workload_spec.update_pct,
            .scan_pct = workload_spec.scan_pct,
            .scan_len = workload_spec.scan_len,
            .read_path = read_path.value(),
            .warmup_secs = effective_warmup_secs,
            .measure_secs = effective_measure_secs,
            .total_op = total_op,
            .ok_op = ok_op,
            .err_op = err_op,
            .ops = ops,
            .quantiles =
                    Quantiles{
                            .p50_us = histogram_quantile_us(merged_hist, 0.50),
                            .p95_us = histogram_quantile_us(merged_hist, 0.95),
                            .p99_us = histogram_quantile_us(merged_hist, 0.99),
                            .p999_us = histogram_quantile_us(merged_hist, 0.999),
                    },
            .elapsed_us = elapsed_us,
    };

    if (!append_result_row(args.result_file, row)) {
        fmt::println(stderr, "failed to write result file {}", args.result_file);
        return 1;
    }

    fmt::println("engine=rocksdb workload={} mode={} durability={} threads={} total_op={} ok_op={} err_op={} ops={} "
                 "p99_us={} result_file={}",
                 row.workload_id, row.mode, durability_str(row.durability_mode), row.threads, row.total_op, row.ok_op,
                 row.err_op, static_cast<uint64_t>(row.ops), row.quantiles.p99_us, args.result_file);

    delete handle;
    delete db;

    if (args.cleanup) {
        std::filesystem::remove_all(args.path);
    }
    return 0;
}
