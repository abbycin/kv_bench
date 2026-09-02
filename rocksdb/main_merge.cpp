// Merge-operator counter benchmark for RocksDB.
//
// Workload: a pool of `--num-keys` keys (deterministic pseudo-random bytes of
// `--key-size`), each holding a u64 little-endian counter. Every op picks a
// key uniformly at random, then with probability `--merge-ratio` appends a
// merge operand (+1) and otherwise reads the current counter. Value size is
// fixed at 8 bytes (u64). Threads, key size, key count, ratio, and duration
// are configurable; results append to a CSV shared with `src/bin/mace_merge.rs`.
//
// With `--accumulate-per-key N > 0` the bench first appends exactly N merge
// operands to every key (batched WriteBatch, auto-compaction stays disabled
// so the chains genuinely accumulate), then measures a 100% get workload:
// get cost as a function of accumulated operand-chain length.
//
// The merge operator is a u64 add (`AssociativeMergeOperator`), the same
// algebra as mace's `U64AddOperator`. Merge writes go through `DB::Merge`
// (the canonical operand-append path, no read-modify-write); reads use a
// snapshot `DB::Get`.

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>

#include <pthread.h>
#include <unistd.h>

#include "CLI/CLI.hpp"

template <class T>
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
constexpr size_t kPrefillBatch = 1024;
constexpr size_t kValueLen = 8; // u64 LE counter

// u64 add merge operator: base (absent = 0) + operand, little-endian
class UInt64AddOperator : public rocksdb::AssociativeMergeOperator {
public:
    bool Merge(const rocksdb::Slice & /*key*/, const rocksdb::Slice *existing_value, const rocksdb::Slice &value,
               std::string *new_value, rocksdb::Logger * /*logger*/) const override {
        uint64_t base = 0;
        if (existing_value != nullptr && existing_value->size() == kValueLen) {
            std::memcpy(&base, existing_value->data(), kValueLen);
        }
        uint64_t delta = 0;
        if (value.size() == kValueLen) {
            std::memcpy(&delta, value.data(), kValueLen);
        }
        base += delta;
        new_value->assign(reinterpret_cast<const char *>(&base), kValueLen);
        return true;
    }

    const char *Name() const override { return "UInt64AddOperator"; }
};

static const std::string kOperand("\x01\x00\x00\x00\x00\x00\x00\x00", kValueLen);
static const std::string kZero("\x00\x00\x00\x00\x00\x00\x00\x00", kValueLen);

struct Args {
    size_t threads = 4;
    size_t iterations = 10000;
    size_t key_size = 32;
    size_t num_keys = 100;
    double merge_ratio = 0.7;
    std::optional<size_t> accumulate_per_key;
    std::string path;
    uint64_t warmup_secs = 0;
    uint64_t measure_secs = 0;
    std::string durability = "relaxed";
    std::string result_file = "merge_benchmark_results.csv";
    bool cleanup = true;
    bool skip_prefill = false;
    bool reuse_path = false;
};

enum class DurabilityMode {
    Relaxed,
    Durable,
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
    DurabilityMode durability_mode;
    size_t threads;
    size_t key_size;
    size_t value_size;
    size_t prefill_keys;
    uint8_t read_pct;
    uint8_t update_pct;
    uint64_t warmup_secs;
    uint64_t measure_secs;
    uint64_t total_op;
    uint64_t ok_op;
    uint64_t err_op;
    double ops;
    Quantiles quantiles;
    uint64_t elapsed_us;
};

static std::string to_lower(std::string v) {
    for (auto &c: v) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c - 'A' + 'a');
        }
    }
    return v;
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

static std::vector<size_t> split_ranges(size_t total, size_t n) {
    std::vector<size_t> lens;
    lens.reserve(n);
    if (n == 0) {
        return lens;
    }
    auto base = total / n;
    auto rem = total % n;
    for (size_t tid = 0; tid < n; ++tid) {
        lens.push_back(base + (tid < rem ? 1 : 0));
    }
    return lens;
}

// deterministic pseudo-random key bytes for a key id (splitmix64 stream,
// identical to src/bin/mace_merge.rs)
static uint64_t splitmix64(uint64_t &state) {
    state += 0x9E3779B97F4A7C15ULL;
    uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

static std::string make_key(size_t id, size_t key_size) {
    uint64_t state = static_cast<uint64_t>(id);
    std::string out;
    out.reserve(key_size);
    while (out.size() < key_size) {
        auto v = splitmix64(state);
        out.append(reinterpret_cast<const char *>(&v), sizeof(v));
    }
    out.resize(key_size);
    return out;
}

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

static const char *result_header() {
    return "engine,workload_id,mode,durability_mode,threads,key_size,value_size,prefill_keys,shared_keyspace,"
           "distribution,zipf_theta,read_pct,update_pct,scan_pct,scan_len,read_path,warmup_secs,measure_secs,"
           "total_op,ok_op,err_op,ops,p50_us,p95_us,p99_us,p999_us,elapsed_us";
}

static std::string result_row_csv(const ResultRow &r, const std::string &workload_id) {
    return fmt::format("{},{},{},{},{},{},{},{},{},{},{:.4f},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}", "rocksdb",
                       workload_id, "merge_get", durability_str(r.durability_mode), r.threads, r.key_size,
                       r.value_size, r.prefill_keys, true, "uniform", 0.99, r.read_pct, r.update_pct, 0, 0,
                       "snapshot", r.warmup_secs, r.measure_secs, r.total_op, r.ok_op, r.err_op,
                       static_cast<uint64_t>(r.ops), r.quantiles.p50_us, r.quantiles.p95_us, r.quantiles.p99_us,
                       r.quantiles.p999_us, r.elapsed_us);
}

static bool append_result_row(const std::string &path, const ResultRow &row, const std::string &workload_id) {
    auto exists = std::filesystem::exists(path);
    std::ofstream out(path, std::ios::out | std::ios::app);
    if (!out.is_open()) {
        return false;
    }
    if (!exists) {
        out << result_header() << "\n";
    }
    out << result_row_csv(row, workload_id) << "\n";
    return true;
}

// append +1 operand to the key
static bool run_merge(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *handle, const rocksdb::WriteOptions &wopt,
                      const std::string &key) {
    auto st = db->Merge(wopt, handle, key, kOperand);
    return st.ok();
}

// read the current counter through a snapshot
static bool run_get(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *handle, const std::string &key) {
    auto ropt = rocksdb::ReadOptions();
    auto *snapshot = db->GetSnapshot();
    ropt.snapshot = snapshot;
    std::string out;
    auto st = db->Get(ropt, handle, key, &out);
    db->ReleaseSnapshot(snapshot);
    if (st.ok()) {
        black_box(out);
    }
    return st.ok();
}

int main(int argc, char *argv[]) {
    CLI::App app{"rocksdb merge bench"};
    Args args;

    bool disable_cleanup = false;

    app.add_option("-p,--path", args.path, "Database path");
    app.add_option("-k,--key-size", args.key_size, "Key Size");
    app.add_option("-n,--num-keys", args.num_keys, "Key pool size");
    app.add_option("-t,--threads", args.threads, "Threads");
    app.add_option("-i,--iterations", args.iterations, "Iterations");
    app.add_option("--merge-ratio", args.merge_ratio, "Probability of merge per op (rest are gets)");
    app.add_option("--accumulate-per-key", args.accumulate_per_key,
                   "Append N merge operands to every key first, then measure 100% gets");
    app.add_option("--warmup-secs", args.warmup_secs, "Warmup duration seconds");
    app.add_option("--measure-secs", args.measure_secs, "Measure duration seconds");
    app.add_option("--durability", args.durability, "relaxed|durable");
    app.add_option("--result-file", args.result_file, "Unified result csv");
    app.add_flag("--no-cleanup", disable_cleanup, "Keep db directory after run");
    app.add_flag("--skip-prefill", args.skip_prefill, "Skip prefill and use existing dataset");
    app.add_flag("--reuse-path", args.reuse_path, "Allow opening existing db path");

    CLI11_PARSE(app, argc, argv);

    args.cleanup = !disable_cleanup;

    if (args.path.empty()) {
        fmt::println(stderr, "path is empty");
        return 1;
    }
    if (std::filesystem::exists(args.path) && !args.reuse_path) {
        fmt::println(stderr, "path `{}` already exists", args.path);
        return 1;
    }
    if (args.skip_prefill && !args.reuse_path) {
        fmt::println(stderr, "--skip-prefill requires --reuse-path");
        return 1;
    }
    if (args.skip_prefill && !std::filesystem::exists(args.path)) {
        fmt::println(stderr, "--skip-prefill requires existing path, but `{}` does not exist", args.path);
        return 1;
    }
    if (args.threads == 0) {
        fmt::println(stderr, "threads must be greater than 0");
        return 1;
    }
    if (args.key_size < 16) {
        fmt::println(stderr, "key_size must be >= 16");
        return 1;
    }
    if (args.num_keys == 0) {
        fmt::println(stderr, "num_keys must be greater than 0");
        return 1;
    }
    if (!(args.merge_ratio >= 0.0 && args.merge_ratio <= 1.0)) {
        fmt::println(stderr, "merge_ratio must be in range [0, 1]");
        return 1;
    }

    auto durability = parse_durability(args.durability);
    if (!durability.has_value()) {
        fmt::println(stderr, "invalid durability `{}` (supported: relaxed, durable)", args.durability);
        return 1;
    }

    bool accumulate = args.accumulate_per_key.has_value();
    size_t accumulate_per_key = args.accumulate_per_key.value_or(0);

    auto op_lens = split_ranges(args.iterations, args.threads);

    rocksdb::ColumnFamilyOptions cfo{};
    cfo.enable_blob_files = true;
    cfo.min_blob_size = 8192;
    cfo.write_buffer_size = 64 << 20;
    cfo.max_write_buffer_number = 16;
    cfo.merge_operator.reset(new UInt64AddOperator());
    // accumulate mode must keep operand chains raw: no background compaction
    // may fold them before the get phase measures them
    cfo.disable_auto_compactions = accumulate;

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

    rocksdb::DB *db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle *> handles{};
    auto st = rocksdb::DB::Open(options, args.path, cfd, &handles, &db);
    require_ok(st, "open db");
    auto *handle = handles[0];

    if (!args.skip_prefill) {
        std::vector<std::thread> fill_threads;
        fill_threads.reserve(args.threads);
        for (size_t tid = 0; tid < args.threads; ++tid) {
            fill_threads.emplace_back([&, tid] {
                bind_core(tid);
                rocksdb::WriteBatch batch;
                size_t in_batch = 0;
                for (size_t id = tid; id < args.num_keys; id += args.threads) {
                    auto key = make_key(id, args.key_size);
                    require_ok(batch.Put(handle, key, kZero), "prefill put");
                    in_batch += 1;
                    if (in_batch >= kPrefillBatch) {
                        require_ok(db->Write(wopt, &batch), "prefill write");
                        batch.Clear();
                        in_batch = 0;
                    }
                }
                if (in_batch > 0) {
                    require_ok(db->Write(wopt, &batch), "prefill tail write");
                }
            });
        }
        for (auto &t: fill_threads) {
            t.join();
        }
    }

    if (accumulate) {
        // keep auto-compaction disabled so accumulated operand chains stay raw
        std::vector<std::thread> acc_threads;
        acc_threads.reserve(args.threads);
        auto lens = split_ranges(args.num_keys, args.threads);
        size_t start = 0;
        for (size_t tid = 0; tid < args.threads; ++tid) {
            auto range_start = start;
            start += lens[tid];
            acc_threads.emplace_back([&, tid, range_start] {
                bind_core(tid);
                auto range_end = range_start + lens[tid];
                rocksdb::WriteBatch batch;
                size_t in_batch = 0;
                for (size_t id = range_start; id < range_end; ++id) {
                    auto key = make_key(id, args.key_size);
                    for (size_t k = 0; k < accumulate_per_key; ++k) {
                        require_ok(batch.Merge(handle, key, kOperand), "accumulate merge");
                        in_batch += 1;
                        if (in_batch >= kPrefillBatch) {
                            require_ok(db->Write(wopt, &batch), "accumulate write");
                            batch.Clear();
                            in_batch = 0;
                        }
                    }
                }
                if (in_batch > 0) {
                    require_ok(db->Write(wopt, &batch), "accumulate tail write");
                }
            });
        }
        for (auto &t: acc_threads) {
            t.join();
        }
    } else {
        require_ok(db->EnableAutoCompaction({handle}), "enable auto compaction");
    }

    std::barrier ready_barrier(static_cast<ptrdiff_t>(args.threads + 1));
    std::barrier measure_barrier(static_cast<ptrdiff_t>(args.threads + 1));

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
            auto local_op_len = op_lens[tid];

            ready_barrier.arrive_and_wait();

            if (args.warmup_secs > 0) {
                auto warmup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(args.warmup_secs);
                while (std::chrono::steady_clock::now() < warmup_deadline) {
                    auto id = std::uniform_int_distribution<size_t>(0, args.num_keys - 1)(rng);
                    auto key = make_key(id, args.key_size);
                    auto roll = std::uniform_real_distribution<double>(0.0, 1.0)(rng);
                    if (!accumulate && roll < args.merge_ratio) {
                        (void) run_merge(db, handle, wopt, key);
                    } else {
                        (void) run_get(db, handle, key);
                    }
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

            if (args.measure_secs > 0) {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(args.measure_secs);
                while (std::chrono::steady_clock::now() < deadline) {
                    auto id = std::uniform_int_distribution<size_t>(0, args.num_keys - 1)(rng);
                    auto key = make_key(id, args.key_size);
                    auto roll = std::uniform_real_distribution<double>(0.0, 1.0)(rng);
                    auto started = std::chrono::steady_clock::now();
                    bool ok;
                    if (!accumulate && roll < args.merge_ratio) {
                        ok = run_merge(db, handle, wopt, key);
                    } else {
                        ok = run_get(db, handle, key);
                    }
                    auto us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                           std::chrono::steady_clock::now() - started)
                                                           .count());
                    record(ok, us);
                }
            } else {
                for (size_t i = 0; i < local_op_len; ++i) {
                    auto id = std::uniform_int_distribution<size_t>(0, args.num_keys - 1)(rng);
                    auto key = make_key(id, args.key_size);
                    auto roll = std::uniform_real_distribution<double>(0.0, 1.0)(rng);
                    auto started = std::chrono::steady_clock::now();
                    bool ok;
                    if (!accumulate && roll < args.merge_ratio) {
                        ok = run_merge(db, handle, wopt, key);
                    } else {
                        ok = run_get(db, handle, key);
                    }
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

    auto workload_id = accumulate ? fmt::format("MERGE_GET_{}", accumulate_per_key)
                                  : fmt::format("MERGE_{}", static_cast<uint8_t>(std::round(args.merge_ratio * 100.0)));
    uint8_t read_pct = 0;
    uint8_t update_pct = 0;
    if (accumulate) {
        read_pct = 100;
    } else {
        update_pct = static_cast<uint8_t>(std::round(args.merge_ratio * 100.0));
        read_pct = static_cast<uint8_t>(100 - update_pct);
    }

    auto row = ResultRow{
            .durability_mode = durability.value(),
            .threads = args.threads,
            .key_size = args.key_size,
            .value_size = kValueLen,
            .prefill_keys = args.num_keys,
            .read_pct = read_pct,
            .update_pct = update_pct,
            .warmup_secs = args.warmup_secs,
            .measure_secs = args.measure_secs,
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

    if (!append_result_row(args.result_file, row, workload_id)) {
        fmt::println(stderr, "failed to write result file {}", args.result_file);
        return 1;
    }

    fmt::println("engine=rocksdb workload={} mode=merge_get durability={} threads={} total_op={} ok_op={} err_op={} "
                 "ops={} p99_us={} result_file={}",
                 workload_id, durability_str(row.durability_mode), row.threads, row.total_op, row.ok_op, row.err_op,
                 static_cast<uint64_t>(row.ops), row.quantiles.p99_us, args.result_file);

    delete handle;
    delete db;

    if (args.cleanup) {
        std::filesystem::remove_all(args.path);
    }
    return 0;
}
