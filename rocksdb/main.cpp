#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fmt/base.h>
#include <fmt/format.h>
#include <memory>
#include <random>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

#include <algorithm>
#include <barrier>
#include <filesystem>
#include <format>
#include <numeric>
#include <string>

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include "CLI/CLI.hpp"
#include "instant.h"

template<class T>
static void black_box(const T &t) {
    asm volatile("" ::"m"(t) : "memory");
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

struct Args {
    size_t threads;
    size_t iterations;
    size_t key_size;
    size_t value_size;
    size_t blob_size;
    size_t insert_ratio;
    bool random;
    std::string mode;
    std::string path;
};

int main(int argc, char *argv[]) {
    CLI::App app{"rocksdb bench"};
    Args args{
            .threads = 4,
            .iterations = 100000,
            .key_size = 16,
            .value_size = 1024,
            .blob_size = 8192,
            .insert_ratio = 30,
            .mode = "insert",
            .path = "/tmp/rocksdb_tmp",
    };

    app.add_option("-m,--mode", args.mode, "Mode: insert, get, mixed, scan");
    app.add_option("-t,--threads", args.threads, "Threads");
    app.add_option("-k,--key-size", args.key_size, "Key Size");
    app.add_option("-v,--value-size", args.value_size, "Value Size");
    app.add_option("-b,--blob-size", args.blob_size, "Blob Size");
    app.add_option("-i,--iterations", args.iterations, "Iterations");
    app.add_option("-r,--insert-ratio", args.insert_ratio, "Insert Ratio for mixed mode");
    app.add_option("-p,--path", args.path, "DataBase Home");
    app.add_flag("--random", args.random, "Shuffle insert keys");

    CLI11_PARSE(app, argc, argv);

    if (args.path.empty()) {
        fmt::println("path is empty");
        return 1;
    }

    if (std::filesystem::exists(args.path)) {
        fmt::println("path `{}` already exists", args.path);
        return 1;
    }

    if (args.threads == 0) {
        fmt::println("Error: threads must be greater than 0");
        return 1;
    }

    if (args.mode != "insert" && args.mode != "get" && args.mode != "mixed" && args.mode != "scan") {
        fmt::println("Error: Invalid mode");
        return 1;
    }

    if (args.key_size < 16 || args.value_size < 16) {
        fmt::println("Error: key_size or value_size too small, must >= 16");
        return 1;
    }

    if (args.insert_ratio > 100) {
        fmt::println("Error: Insert ratio must be between 0 and 100");
        return 1;
    }

    auto find_upper_bound = [](std::string prefix) {
        std::string upper_bound_key = prefix;
        for (int i = upper_bound_key.length() - 1; i >= 0; --i) {
            if ((unsigned char) upper_bound_key[i] != 0xff) {
                upper_bound_key[i] = (unsigned char) upper_bound_key[i] + 1;
                upper_bound_key.resize(i + 1);
                break;
            }
            if (i == 0) {
                upper_bound_key = "";
                break;
            }
        }
        return upper_bound_key;
    };

    rocksdb::ColumnFamilyOptions cfo{};
    cfo.enable_blob_files = true;
    cfo.min_blob_size = args.blob_size;
    cfo.disable_auto_compactions = true;
    cfo.max_compaction_bytes = (1ULL << 60);
    cfo.level0_stop_writes_trigger = 1000000;
    cfo.level0_slowdown_writes_trigger = 1000000;
    cfo.level0_file_num_compaction_trigger = 1000000;
    cfo.write_buffer_size = 64 << 20;
    cfo.max_write_buffer_number = 128;

    // use 3GB block cache
    auto cache = rocksdb::NewLRUCache(3 << 30);
    rocksdb::BlockBasedTableOptions table_options{};
    table_options.block_cache = cache;
    cfo.table_factory.reset(NewBlockBasedTableFactory(table_options));

    std::vector<rocksdb::ColumnFamilyDescriptor> cfd{};
    cfd.push_back(rocksdb::ColumnFamilyDescriptor("default", cfo));

    rocksdb::DBOptions options;
    options.create_if_missing = true;
    options.allow_concurrent_memtable_write = true;
    options.enable_pipelined_write = true;
    options.max_background_flushes = 8;
    options.env->SetBackgroundThreads(8, rocksdb::Env::Priority::HIGH);

    auto wopt = rocksdb::WriteOptions();
    wopt.no_slowdown = true;
    std::vector<std::thread> wg;
    std::atomic<uint64_t> total_op{0};
    rocksdb::OptimisticTransactionDB *db;
    auto b = nm::Instant::now();
    std::vector<rocksdb::ColumnFamilyHandle *> handles{};
    auto s = rocksdb::OptimisticTransactionDB::Open(options, args.path, cfd, &handles, &db);
    require_ok(s, "open db");
    std::barrier ready_barrier{static_cast<ptrdiff_t>(args.threads + 1)};
    std::barrier start_barrier{static_cast<ptrdiff_t>(args.threads + 1)};

    std::random_device rd{};

    std::string val(args.value_size, 'x');

    auto *handle = handles[0];

    if (args.mode == "get" || args.mode == "scan") {
        std::vector<std::thread> fill_threads;
        for (size_t tid = 0; tid < args.threads; ++tid) {
            fill_threads.emplace_back([&, tid] {
                bind_core(tid);
                size_t count = args.iterations / args.threads + (tid < args.iterations % args.threads ? 1 : 0);
                const size_t batch_size = 10000;
                for (size_t i = 0; i < count; i += batch_size) {
                    auto *kv = db->BeginTransaction(wopt);
                    for (size_t j = 0; j < batch_size && (i + j) < count; ++j) {
                        auto key = std::format("key_{}_{}", tid, i + j);
                        key.resize(args.key_size, 'x');
                        require_ok(kv->Put(handle, key, val), "fill put");
                    }
                    require_ok(kv->Commit(), "fill commit");
                    delete kv;
                }
            });
        }
        for (auto &t: fill_threads)
            t.join();

        delete handle;
        delete db;
        handles.clear();
        // re-open db
        s = rocksdb::OptimisticTransactionDB::Open(options, args.path, cfd, &handles, &db);
        require_ok(s, "reopen db");
        handle = handles[0];
    }

    auto base_seed = rd();
    for (size_t tid = 0; tid < args.threads; ++tid) {
        wg.emplace_back([&, tid] {
            bind_core(tid);
            std::string rval(args.value_size, '0');
            auto prefix = std::format("key_{}_", tid);
            auto ropt = rocksdb::ReadOptions();
            auto upper_bound = find_upper_bound(prefix);
            auto upper_bound_slice = rocksdb::Slice(upper_bound);
            if (!upper_bound.empty()) {
                ropt.iterate_upper_bound = &upper_bound_slice;
            }
            ropt.prefix_same_as_start = true;
            size_t round = 0;
            std::mt19937 thread_gen(static_cast<uint32_t>(base_seed) ^ static_cast<uint32_t>(tid));
            std::uniform_int_distribution<int> mixed_dist(0, 99);

            size_t key_count = args.iterations / args.threads + (tid < args.iterations % args.threads ? 1 : 0);
            std::vector<size_t> indices(key_count);
            std::iota(indices.begin(), indices.end(), 0);
            if (args.random) {
                std::shuffle(indices.begin(), indices.end(), thread_gen);
            }

            ready_barrier.arrive_and_wait();
            start_barrier.arrive_and_wait();

            if (args.mode == "insert") {
                for (size_t i: indices) {
                    auto key = std::format("key_{}_{}", tid, i);
                    key.resize(args.key_size, 'x');
                    round += 1;
                    auto *kv = db->BeginTransaction(wopt);
                    require_ok(kv->Put(handle, key, val), "insert put");
                    require_ok(kv->Commit(), "insert commit");
                    delete kv;
                }
            } else if (args.mode == "get") {
                // rocksdb has no dedicated read-only txn in this bench path, use direct get for fair read-path
                // comparison with mace view
                for (size_t i: indices) {
                    auto key = std::format("key_{}_{}", tid, i);
                    key.resize(args.key_size, 'x');
                    round += 1;
                    require_ok(db->Get(ropt, handle, key, &rval), "get");
                }
            } else if (args.mode == "mixed") {
                for (size_t i: indices) {
                    auto key = std::format("key_{}_{}", tid, i);
                    key.resize(args.key_size, 'x');
                    round += 1;
                    auto is_insert = mixed_dist(thread_gen) < static_cast<int>(args.insert_ratio);
                    auto *kv = db->BeginTransaction(wopt);
                    if (is_insert) {
                        require_ok(kv->Put(handle, key, val), "mixed put");
                    } else {
                        auto st = kv->Get(ropt, handle, key, &rval);
                        if (!st.ok() && !st.IsNotFound()) {
                            require_ok(st, "mixed get");
                        }
                    }
                    require_ok(kv->Commit(), "mixed commit");
                    delete kv;
                }
            } else if (args.mode == "scan") {
                auto *iter = db->NewIterator(ropt);
                iter->Seek(prefix);
                while (iter->Valid()) {
                    round += 1;
                    auto k = iter->key();
                    auto v = iter->value();
                    black_box(k);
                    black_box(v);
                    iter->Next();
                }
                require_ok(iter->status(), "scan iterate");
                delete iter;
            }
            total_op.fetch_add(round, std::memory_order::relaxed);
        });
    }

    ready_barrier.arrive_and_wait();
    b = nm::Instant::now();
    start_barrier.arrive_and_wait();

    for (auto &w: wg) {
        w.join();
    }
    size_t ratio = [&args] -> size_t {
        if (args.mode == "mixed")
            return args.insert_ratio;
        return args.mode == "insert" ? 100 : 0;
    }();
    const auto elapsed_us = b.elapse_usec();
    uint64_t ops = 0;
    const auto total = total_op.load(std::memory_order_relaxed);
    if (elapsed_us > 0) {
        ops = static_cast<uint64_t>(static_cast<double>(total) * 1000000.0 / elapsed_us);
    }

    if (args.mode == "insert") {
        if (args.random) {
            args.mode = "random_insert";
        } else {
            args.mode = "sequential_insert";
        }
    }
    fmt::println("{},{},{},{},{},{},{}", args.mode, args.threads, args.key_size, args.value_size, ratio, ops,
                 static_cast<uint64_t>(elapsed_us));
    delete handle;
    delete db;
    std::filesystem::remove_all(args.path);
}
