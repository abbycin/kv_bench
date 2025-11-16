#include <algorithm>
#include <atomic>
#include <cstdint>
#include <fmt/format.h>
#include <memory>
#include <random>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

#include <barrier>
#include <filesystem>
#include <format>
#include <string>

#include "CLI/CLI.hpp"
#include "instant.h"

struct Args {
    size_t threads;
    size_t iterations;
    size_t key_size;
    size_t value_size;
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
            .insert_ratio = 30,
            .mode = "insert",
            .path = "/tmp/rocksdb_tmp",
    };

    app.add_option("-m,--mode", args.mode, "Mode: insert, get, mixed");
    app.add_option("-t,--threads", args.threads, "Threads");
    app.add_option("-k,--key-size", args.key_size, "Key Size");
    app.add_option("-v,--value-size", args.value_size, "Value Size");
    app.add_option("-i,--iterations", args.iterations, "Iterations");
    app.add_option("-r,--insert-ratio", args.insert_ratio, "Insert Ratio for mixed mode");
    app.add_option("-p,--path", args.path, "DataBase Home");
    app.add_option("--random", args.random, "Shuffle insert keys");

    CLI11_PARSE(app, argc, argv);

    if (args.path.empty()) {
        fmt::println("path is empty");
        return 1;
    }

    if (std::filesystem::exists(args.path)) {
        fmt::println("path `{}` already exists", args.path);
        return 1;
    }

    if (args.mode != "insert" && args.mode != "get" && args.mode != "mixed") {
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

    rocksdb::ColumnFamilyOptions cfo{};
    cfo.enable_blob_files = true;
    cfo.min_blob_size = 8192;
    // use 1GB block cache
    auto cache = rocksdb::NewLRUCache(1 << 30);
    rocksdb::BlockBasedTableOptions table_options{};
    table_options.block_cache = cache;
    cfo.table_factory.reset(NewBlockBasedTableFactory(table_options));
    // the following three options makes it not trigger GC in test
    cfo.level0_file_num_compaction_trigger = 10000;
    cfo.write_buffer_size = 64 << 20;
    cfo.max_write_buffer_number = 16;

    std::vector<rocksdb::ColumnFamilyDescriptor> cfd{};
    cfd.push_back(rocksdb::ColumnFamilyDescriptor("default", cfo));

    rocksdb::DBOptions options;
    options.create_if_missing = true;
    options.allow_concurrent_memtable_write = true;
    options.enable_pipelined_write = true;
    options.env->SetBackgroundThreads(4, rocksdb::Env::Priority::HIGH);

    auto ropt = rocksdb::ReadOptions();
    auto wopt = rocksdb::WriteOptions();
    wopt.no_slowdown = true;
    // wopt.disableWAL = true;
    std::vector<std::thread> wg;
    std::vector<std::vector<std::string>> keys{};
    std::atomic<uint64_t> total_op{0};
    rocksdb::OptimisticTransactionDB *db;
    auto b = nm::Instant::now();
    std::mutex mtx{};
    std::vector<rocksdb::ColumnFamilyHandle *> handles{};
    auto s = rocksdb::OptimisticTransactionDB::Open(options, args.path, cfd, &handles, &db);
    assert(s.ok());
    std::barrier barrier{static_cast<ptrdiff_t>(args.threads)};

    std::random_device rd{};
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 100);


    std::string val(args.value_size, 'x');
    for (size_t tid = 0; tid < args.threads; ++tid) {
        std::vector<std::string> key{};
        for (size_t i = 0; i < args.iterations; ++i) {
            auto tmp = std::format("key_{}_{}", tid, i);
            tmp.resize(args.key_size, 'x');
            key.emplace_back(std::move(tmp));
        }
        if (args.random) {
            std::shuffle(keys.begin(), keys.end(), gen);
        }
        keys.emplace_back(std::move(key));
    }

    auto *handle = handles[0];

    if (args.mode == "get") {
        auto *kv = db->BeginTransaction(wopt);
        for (size_t tid = 0; tid < args.threads; ++tid) {
            auto *tk = &keys[tid];
            for (auto &key: *tk) {
                kv->Put(handle, key, val);
            }
        }
        kv->Commit();
        delete kv;
        delete handle;
        delete db;
        handles.clear();
        // re-open db
        s = rocksdb::OptimisticTransactionDB::Open(options, args.path, cfd, &handles, &db);
        assert(s.ok());
    }

    handle = handles[0];
    for (size_t tid = 0; tid < args.threads; ++tid) {
        auto *tk = &keys[tid];
        wg.emplace_back([&] {
            std::string rval(args.value_size, '0');
            barrier.arrive_and_wait();
            if (mtx.try_lock()) {
                b = nm::Instant::now();
                mtx.unlock();
            }

            if (args.mode == "insert") {
                for (auto &key: *tk) {
                    auto *kv = db->BeginTransaction(wopt);
                    kv->Put(handle, key, val);
                    kv->Commit();
                    delete kv;
                }

            } else if (args.mode == "get") {
                for (auto &key: *tk) {
                    auto *kv = db->BeginTransaction(wopt);
                    kv->Get(ropt, handle, key, &rval);
                    kv->Commit();
                    delete kv;
                }
            } else if (args.mode == "mixed") {
                for (auto &key: *tk) {
                    auto is_insert = dist(gen) < args.insert_ratio;
                    auto *kv = db->BeginTransaction(wopt);
                    if (is_insert) {
                        kv->Put(handle, key, val);
                    } else {
                        kv->Get(ropt, handle, key, &rval); // not found
                    }
                    kv->Commit();
                    delete kv;
                }
            }
            total_op.fetch_add(args.iterations, std::memory_order::relaxed);
        });
    }

    for (auto &w: wg) {
        w.join();
    }
    size_t ratio = [&args] -> size_t {
        if (args.mode == "mixed")
            return args.insert_ratio;
        return args.mode == "insert" ? 100 : 0;
    }();
    uint64_t ops = total_op.load(std::memory_order_relaxed) / b.elapse_sec();
    fmt::println("{},{},{},{},{},{},{}", args.mode, args.threads, args.key_size, args.value_size, ratio, (uint64_t) ops,
                 (uint64_t) b.elapse_ms());
    delete handle;
    delete db;
    std::filesystem::remove_all(args.path);
}
