#include <algorithm>
#include <atomic>
#include <print>
#include <random>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
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
        std::println("path is empty");
        return 1;
    }

    if (std::filesystem::exists(args.path)) {
        std::println("path `{}` already exists", args.path);
        return 1;
    }

    if (args.mode != "insert" && args.mode != "get" && args.mode != "mixed") {
        std::println("Error: Invalid mode");
        return 1;
    }

    if (args.key_size < 16 || args.value_size < 16) {
        std::println("Error: key_size or value_size too small, must >= 16");
        return 1;
    }

    if (args.insert_ratio > 100) {
        std::println("Error: Insert ratio must be between 0 and 100");
        return 1;
    }

    rocksdb::Options options;
    options.disable_auto_compactions = true;
    options.create_if_missing = true;
    options.max_write_buffer_number = 10;
    options.target_file_size_base = 64 << 20;
    options.write_buffer_size = 64 << 20;
    options.level0_file_num_compaction_trigger = 500;
    options.max_bytes_for_level_base = 2 << 30;
    auto ropt = rocksdb::ReadOptions();
    auto wopt = rocksdb::WriteOptions();
    std::vector<std::thread> wg;
    std::vector<std::vector<std::string>> keys{};
    std::atomic<uint64_t> total_op{0};
    rocksdb::OptimisticTransactionDB *db;
    auto b = nm::Instant::now();
    std::mutex mtx{};
    auto s = rocksdb::OptimisticTransactionDB::Open(options, args.path, &db);
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


    if (args.mode == "get") {
        auto *kv = db->BeginTransaction(wopt);
        for (size_t tid = 0; tid < args.threads; ++tid) {
            auto *tk = &keys[tid];
            for (auto &key: *tk) {
                kv->Put(key, val);
            }
        }
        kv->Commit();
        delete kv;
    }

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
                    kv->Put(key, val);
                    kv->Commit();
                    delete kv;
                }

            } else if (args.mode == "get") {
                for (auto &key: *tk) {
                    auto *kv = db->BeginTransaction(wopt);
                    kv->Get(ropt, key, &rval);
                    kv->Commit();
                    delete kv;
                }
            } else if (args.mode == "mixed") {
                for (auto &key: *tk) {
                    auto is_insert = dist(gen) < args.insert_ratio;
                    auto *kv = db->BeginTransaction(wopt);
                    if (is_insert) {
                        kv->Put(key, val);
                    } else {
                        kv->Get(ropt, key, &rval); // not found
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
    double ops = static_cast<double>(total_op.load(std::memory_order_relaxed)) / b.elapse_sec();
    std::println("{},{},{},{},{},{:.2f}", args.mode, args.threads, args.key_size, args.value_size, ratio, ops);
    delete db;
    std::filesystem::remove_all(args.path);
}
