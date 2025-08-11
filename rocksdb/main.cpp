#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

#include <algorithm>
#include <barrier>
#include <filesystem>
#include <format>
#include <iostream>
#include <random>
#include <string>

#include "instant.h"

int main()
{
	std::string db_root = "/home/abby/rocksdb_tmp";
	std::filesystem::remove_all(db_root);
	rocksdb::Options options;
	options.disable_auto_compactions = true;
	options.create_if_missing = true;
	options.max_write_buffer_number = 10;
	options.target_file_size_base = 64 << 20;
	options.write_buffer_size = 64 << 20;
	options.level0_file_num_compaction_trigger = 500;
	options.max_bytes_for_level_base = 2 << 30;
	constexpr size_t count = 100000;
	constexpr size_t workers = 4;
	auto ropt = rocksdb::ReadOptions();
	auto wopt = rocksdb::WriteOptions();
	std::vector<std::thread> wg;
	std::vector<std::vector<std::string>> keys {};
	rocksdb::OptimisticTransactionDB *db;
	std::cout << "db_root " << db_root << '\n';
	auto s = rocksdb::OptimisticTransactionDB::Open(options, db_root, &db);
	assert(s.ok());
	std::string val(1024, 'x');

	for (size_t tid = 0; tid < workers; ++tid) {
		std::vector<std::string> key {};

		for (size_t i = 0; i < count; ++i) {
			auto tmp = std::format("key_{}_{}", tid, i);
			tmp.resize(1024, 'x');
			key.push_back(std::move(tmp));
		}
		keys.emplace_back(std::move(key));
	}

	for (size_t tid = 0; tid < workers; ++tid) {
		auto ks = &keys[tid];
		wg.emplace_back(
			[&]
			{
				for (auto &x : *ks) {
					auto kv = db->BeginTransaction(wopt);
					kv->Put(x, x);
					kv->Commit();
					delete kv;
				}
			});
	}

	for (auto &w : wg) {
		w.join();
	}

	delete db;
	wg.clear();

	auto sts =
		rocksdb::OptimisticTransactionDB::Open(options, db_root, &db);
	assert(sts.ok());
	std::barrier g { workers };
	auto b = nm::Instant::now();
	std::mutex mtx {};
	std::atomic<uint64_t> operation { 0 };

	for (size_t tid = 0; tid < workers; ++tid) {
		auto ks = &keys[tid];
		wg.emplace_back(
			[&]
			{
				g.arrive_and_wait();

				std::string val;
				if (mtx.try_lock()) {
					b = nm::Instant::now();
					mtx.unlock();
				}

				for (auto &x : *ks) {
					auto kv = db->BeginTransaction(wopt);
					kv->Get(ropt, x, &val);
					kv->Commit();
					delete kv;
				}
				operation.fetch_add(count,
						    std::memory_order_relaxed);
			});
	}

	for (auto &w : wg) {
		w.join();
	}

	printf("thread %ld\niterations %ld\nkey_size %ld\nvalue_size %ld\nops: "
	       "%.2f\n",
	       workers,
	       count,
	       keys[0][0].size(),
	       val.size(),
	       static_cast<double>(operation.load(std::memory_order_relaxed)) /
		       b.elapse_sec());
	delete db;
}
