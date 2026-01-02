# mace 0.0.23 vs rocksdb 10.4.2

## sequential insert
![mace__sequential_insert](./scripts/mace_sequential_insert.png)

![rocksdb_sequential_insert](./scripts/rocksdb_sequential_insert.png)

## random insert
![mace_random_insert](./scripts/mace_random_insert.png)

![rocksdb_random_insert](./scripts/rocksdb_random_insert.png)

---

## random get (warm get)

![mace_get](./scripts/mace_get.png)

![rocksdb_get](./scripts/rocksdb_get.png)

---

# mixed perfomance (hot get)

![mace_mixed](./scripts/mace_mixed.png)

![rockdb_mixed](./scripts/rocksdb_mixed.png)

# sequential scan (warm scan)

![mace_scan](./scripts/mace_scan.png)

![rocksdb_scan](./scripts/rocksdb_scan.png)
