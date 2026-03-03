# kv_bench 执行记录（benchmark_refactor）

## Phase 0（已完成）
- 日期：2026-03-03
- 范围：
  - 重构 `src/main.rs` 与 `rocksdb/main.cpp`，完成 v2 方法学最小清单：
    - workload preset：`W1..W6`
    - mixed/read/scan 的 prefill + shared keyspace
    - 时长模式：`--warmup-secs` / `--measure-secs`
    - 显式 read path parity：`--read-path snapshot|rw_txn`
    - 统一 schema 结果落盘（CSV）并自动附带机器/环境元数据
  - 更新脚本：`scripts/mace.sh`、`scripts/rocksdb.sh`、`scripts/plot.py`、`scripts/init.sh`
  - 默认数据目录切换为 `/nvme` 体系（脚本强制 db_root 在 `/nvme` 下）
- 编译验证：
  - `cargo check -q` 通过
  - `cargo build --release -q` 通过
  - `cmake --build --preset release -j` 通过
- 运行烟测：
  - `mace` 与 `rocksdb` 均可按新参数运行并写入统一 schema 结果文件
- 提交：`0649db5` (`phase0: align benchmark v2 workload protocol`)

## Phase 1（已完成）
- 日期：2026-03-03
- 范围：
  - 新增 `scripts/phase1.sh`：按文档矩阵执行小规模试跑
    - dataset：`tier-s`
    - workload：`W1/W3/W6`
    - profile：`P2/P3`
    - threads：`1/12`
    - repeats：默认 `3`（可由 `REPEATS` 覆盖）
  - 新增 `scripts/phase1_eval.py`：按 case 聚合并计算
    - throughput CV
    - p99 CV
    - 稳定性通过率（门槛：throughput CV<=10%, p99 CV<=15%）
- 验证：
  - `bash -n scripts/phase1.sh` 通过
  - `python3 -m py_compile scripts/phase1_eval.py` 通过
- 提交：`436e813` (`phase1: add trial matrix runner and cv evaluator`)

## Phase 2（已完成）
- 日期：2026-03-03
- 范围：
  - 新增 `scripts/phase2.sh`：稳态核心报告矩阵执行器
    - `tier-m` 全量：`W1/W2/W3/W4/W6` × `P2/P3` × `threads(1/6/12)` × `repeats(默认5)`
    - 可选 `tier-l` 代表集：`RUN_TIER_L_REPRESENTATIVE=1` 启用，默认 `TIER_L_REPEATS=1`
  - 新增 `scripts/phase2_report.py`：输出按 case 的 `throughput/p95/p99 median`，并给出慢场景对比表
- 验证：
  - `bash -n scripts/phase2.sh` 通过
  - `python3 -m py_compile scripts/phase2_report.py` 通过
- 提交：`f0fd573` (`phase2: add steady-state matrix runner and report`)

## Phase 3（已完成）
- 日期：2026-03-03
- 范围：
  - 扩展 `src/main.rs` 与 `rocksdb/main.cpp`：
    - 新增 `--durability relaxed|durable`
    - 统一结果 schema 新增 `durability_mode` 字段
  - 新增 `scripts/phase3.sh`：耐久性成本矩阵执行器
    - dataset：`tier-m`
    - workload：`W1/W3/W6`
    - profile：`P2`
    - threads：`1/12`
    - repeats：默认 `5`
    - durability：`relaxed` 与 `durable`
  - 新增 `scripts/phase3_report.py`：
    - 输出 `throughput_drop_pct`
    - 输出 `p99_inflation_pct`
- 验证：
  - `cargo check -q` 通过
  - `cmake --build --preset release -j` 通过
  - `bash -n scripts/phase3.sh` 通过
  - `python3 -m py_compile scripts/phase3_report.py` 通过
- 提交：`4b7c511` (`phase3: add durability mode matrix and cost report`)

## Phase 4（已完成）
- 日期：2026-03-03
- 范围：
  - 扩展 `src/main.rs` 与 `rocksdb/main.cpp`：
    - 新增 `--reuse-path`（允许复用已存在数据目录）
    - 新增 `--skip-prefill`（重启后直接用既有数据集）
  - 新增 `scripts/phase4_soak.sh`：
    - baseline：`tier-m + W3 + P2 + 12 threads`
    - 周期性 `kill -9` + restart 验证（默认每 30 分钟）
    - 记录 `restart_ready_ms` 与退出状态到 `phase4_restart.csv`
  - 新增 `scripts/phase4_report.py`：
    - 汇总 restart 成功率
    - 输出 restart ready 时间分位（p50/p95/p99/max）
- 验证：
  - `cargo check -q` 通过
  - `cmake --build --preset release -j` 通过
  - `bash -n scripts/phase4_soak.sh` 通过
  - `python3 -m py_compile scripts/phase4_report.py` 通过
- 提交：`536a3f8` (`phase4: add soak/restart runner and recovery checks`)
