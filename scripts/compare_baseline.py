#!/usr/bin/env python3

import argparse
import sys
import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare mace vs rocksdb from benchmark_results.csv"
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default="./scripts/benchmark_results.csv",
        help="Path to benchmark CSV (default: ./scripts/benchmark_results.csv)",
    )
    parser.add_argument(
        "--filter-errors",
        action="store_true",
        help="Only compare rows with err_op == 0 (default: include all rows)",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path)

    legacy_columns = {
        "total_ops": "total_op",
        "ok_ops": "ok_op",
        "err_ops": "err_op",
    }
    for legacy, current in legacy_columns.items():
        if legacy in df.columns and current not in df.columns:
            df = df.rename(columns={legacy: current})

    required = {
        "engine",
        "workload_id",
        "threads",
        "key_size",
        "value_size",
        "durability_mode",
        "read_path",
        "ops",
        "p99_us",
        "err_op",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in csv: {sorted(missing)}")

    keys = [
        "workload_id",
        "threads",
        "key_size",
        "value_size",
        "durability_mode",
        "read_path",
    ]

    if args.filter_errors:
        base = df[df["err_op"] == 0].copy()
    else:
        base = df.copy()

    if base.empty:
        if args.filter_errors:
            print("No rows with err_op == 0, cannot compare.")
        else:
            print("No rows found in csv, cannot compare.")
        return 0

    agg = base.groupby(keys + ["engine"], as_index=False).agg(
        ops=("ops", "median"),
        p99_us=("p99_us", "median"),
        err_op=("err_op", "median"),
    )

    piv = agg.pivot_table(
        index=keys,
        columns="engine",
        values=["ops", "p99_us", "err_op"],
        aggfunc="first",
    )
    piv.columns = [f"{metric}_{engine}" for metric, engine in piv.columns]
    out = piv.reset_index()

    for col in [
        "ops_mace",
        "ops_rocksdb",
        "p99_us_mace",
        "p99_us_rocksdb",
        "err_op_mace",
        "err_op_rocksdb",
    ]:
        if col not in out.columns:
            out[col] = pd.NA

    out["ops_ratio_mace_over_rocksdb"] = (
        out["ops_mace"] / out["ops_rocksdb"]
    )
    out["p99_ratio_mace_over_rocksdb"] = out["p99_us_mace"] / out["p99_us_rocksdb"]
    out = out.sort_values(keys)

    print(out.to_string(index=False))
    print("\nInterpretation:")
    print("- ops_ratio_mace_over_rocksdb > 1: mace has higher throughput")
    print("- p99_ratio_mace_over_rocksdb < 1: mace has lower p99 latency")

    return 0


if __name__ == "__main__":
    sys.exit(main())
