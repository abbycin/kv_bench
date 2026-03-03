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
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path)

    required = {
        "engine",
        "workload_id",
        "threads",
        "key_size",
        "value_size",
        "durability_mode",
        "read_path",
        "ops_per_sec",
        "p99_us",
        "error_ops",
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

    ok = df[df["error_ops"] == 0].copy()
    if ok.empty:
        print("No rows with error_ops == 0, cannot compare.")
        return 0

    agg = ok.groupby(keys + ["engine"], as_index=False).agg(
        ops_per_sec=("ops_per_sec", "median"),
        p99_us=("p99_us", "median"),
    )

    piv = agg.pivot_table(
        index=keys,
        columns="engine",
        values=["ops_per_sec", "p99_us"],
        aggfunc="first",
    )
    piv.columns = [f"{metric}_{engine}" for metric, engine in piv.columns]
    out = piv.reset_index()

    for col in [
        "ops_per_sec_mace",
        "ops_per_sec_rocksdb",
        "p99_us_mace",
        "p99_us_rocksdb",
    ]:
        if col not in out.columns:
            out[col] = pd.NA

    out["qps_ratio_mace_over_rocksdb"] = (
        out["ops_per_sec_mace"] / out["ops_per_sec_rocksdb"]
    )
    out["p99_ratio_mace_over_rocksdb"] = out["p99_us_mace"] / out["p99_us_rocksdb"]
    out = out.sort_values(keys)

    print(out.to_string(index=False))
    print("\nInterpretation:")
    print("- qps_ratio_mace_over_rocksdb > 1: mace has higher throughput")
    print("- p99_ratio_mace_over_rocksdb < 1: mace has lower p99 latency")

    return 0


if __name__ == "__main__":
    sys.exit(main())
