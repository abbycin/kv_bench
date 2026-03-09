#!/usr/bin/env python3

import sys
import pandas as pd


def infer_tier(row: pd.Series) -> str:
    key = int(row["key_size"])
    val = int(row["value_size"])
    prefill = int(row["prefill_keys"])

    table = {
        (32, 1024, 18302417): "tier-m",
        (32, 16384, 1177348): "tier-m",
        (32, 1024, 28470427): "tier-l",
        (32, 16384, 1831430): "tier-l",
    }
    return table.get((key, val, prefill), "unknown")


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <result_csv>")
        return 1

    df = pd.read_csv(sys.argv[1])

    needed = {
        "engine",
        "workload_id",
        "key_size",
        "value_size",
        "prefill_keys",
        "threads",
        "ops",
        "p95_us",
        "p99_us",
    }
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    sub = df[df["workload_id"].isin(["W1", "W2", "W3", "W4", "W6"])].copy()
    if sub.empty:
        print("No phase2 rows found in csv")
        return 0

    sub["tier"] = sub.apply(infer_tier, axis=1)

    grp_cols = ["tier", "engine", "workload_id", "key_size", "value_size", "threads"]
    summary = (
        sub.groupby(grp_cols)
        .agg(
            repeats=("ops", "count"),
            throughput_median=("ops", "median"),
            p95_median=("p95_us", "median"),
            p99_median=("p99_us", "median"),
        )
        .reset_index()
    )

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(summary.to_string(index=False))

    # Slow-scenario extraction by throughput median.
    piv = summary.pivot_table(
        index=["tier", "workload_id", "key_size", "value_size", "threads"],
        columns="engine",
        values="throughput_median",
        aggfunc="first",
    ).reset_index()

    if {"mace", "rocksdb"}.issubset(set(piv.columns)):
        piv["slower_engine"] = piv.apply(
            lambda r: "mace" if r["mace"] < r["rocksdb"] else "rocksdb",
            axis=1,
        )
        piv["slower_ratio"] = piv.apply(
            lambda r: min(r["mace"], r["rocksdb"]) / max(r["mace"], r["rocksdb"])
            if max(r["mace"], r["rocksdb"]) > 0
            else 0.0,
            axis=1,
        )
        print("\nSlow scenarios (by throughput median):")
        print(
            piv[
                [
                    "tier",
                    "workload_id",
                    "key_size",
                    "value_size",
                    "threads",
                    "mace",
                    "rocksdb",
                    "slower_engine",
                    "slower_ratio",
                ]
            ].to_string(index=False)
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
