#!/usr/bin/env python3

import sys
import pandas as pd
import os


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <result_csv>")
        return 1

    df = pd.read_csv(sys.argv[1])

    needed = {
        "engine",
        "workload_id",
        "threads",
        "durability_mode",
        "ops_per_sec",
        "p99_us",
    }
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    target_key_size = int(os.getenv("PHASE3_REPORT_KEY_SIZE", "32"))
    target_value_size = int(os.getenv("PHASE3_REPORT_VALUE_SIZE", "1024"))
    sub = df[
        (df["workload_id"].isin(["W1", "W3", "W6"]))
        & (df["key_size"] == target_key_size)
        & (df["value_size"] == target_value_size)
    ].copy()

    if sub.empty:
        print("No phase3 rows found in csv")
        return 0

    base = (
        sub.groupby(["engine", "workload_id", "threads", "durability_mode"])
        .agg(
            repeats=("ops_per_sec", "count"),
            throughput_median=("ops_per_sec", "median"),
            p99_median=("p99_us", "median"),
        )
        .reset_index()
    )

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(base.to_string(index=False))

    piv_tput = base.pivot_table(
        index=["engine", "workload_id", "threads"],
        columns="durability_mode",
        values="throughput_median",
        aggfunc="first",
    ).reset_index()

    piv_p99 = base.pivot_table(
        index=["engine", "workload_id", "threads"],
        columns="durability_mode",
        values="p99_median",
        aggfunc="first",
    ).reset_index()

    merged = piv_tput.merge(
        piv_p99,
        on=["engine", "workload_id", "threads"],
        suffixes=("_tput", "_p99"),
    )

    for col in ["relaxed_tput", "durable_tput", "relaxed_p99", "durable_p99"]:
        if col not in merged.columns:
            merged[col] = pd.NA

    merged["throughput_drop_pct"] = (
        (1.0 - (merged["durable_tput"] / merged["relaxed_tput"])) * 100.0
    )
    merged["p99_inflation_pct"] = (
        ((merged["durable_p99"] / merged["relaxed_p99"]) - 1.0) * 100.0
    )

    print("\nDurability cost summary:")
    print(
        merged[
            [
                "engine",
                "workload_id",
                "threads",
                "relaxed_tput",
                "durable_tput",
                "throughput_drop_pct",
                "relaxed_p99",
                "durable_p99",
                "p99_inflation_pct",
            ]
        ].to_string(index=False)
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
