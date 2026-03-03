#!/usr/bin/env python3

import sys
import pandas as pd


def cv(series: pd.Series) -> float:
    mean = float(series.mean())
    if mean == 0:
        return 0.0
    return float(series.std(ddof=1) / mean * 100.0)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <result_csv>")
        return 1

    result_csv = sys.argv[1]
    df = pd.read_csv(result_csv)

    needed = {
        "engine",
        "workload_id",
        "key_size",
        "value_size",
        "threads",
        "ops_per_sec",
        "p99_us",
    }
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    sub = df[df["workload_id"].isin(["W1", "W3", "W6"])].copy()
    if sub.empty:
        print("No phase1 rows found in csv")
        return 0

    grp_cols = ["engine", "workload_id", "key_size", "value_size", "threads"]
    agg = (
        sub.groupby(grp_cols)
        .agg(
            repeats=("ops_per_sec", "count"),
            throughput_cv=("ops_per_sec", cv),
            p99_cv=("p99_us", cv),
            throughput_median=("ops_per_sec", "median"),
            p99_median=("p99_us", "median"),
        )
        .reset_index()
    )
    agg["stable"] = (agg["throughput_cv"] <= 10.0) & (agg["p99_cv"] <= 15.0)

    stable_ratio = float(agg["stable"].mean() * 100.0) if len(agg) > 0 else 0.0

    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(agg.to_string(index=False))
    print(f"\nStable cases: {stable_ratio:.1f}% ({agg['stable'].sum()}/{len(agg)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
