#!/usr/bin/env python3

"""Render a redb-style comparison table (absolute values, best per row bold)
from benchmark_results.csv for pasting into the mace README.

Usage:
    python3 scripts/csv_to_readme.py [--csv PATH] [--key-size 16] [--value-size 128]
        [--threads 1,2,4,8] [--engines mace,rocksdb]

Only successful operations count: ops/s = ok_op / elapsed. p99_us is the
99th-percentile latency reported by the run. Rows are aggregated by median
across repeated runs of the same configuration.
"""

import argparse
import sys

import pandas as pd

WORKLOAD_TEMPLATE = [
    ("W1", "`W1` (95R/5U, uniform)"),
    ("W2", "`W2` (95R/5U, zipf)"),
    ("W3", "`W3` (50R/50U)"),
    ("W4", "`W4` (5R/95U)"),
    ("W5", "`W5` (70R/25U/5S)"),
    ("W6", "`W6` (100% scan)"),
    ("MERGE_70", "`MERGE_70` (30% get / 70% merge)"),
    ("MERGE_100", "`MERGE_100` (100% merge)"),
    ("MERGE_GET_0", "`MERGE_GET_0` (fresh get)"),
    ("MERGE_GET_1000", "`MERGE_GET_1000` (1k operand chain)"),
    ("MERGE_GET_10000", "`MERGE_GET_10000` (10k operand chain)"),
]
WORKLOAD_LABELS = dict(WORKLOAD_TEMPLATE)
DEFAULT_ENGINE_ORDER = ["mace", "rocksdb"]
ENGINE_DISPLAY = {
    "mace": "Mace",
    "rocksdb": "RocksDB",
    "redb": "redb",
    "sqlite": "SQLite",
    "lmdb": "LMDB",
    "fjall": "fjall",
}


def fmt_ops(value: float) -> str:
    return f"{value:,.0f}"


def fmt_p99(value: float) -> str:
    return f"{value:,.0f}"


def build_mix_map(rows: list[dict]) -> dict:
    """per-workload '70% Read, 25% Update, 5% Scan[, zipf]' text from csv columns"""
    mix_map: dict[str, str] = {}
    seen: set[str] = set()
    for row in rows:
        workload_id = str(row["workload_id"])
        if workload_id in seen or workload_id.startswith("MERGE"):
            continue
        seen.add(workload_id)
        parts = []
        for column, name in (
            ("read_pct", "Read"),
            ("update_pct", "Update"),
            ("scan_pct", "Scan"),
        ):
            value = row.get(column)
            if value is not None and not pd.isna(value) and float(value) > 0:
                parts.append(f"{int(float(value))}% {name}")
        distribution = row.get("distribution")
        if (
            distribution is not None
            and not pd.isna(distribution)
            and str(distribution).strip() not in ("", "uniform")
        ):
            parts.append(str(distribution).strip())
        if parts:
            mix_map[workload_id] = ", ".join(parts)
    return mix_map


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="./scripts/benchmark_results.csv")
    parser.add_argument("--key-size", type=int, default=16)
    parser.add_argument("--value-size", type=int, default=128)
    parser.add_argument(
        "--threads",
        default="",
        help="comma-separated thread counts; default: all present at the slice",
    )
    parser.add_argument(
        "--engines",
        default="",
        help="comma-separated engine columns; default: mace first, then sorted",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv)

    for legacy, current in {"total_ops": "total_op", "ok_ops": "ok_op"}.items():
        if legacy in df.columns and current not in df.columns:
            df = df.rename(columns={legacy: current})
    for col in ["engine", "workload_id", "threads", "key_size", "value_size",
                "ok_op", "elapsed_us", "p99_us", "ops"]:
        if col not in df.columns:
            raise ValueError(f"missing column in csv: {col}")

    # successful throughput only; fall back to total when ok_op is absent
    df["ok_ops_s"] = df["ok_op"] / df["elapsed_us"] * 1e6
    df["ops_s"] = df["ops"] * 1e6 / df["elapsed_us"]
    df["throughput"] = df["ok_ops_s"].where(df["ok_op"] >= 0, df["ops_s"])

    threads = (
        [int(t) for t in args.threads.split(",") if t.strip() != ""]
        if args.threads
        else sorted(df["threads"].unique().tolist())
    )
    slice_df = df[
        (df["key_size"] == args.key_size)
        & (df["value_size"] == args.value_size)
        & (df["threads"].isin(threads))
    ]
    if slice_df.empty:
        print(
            f"no rows for key_size={args.key_size} value_size={args.value_size} "
            f"threads={threads}",
            file=sys.stderr,
        )
        return 1

    observed_engines = sorted(
        slice_df["engine"].dropna().astype(str).unique().tolist()
    )
    if args.engines:
        wanted = [e.strip() for e in args.engines.split(",") if e.strip()]
        engines = [e for e in wanted if e in observed_engines] + [
            e for e in observed_engines if e not in wanted
        ]
    else:
        engines = [e for e in DEFAULT_ENGINE_ORDER if e in observed_engines]
        engines += [e for e in observed_engines if e not in engines]
    if len(engines) < 2:
        print(f"need at least two engines, found: {observed_engines}", file=sys.stderr)
        return 1

    observed_workloads = sorted(
        slice_df["workload_id"].dropna().astype(str).unique().tolist()
    )
    workload_order = [w for w, _ in WORKLOAD_TEMPLATE if w in observed_workloads]
    workload_order += [w for w in observed_workloads if w not in workload_order]

    agg = (
        slice_df.groupby(["workload_id", "threads", "engine"], as_index=False)
        .agg(throughput=("throughput", "median"), p99_us=("p99_us", "median"))
    )
    key = lambda r: (r["workload_id"], r["threads"])
    by_cell = {
        (row["workload_id"], row["threads"], row["engine"]): row
        for row in agg.to_dict("records")
    }

    def display_name(engine: str) -> str:
        return ENGINE_DISPLAY.get(engine, engine.capitalize())

    def workload_cell(workload_id: str, thread_count: int) -> str:
        thread_word = "thread" if thread_count == 1 else "threads"
        mix = mix_map.get(workload_id)
        if mix is not None:
            return f"{workload_id} ({mix}, {thread_count} {thread_word})"
        label = WORKLOAD_LABELS.get(workload_id, f"`{workload_id}`")
        return f"{label}, {thread_count} {thread_word}"

    mix_map = build_mix_map(slice_df.to_dict("records"))
    cols = ["Workload"]
    cols += [f"{display_name(e)} ops/s" for e in engines]
    if len(engines) == 2:
        cols.append(f"{engines[0]}/{engines[1]} ops")
    cols += [f"{display_name(e)} p99" for e in engines]

    lines = ["| " + " | ".join(cols) + " |"]
    sep = ["---"] + [":---"] * (len(cols) - 1)
    lines += ["| " + " | ".join(sep) + " |"]
    for workload_id in workload_order:
        for thread_count in sorted(threads):
            cells = {}
            ok = True
            for engine in engines:
                row = by_cell.get((workload_id, thread_count, engine))
                if row is None:
                    ok = False
                    break
                cells[engine] = row
            if not ok:
                continue

            ops_best = max(cells[e]["throughput"] for e in engines)
            p99_best = min(cells[e]["p99_us"] for e in engines)
            row_cells = [workload_cell(workload_id, thread_count)]
            for engine in engines:
                value = cells[engine]["throughput"]
                text = fmt_ops(value)
                row_cells.append(f"**{text}**" if value == ops_best else text)
            if len(engines) == 2:
                e0, e1 = engines
                ratio = cells[e0]["throughput"] / cells[e1]["throughput"]
                row_cells.append(f"{ratio:.2f}x")
            for engine in engines:
                value = cells[engine]["p99_us"]
                text = fmt_p99(value)
                row_cells.append(f"**{text}**" if value == p99_best else text)
            lines.append("| " + " | ".join(row_cells) + " |")

    print("\n".join(lines))
    print()
    print(f"_Dataset: {args.key_size}B key / {args.value_size}B value, "
          "1M keys, relaxed durability, snapshot reads. ops/s = successful "
          "operations per second (higher is better), p99 = 99th-percentile "
          "latency in µs (lower is better). Bold marks the best value per row._")
    return 0


if __name__ == "__main__":
    sys.exit(main())
