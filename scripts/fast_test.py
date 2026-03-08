#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Sequence

def ensure_venv_python() -> None:
    script_path = Path(__file__).resolve()
    script_dir = script_path.parent
    venv_python = script_dir / "bin" / "python"
    if not venv_python.exists():
        return

    current = Path(sys.executable).absolute()
    target = venv_python.absolute()
    if current == target:
        return
    if os.environ.get("FAST_TEST_REEXEC") == "1":
        return

    env = os.environ.copy()
    env["FAST_TEST_REEXEC"] = "1"
    os.execve(str(target), [str(target), str(script_path), *sys.argv[1:]], env)


ensure_venv_python()

import matplotlib.pyplot as plt
import pandas as pd

ENGINE_ORDER = ("mace", "rocksdb")
MODE_PLAN = (
    ("put", "insert"),
    ("get", "get"),
    ("mixed", "mixed"),
    ("scan", "scan"),
)
KV_PROFILES = (
    (16, 128),
    (32, 1024),
    (32, 4096),
    (32, 16384),
)
LINE_STYLES = {"mace": "-", "rocksdb": ":"}


def detect_logical_cpus() -> int:
    count = os.cpu_count() or 1
    return max(1, int(count))


def power_of_two_threads(cpu_count: int) -> list[int]:
    points: list[int] = []
    t = 1
    while t <= cpu_count:
        points.append(t)
        t *= 2
    return points


def format_bytes(size: int) -> str:
    if size >= 1024 and size % 1024 == 0:
        return f"{size // 1024}KB"
    return f"{size}B"


def repo_root_from_script(script_dir: Path) -> Path:
    return script_dir.parent


def build_binaries(repo_root: Path) -> None:
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(repo_root / "Cargo.toml")],
        check=True,
    )
    subprocess.run(["cmake", "--preset", "release"], check=True, cwd=repo_root / "rocksdb")
    subprocess.run(
        ["cmake", "--build", "--preset", "release"], check=True, cwd=repo_root / "rocksdb"
    )


def run_engine_cases(
    *,
    engine: str,
    bench_bin: Path,
    engine_storage_root: Path,
    result_csv: Path,
    thread_points: Sequence[int],
    warmup_secs: int,
    measure_secs: int,
    iterations: int,
    prefill_keys: int,
    read_path: str,
    durability: str,
    insert_ratio: int,
) -> None:
    for mode_display, mode_cli in MODE_PLAN:
        for threads in thread_points:
            for key_size, value_size in KV_PROFILES:
                run_path = engine_storage_root / (
                    f"{engine}_{mode_display}_t{threads}_k{key_size}_v{value_size}_"
                    f"{uuid.uuid4().hex[:8]}"
                )
                args = [
                    str(bench_bin),
                    "--path",
                    str(run_path),
                    "--mode",
                    mode_cli,
                    "--threads",
                    str(threads),
                    "--key-size",
                    str(key_size),
                    "--value-size",
                    str(value_size),
                    "--iterations",
                    str(iterations),
                    "--warmup-secs",
                    str(warmup_secs),
                    "--measure-secs",
                    str(measure_secs),
                    "--read-path",
                    read_path,
                    "--durability",
                    durability,
                    "--result-file",
                    str(result_csv),
                ]

                if mode_cli != "insert":
                    args.extend(["--prefill-keys", str(prefill_keys)])
                if mode_cli == "mixed":
                    args.extend(["--insert-ratio", str(insert_ratio)])
                if engine == "mace":
                    args.append("--shared-keyspace")

                print(
                    f"[run] engine={engine} mode={mode_display} "
                    f"threads={threads} key={key_size} value={value_size}"
                )
                # print(f"{' '.join(args)}")
                subprocess.run(args, check=True)


def annotate_points(x_values: Sequence[int], y_values: Sequence[float], y_max: float, color: str) -> None:
    if y_max <= 0:
        return
    y_offset = 0.035 * y_max
    for x, y in zip(x_values, y_values):
        top_half = y >= y_max * 0.9
        y_pos = y - y_offset if top_half else y + y_offset
        va = "top" if top_half else "bottom"
        plt.text(
            x,
            y_pos,
            f"{y:.0f}",
            fontsize=8,
            ha="center",
            va=va,
            color=color,
            bbox={
                "facecolor": "white",
                "alpha": 0.75,
                "edgecolor": "none",
                "boxstyle": "round,pad=0.2",
            },
        )


def mode_title(mode_display: str, insert_ratio: int) -> str:
    if mode_display == "mixed":
        return f"Mixed ({100 - insert_ratio}% Get, {insert_ratio}% Put)"
    return mode_display.capitalize()


def plot_results(
    *,
    result_csv: Path,
    output_dir: Path,
    thread_points: Sequence[int],
    insert_ratio: int,
) -> list[Path]:
    df = pd.read_csv(result_csv)
    required = {"engine", "mode", "threads", "key_size", "value_size", "ops_per_sec"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in csv: {sorted(missing)}")

    df = df[df["engine"].isin(ENGINE_ORDER) & df["mode"].isin([x[1] for x in MODE_PLAN])]
    if df.empty:
        raise ValueError("No legacy rows found in csv for mace/rocksdb")

    grouped = (
        df.groupby(["engine", "mode", "key_size", "value_size", "threads"], as_index=False)[
            "ops_per_sec"
        ]
        .mean()
        .sort_values(["engine", "mode", "key_size", "value_size", "threads"])
    )

    color_list = plt.get_cmap("tab10").colors
    profile_colors = {
        profile: color_list[idx % len(color_list)] for idx, profile in enumerate(KV_PROFILES)
    }

    output_paths: list[Path] = []
    for mode_display, mode_cli in MODE_PLAN:
        mode_df = grouped[grouped["mode"] == mode_cli]
        if mode_df.empty:
            continue

        plt.figure(figsize=(16, 10))
        y_max = float(mode_df["ops_per_sec"].max()) if not mode_df.empty else 0.0

        for engine in ENGINE_ORDER:
            for key_size, value_size in KV_PROFILES:
                sub = mode_df[
                    (mode_df["engine"] == engine)
                    & (mode_df["key_size"] == key_size)
                    & (mode_df["value_size"] == value_size)
                ].sort_values("threads")
                if sub.empty:
                    continue

                x = sub["threads"].tolist()
                y = sub["ops_per_sec"].tolist()
                label = (
                    f"{engine} ({format_bytes(key_size)}/{format_bytes(value_size)})"
                )
                line_color = profile_colors[(key_size, value_size)]
                plt.plot(
                    x,
                    y,
                    label=label,
                    linestyle=LINE_STYLES.get(engine, "-"),
                    marker="o",
                    markersize=6,
                    linewidth=2,
                    color=line_color,
                )
                annotate_points(x, y, y_max, line_color)

        plt.title(mode_title(mode_display, insert_ratio), fontsize=16)
        plt.xlabel("Threads", fontsize=14)
        plt.ylabel("OPS/s", fontsize=14)
        plt.xticks(list(thread_points), fontsize=12)
        plt.yticks(fontsize=12)
        plt.grid(True, linestyle="--", alpha=0.4)
        plt.legend(fontsize=10)
        plt.tight_layout()

        out = output_dir / f"fast_test_{mode_display}_ops.png"
        plt.savefig(out, dpi=260, bbox_inches="tight")
        plt.close()
        output_paths.append(out)

    return output_paths


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run legacy fast benchmark for mace + rocksdb, then plot mode-level OPS charts."
    )
    parser.add_argument("storage_root", type=Path, help="Root directory for benchmark temporary data")
    parser.add_argument("--warmup-secs", type=int, default=3)
    parser.add_argument("--measure-secs", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=500_000)
    parser.add_argument("--prefill-keys", type=int, default=500_000)
    parser.add_argument("--insert-ratio", type=int, default=30)
    parser.add_argument("--read-path", choices=("snapshot", "rw_txn"), default="snapshot")
    parser.add_argument("--durability", choices=("relaxed", "durable"), default="relaxed")
    parser.add_argument("--csv-name", default="fast_test_results.csv")
    parser.add_argument("--skip-build", action="store_true", help="Skip cargo/cmake build")
    parser.add_argument("--plot-only", action="store_true", help="Skip run and only plot existing csv")
    parser.add_argument("--append", action="store_true", help="Append to existing csv instead of overwrite")
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    repo_root = repo_root_from_script(script_dir)
    result_csv = script_dir / args.csv_name

    cpu_count = detect_logical_cpus()
    thread_points = power_of_two_threads(cpu_count)
    print(f"[info] detected logical cpus={cpu_count}, threads={thread_points}")
    print(f"[info] csv={result_csv}")

    if not args.plot_only:
        if not args.append and result_csv.exists():
            result_csv.unlink()

        args.storage_root.mkdir(parents=True, exist_ok=True)
        mace_root = args.storage_root / "mace"
        rocksdb_root = args.storage_root / "rocksdb"
        mace_root.mkdir(parents=True, exist_ok=True)
        rocksdb_root.mkdir(parents=True, exist_ok=True)

        if not args.skip_build:
            build_binaries(repo_root)

        bench_bins = {
            "mace": repo_root / "target" / "release" / "kv_bench",
            "rocksdb": repo_root / "rocksdb" / "build" / "release" / "rocksdb_bench",
        }
        for engine in ENGINE_ORDER:
            if not bench_bins[engine].exists():
                raise FileNotFoundError(f"benchmark binary not found: {bench_bins[engine]}")

        # Run sequentially: mace first, then rocksdb.
        run_engine_cases(
            engine="mace",
            bench_bin=bench_bins["mace"],
            engine_storage_root=mace_root,
            result_csv=result_csv,
            thread_points=thread_points,
            warmup_secs=args.warmup_secs,
            measure_secs=args.measure_secs,
            iterations=args.iterations,
            prefill_keys=args.prefill_keys,
            read_path=args.read_path,
            durability=args.durability,
            insert_ratio=args.insert_ratio,
        )
        run_engine_cases(
            engine="rocksdb",
            bench_bin=bench_bins["rocksdb"],
            engine_storage_root=rocksdb_root,
            result_csv=result_csv,
            thread_points=thread_points,
            warmup_secs=args.warmup_secs,
            measure_secs=args.measure_secs,
            iterations=args.iterations,
            prefill_keys=args.prefill_keys,
            read_path=args.read_path,
            durability=args.durability,
            insert_ratio=args.insert_ratio,
        )

    outputs = plot_results(
        result_csv=result_csv,
        output_dir=script_dir,
        thread_points=thread_points,
        insert_ratio=args.insert_ratio,
    )
    print("[done] generated charts:")
    for p in outputs:
        print(f"  - {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
