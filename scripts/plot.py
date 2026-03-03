#!/usr/bin/env python3

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print(f"Usage: {sys.argv[0]} <result_csv> [output_dir]")
        return 1

    result_csv = Path(sys.argv[1])
    output_dir = Path(sys.argv[2]) if len(sys.argv) == 3 else result_csv.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(result_csv)

    required = {
        "engine",
        "workload_id",
        "threads",
        "key_size",
        "value_size",
        "ops_per_sec",
        "p99_us",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    for engine in sorted(df["engine"].unique()):
        engine_df = df[df["engine"] == engine]
        profiles = (
            engine_df[["key_size", "value_size"]]
            .drop_duplicates()
            .sort_values(["key_size", "value_size"])
            .itertuples(index=False)
        )

        for key_size, value_size in profiles:
            sub = engine_df[
                (engine_df["key_size"] == key_size)
                & (engine_df["value_size"] == value_size)
            ]
            if sub.empty:
                continue

            for metric, ylabel in (("ops_per_sec", "OPS/s"), ("p99_us", "P99 Latency (us)")):
                plt.figure(figsize=(12, 7))
                for workload in sorted(sub["workload_id"].unique()):
                    wdf = sub[sub["workload_id"] == workload].sort_values("threads")
                    plt.plot(
                        wdf["threads"],
                        wdf[metric],
                        marker="o",
                        linewidth=2,
                        label=workload,
                    )

                plt.title(
                    f"{engine.upper()} {metric} (key={key_size}, value={value_size})",
                    fontsize=14,
                )
                plt.xlabel("Threads")
                plt.ylabel(ylabel)
                plt.grid(True, linestyle="--", alpha=0.5)
                plt.legend()
                plt.tight_layout()
                out = output_dir / f"{engine}_{metric}_k{key_size}_v{value_size}.png"
                plt.savefig(out)
                plt.close()

    print(f"Charts written to: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
