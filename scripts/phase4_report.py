#!/usr/bin/env python3

import sys
import pandas as pd


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <restart_csv>")
        return 1

    df = pd.read_csv(sys.argv[1])
    needed = {"cycle", "restart_status", "restart_ready_ms", "worker_exit"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    total = len(df)
    restart_ok = int((df["restart_status"] == 0).sum())
    worker_nonzero = int((df["worker_exit"] != 0).sum())

    print(f"cycles={total}")
    print(f"restart_success={restart_ok}/{total} ({(restart_ok / total * 100.0) if total else 0.0:.1f}%)")
    print(f"worker_nonzero_exit={worker_nonzero}/{total}")

    if total:
        q = df["restart_ready_ms"].quantile([0.5, 0.95, 0.99]).to_dict()
        print(
            "restart_ready_ms: "
            f"p50={q.get(0.5, 0):.1f}, "
            f"p95={q.get(0.95, 0):.1f}, "
            f"p99={q.get(0.99, 0):.1f}, "
            f"max={df['restart_ready_ms'].max():.1f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
