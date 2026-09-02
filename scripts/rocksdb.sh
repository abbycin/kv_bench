#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    printf "Usage: %s <storage_root> [result_csv]\n" "$0"
    exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd -- "${script_dir}/.." && pwd)"
rocksdb_dir="${root_dir}/rocksdb"
# shellcheck source=./thread_points.sh
. "${script_dir}/thread_points.sh"

db_root="$1"
result_file="${2:-${script_dir}/benchmark_results.csv}"

warmup_secs="${WARMUP_SECS:-0}"
measure_secs="${MEASURE_SECS:-10}"
prefill_keys="${PREFILL_KEYS:-1000000}"
read_path="${READ_PATH:-snapshot}"

mkdir -p "${db_root}"
mkdir -p "$(dirname -- "${result_file}")"

(cd "${rocksdb_dir}" && cmake --preset release)
(cd "${rocksdb_dir}" && cmake --build --preset release)

workloads=(W1 W2 W3 W4 W5 W6)
rocksdb_threads_raw="${ROCKSDB_THREADS:-$(default_thread_points)}"
IFS=' ' read -r -a threads <<< "${rocksdb_threads_raw}"
if [ "${#threads[@]}" -eq 0 ]; then
    printf "rocksdb threads list must not be empty\n" >&2
    exit 1
fi
profiles=(
  "16 128"
  "32 1024"
  "32 4096"
  "32 16384"
)

for workload in "${workloads[@]}"; do
    for t in "${threads[@]}"; do
        for kv in "${profiles[@]}"; do
            read -r key_size value_size <<< "${kv}"
            run_path="$(mktemp -u -p "${db_root}" "rocksdb_${workload}_${t}_${key_size}_${value_size}_XXXXXX")"
            printf "[rocksdb] workload=%s threads=%s key=%s value=%s path=%s\n" \
              "${workload}" "${t}" "${key_size}" "${value_size}" "${run_path}"
            "${rocksdb_dir}/build/release/rocksdb_bench" \
              --path "${run_path}" \
              --workload "${workload}" \
              --threads "${t}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --read-path "${read_path}" \
              --result-file "${result_file}"
            sleep 20
        done
    done
done

# --- merge operator benchmarks (key size fixed 32) ---
merge_key_size=32
merge_num_keys="${MERGE_NUM_KEYS:-${prefill_keys}}"

# 1) mixed-ratio merges: 70% and 100% merge, uniform keys
for t in "${threads[@]}"; do
    for ratio in 0.7 1.0; do
        run_path="$(mktemp -u -p "${db_root}" "rocksdb_merge_${t}_${ratio}_XXXXXX")"
        printf "[rocksdb] merge ratio=%s threads=%s key=%s num_keys=%s path=%s\n" \
          "${ratio}" "${t}" "${merge_key_size}" "${merge_num_keys}" "${run_path}"
        "${rocksdb_dir}/build/release/rocksdb_merge_bench" \
          --path "${run_path}" \
          --threads "${t}" \
          --key-size "${merge_key_size}" \
          --num-keys "${merge_num_keys}" \
          --merge-ratio "${ratio}" \
          --warmup-secs "${warmup_secs}" \
          --measure-secs "${measure_secs}" \
          --result-file "${result_file}"
        sleep 20
    done
done

# 2) get after accumulated operands: long chains, then 100% gets
accumulate_keys="${ACCUMULATE_KEYS:-100}"
IFS=' ' read -r -a accumulate_levels <<< "${ACCUMULATE_LEVELS:-0 1000 10000}"
if [ "${#accumulate_levels[@]}" -eq 0 ]; then
    printf "accumulate levels list must not be empty\n" >&2
    exit 1
fi
for t in "${threads[@]}"; do
    for level in "${accumulate_levels[@]}"; do
        run_path="$(mktemp -u -p "${db_root}" "rocksdb_merge_get_${t}_${level}_XXXXXX")"
        printf "[rocksdb] merge-get accumulate=%s/key threads=%s key=%s num_keys=%s path=%s\n" \
          "${level}" "${t}" "${merge_key_size}" "${accumulate_keys}" "${run_path}"
        "${rocksdb_dir}/build/release/rocksdb_merge_bench" \
          --path "${run_path}" \
          --threads "${t}" \
          --key-size "${merge_key_size}" \
          --num-keys "${accumulate_keys}" \
          --merge-ratio 0.0 \
          --accumulate-per-key "${level}" \
          --warmup-secs "${warmup_secs}" \
          --measure-secs "${measure_secs}" \
          --result-file "${result_file}"
        sleep 20
    done
done

printf "RocksDB runs finished. Results appended to: %s\n" "${result_file}"
