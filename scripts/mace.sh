#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    printf "Usage: %s <storage_root> [result_csv]\n" "$0"
    exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd -- "${script_dir}/.." && pwd)"
# shellcheck source=./thread_points.sh
. "${script_dir}/thread_points.sh"

# The runner creates per-case unique paths under this root; each path must not exist.
db_root="$1"
result_file="${2:-${script_dir}/benchmark_results.csv}"

warmup_secs="${WARMUP_SECS:-3}"
measure_secs="${MEASURE_SECS:-5}"
prefill_keys="${PREFILL_KEYS:-200000}"
read_path="${READ_PATH:-snapshot}"

mkdir -p "${db_root}"
mkdir -p "$(dirname -- "${result_file}")"

cargo build --release --manifest-path "${root_dir}/Cargo.toml"

workloads=(W1 W2 W3 W4 W5 W6)
mace_threads_raw="${MACE_THREADS:-$(default_thread_points)}"
IFS=' ' read -r -a threads <<< "${mace_threads_raw}"
if [ "${#threads[@]}" -eq 0 ]; then
    printf "mace threads list must not be empty\n" >&2
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
            run_path="$(mktemp -u -p "${db_root}" "mace_${workload}_${t}_${key_size}_${value_size}_XXXXXX")"
            printf "[mace] workload=%s threads=%s key=%s value=%s path=%s\n" \
              "${workload}" "${t}" "${key_size}" "${value_size}" "${run_path}"
            "${root_dir}/target/release/kv_bench" \
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
        done
    done
done

printf "Mace runs finished. Results appended to: %s\n" "${result_file}"
