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
python_bin="${PYTHON_BIN:-${root_dir}/scripts/bin/python}"
if [ ! -x "${python_bin}" ]; then
    python_bin="${PYTHON:-python3}"
fi

db_root="$1"
result_file="${2:-${script_dir}/phase3_results.csv}"

warmup_secs="${WARMUP_SECS:-120}"
measure_secs="${MEASURE_SECS:-300}"
repeats="${REPEATS:-5}"
read_path="${READ_PATH:-snapshot}"

phase3_workloads_raw="${PHASE3_WORKLOADS:-W1 W3 W6}"
phase3_threads_raw="${PHASE3_THREADS:-}"
if [ -z "${phase3_threads_raw}" ]; then
    phase3_threads_raw="$(default_thread_points)"
fi
phase3_durabilities_raw="${PHASE3_DURABILITIES:-relaxed durable}"
key_size="${PHASE3_KEY_SIZE:-32}"
value_size="${PHASE3_VALUE_SIZE:-1024}"
prefill_keys="${PHASE3_PREFILL_KEYS:-18302417}" # tier-m P2

mkdir -p "${db_root}"
mkdir -p "$(dirname -- "${result_file}")"

cargo build --release --manifest-path "${root_dir}/Cargo.toml"
(cd "${root_dir}/rocksdb" && cmake --preset release)
(cd "${root_dir}/rocksdb" && cmake --build --preset release)

IFS=' ' read -r -a workloads <<< "${phase3_workloads_raw}"
IFS=' ' read -r -a threads <<< "${phase3_threads_raw}"
IFS=' ' read -r -a durabilities <<< "${phase3_durabilities_raw}"

if [ "${#workloads[@]}" -eq 0 ] || [ "${#threads[@]}" -eq 0 ] || [ "${#durabilities[@]}" -eq 0 ]; then
    printf "phase3 workloads/threads/durabilities must not be empty\n" >&2
    exit 1
fi

run_case() {
    local engine="$1"
    local workload="$2"
    local t="$3"
    local durability="$4"
    local repeat="$5"
    local run_path

    run_path="$(mktemp -u -p "${db_root}" "${engine}_phase3_${workload}_t${t}_${durability}_r${repeat}_XXXXXX")"

    printf "[phase3][%s] repeat=%s workload=%s threads=%s durability=%s path=%s\n" \
      "${engine}" "${repeat}" "${workload}" "${t}" "${durability}" "${run_path}"

    if [ "${engine}" = "mace" ]; then
        "${root_dir}/target/release/kv_bench" \
          --path "${run_path}" \
          --workload "${workload}" \
          --threads "${t}" \
          --key-size "${key_size}" \
          --value-size "${value_size}" \
          --prefill-keys "${prefill_keys}" \
          --warmup-secs "${warmup_secs}" \
          --measure-secs "${measure_secs}" \
          --shared-keyspace \
          --read-path "${read_path}" \
          --durability "${durability}" \
          --result-file "${result_file}"
    else
        "${root_dir}/rocksdb/build/release/rocksdb_bench" \
          --path "${run_path}" \
          --workload "${workload}" \
          --threads "${t}" \
          --key-size "${key_size}" \
          --value-size "${value_size}" \
          --prefill-keys "${prefill_keys}" \
          --warmup-secs "${warmup_secs}" \
          --measure-secs "${measure_secs}" \
          --read-path "${read_path}" \
          --durability "${durability}" \
          --result-file "${result_file}"
    fi
}

for repeat in $(seq 1 "${repeats}"); do
    for workload in "${workloads[@]}"; do
        for t in "${threads[@]}"; do
            for durability in "${durabilities[@]}"; do
                run_case mace "${workload}" "${t}" "${durability}" "${repeat}"
                run_case rocksdb "${workload}" "${t}" "${durability}" "${repeat}"
            done
        done
    done
done

"${python_bin}" "${script_dir}/phase3_report.py" "${result_file}"
printf "Phase 3 finished. Results: %s\n" "${result_file}"
