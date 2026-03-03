#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 4 ]; then
    printf "Usage: %s <engine:mace|rocksdb> <db_path> [result_csv] [restart_csv]\n" "$0"
    exit 1
fi

engine="$1"
db_path="$2"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd -- "${script_dir}/.." && pwd)"
python_bin="${PYTHON_BIN:-${root_dir}/scripts/bin/python}"
if [ ! -x "${python_bin}" ]; then
    python_bin="${PYTHON:-python3}"
fi

result_file="${3:-${script_dir}/phase4_results.csv}"
restart_file="${4:-${script_dir}/phase4_restart.csv}"

if [[ "${engine}" != "mace" && "${engine}" != "rocksdb" ]]; then
    printf "engine must be mace or rocksdb\n" >&2
    exit 1
fi

mkdir -p "$(dirname -- "${db_path}")"
mkdir -p "$(dirname -- "${result_file}")"
mkdir -p "$(dirname -- "${restart_file}")"

soak_hours="${SOAK_HOURS:-12}"
crash_interval_secs="${CRASH_INTERVAL_SECS:-1800}"
verify_measure_secs="${VERIFY_MEASURE_SECS:-30}"
run_measure_secs="${RUN_MEASURE_SECS:-3600}"
seed_measure_secs="${SEED_MEASURE_SECS:-5}"
warmup_secs="${WARMUP_SECS:-30}"
max_cycles="${PHASE4_MAX_CYCLES:-0}"

# baseline: tier-m + W3 + P2 + 12 threads
workload_main="${PHASE4_WORKLOAD_MAIN:-W3}"
workload_verify="${PHASE4_WORKLOAD_VERIFY:-W6}"
threads="${PHASE4_THREADS:-12}"
key_size="${PHASE4_KEY_SIZE:-32}"
value_size="${PHASE4_VALUE_SIZE:-1024}"
prefill_keys="${PHASE4_PREFILL_KEYS:-18302417}"
read_path="${READ_PATH:-snapshot}"
durability="${DURABILITY:-relaxed}"

start_epoch="$(date +%s)"
end_epoch="$((start_epoch + soak_hours * 3600))"

run_cmd() {
    local workload="$1"
    local measure_secs="$2"
    local skip_prefill="$3"

    if [ "${engine}" = "mace" ]; then
        if [ "${skip_prefill}" = "1" ]; then
            "${root_dir}/target/release/kv_bench" \
              --path "${db_path}" \
              --workload "${workload}" \
              --threads "${threads}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --shared-keyspace \
              --no-cleanup \
              --read-path "${read_path}" \
              --durability "${durability}" \
              --reuse-path \
              --skip-prefill \
              --result-file "${result_file}"
        else
            "${root_dir}/target/release/kv_bench" \
              --path "${db_path}" \
              --workload "${workload}" \
              --threads "${threads}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --shared-keyspace \
              --no-cleanup \
              --read-path "${read_path}" \
              --durability "${durability}" \
              --reuse-path \
              --result-file "${result_file}"
        fi
    else
        if [ "${skip_prefill}" = "1" ]; then
            "${root_dir}/rocksdb/build/release/rocksdb_bench" \
              --path "${db_path}" \
              --workload "${workload}" \
              --threads "${threads}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --read-path "${read_path}" \
              --durability "${durability}" \
              --no-cleanup \
              --reuse-path \
              --skip-prefill \
              --result-file "${result_file}"
        else
            "${root_dir}/rocksdb/build/release/rocksdb_bench" \
              --path "${db_path}" \
              --workload "${workload}" \
              --threads "${threads}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --read-path "${read_path}" \
              --durability "${durability}" \
              --no-cleanup \
              --reuse-path \
              --result-file "${result_file}"
        fi
    fi
}

start_run_bg() {
    local workload="$1"
    local measure_secs="$2"
    local skip_prefill="$3"

    if [ "${engine}" = "mace" ]; then
        if [ "${skip_prefill}" = "1" ]; then
            "${root_dir}/target/release/kv_bench" \
              --path "${db_path}" \
              --workload "${workload}" \
              --threads "${threads}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --shared-keyspace \
              --no-cleanup \
              --read-path "${read_path}" \
              --durability "${durability}" \
              --reuse-path \
              --skip-prefill \
              --result-file "${result_file}" &
        else
            "${root_dir}/target/release/kv_bench" \
              --path "${db_path}" \
              --workload "${workload}" \
              --threads "${threads}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --shared-keyspace \
              --no-cleanup \
              --read-path "${read_path}" \
              --durability "${durability}" \
              --reuse-path \
              --result-file "${result_file}" &
        fi
    else
        if [ "${skip_prefill}" = "1" ]; then
            "${root_dir}/rocksdb/build/release/rocksdb_bench" \
              --path "${db_path}" \
              --workload "${workload}" \
              --threads "${threads}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --read-path "${read_path}" \
              --durability "${durability}" \
              --no-cleanup \
              --reuse-path \
              --skip-prefill \
              --result-file "${result_file}" &
        else
            "${root_dir}/rocksdb/build/release/rocksdb_bench" \
              --path "${db_path}" \
              --workload "${workload}" \
              --threads "${threads}" \
              --key-size "${key_size}" \
              --value-size "${value_size}" \
              --prefill-keys "${prefill_keys}" \
              --warmup-secs "${warmup_secs}" \
              --measure-secs "${measure_secs}" \
              --read-path "${read_path}" \
              --durability "${durability}" \
              --no-cleanup \
              --reuse-path \
              --result-file "${result_file}" &
        fi
    fi
    runner_pid=$!
}

if [ ! -f "${restart_file}" ]; then
    printf "cycle,start_epoch,kill_sent,worker_exit,restart_status,restart_ready_ms\n" > "${restart_file}"
fi

# seed dataset once (with prefill)
printf "[phase4][%s] seed dataset at %s\n" "${engine}" "${db_path}"
run_cmd "${workload_main}" "${seed_measure_secs}" 0

cycle=0
while [ "$(date +%s)" -lt "${end_epoch}" ]; do
    if [ "${max_cycles}" -gt 0 ] && [ "${cycle}" -ge "${max_cycles}" ]; then
        break
    fi
    cycle="$((cycle + 1))"
    cycle_start="$(date +%s)"
    printf "[phase4][%s] cycle=%s start=%s\n" "${engine}" "${cycle}" "${cycle_start}"

    # long run in background; kill after interval
    start_run_bg "${workload_main}" "${run_measure_secs}" 1

    sleep "${crash_interval_secs}"

    kill_sent=0
    if kill -0 "${runner_pid}" 2>/dev/null; then
        kill -9 "${runner_pid}" || true
        kill_sent=1
    fi

    set +e
    wait "${runner_pid}"
    worker_exit=$?
    set -e

    restart_start_ms="$(date +%s%3N)"
    restart_status=0
    set +e
    run_cmd "${workload_verify}" "${verify_measure_secs}" 1
    restart_status=$?
    set -e
    restart_end_ms="$(date +%s%3N)"
    restart_ready_ms="$((restart_end_ms - restart_start_ms))"

    printf "%s,%s,%s,%s,%s,%s\n" \
      "${cycle}" "${cycle_start}" "${kill_sent}" "${worker_exit}" "${restart_status}" "${restart_ready_ms}" >> "${restart_file}"
done

"${python_bin}" "${script_dir}/phase4_report.py" "${restart_file}"
printf "Phase 4 soak finished. Results: %s | Restart log: %s\n" "${result_file}" "${restart_file}"
