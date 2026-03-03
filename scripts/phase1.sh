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
result_file="${2:-${script_dir}/phase1_results.csv}"

warmup_secs="${WARMUP_SECS:-120}"
measure_secs="${MEASURE_SECS:-300}"
repeats="${REPEATS:-3}"
read_path="${READ_PATH:-snapshot}"

phase1_workloads_raw="${PHASE1_WORKLOADS:-W1 W3 W6}"
phase1_threads_raw="${PHASE1_THREADS:-}"
if [ -z "${phase1_threads_raw}" ]; then
    phase1_threads_raw="$(default_thread_points)"
fi
phase1_profiles_raw="${PHASE1_PROFILES:-P1 P2 P3 P4}"
phase1_prefill_tier_s_p1="${PHASE1_PREFILL_TIER_S_P1:-44739242}"
phase1_prefill_tier_s_p2="${PHASE1_PREFILL_TIER_S_P2:-6100805}"
phase1_prefill_tier_s_p3="${PHASE1_PREFILL_TIER_S_P3:-1560671}"
phase1_prefill_tier_s_p4="${PHASE1_PREFILL_TIER_S_P4:-392449}"

mkdir -p "${db_root}"
mkdir -p "$(dirname -- "${result_file}")"

cargo build --release --manifest-path "${root_dir}/Cargo.toml"
(cd "${root_dir}/rocksdb" && cmake --preset release)
(cd "${root_dir}/rocksdb" && cmake --build --preset release)

IFS=' ' read -r -a workloads <<< "${phase1_workloads_raw}"
IFS=' ' read -r -a threads <<< "${phase1_threads_raw}"
IFS=' ' read -r -a profiles <<< "${phase1_profiles_raw}"

if [ "${#workloads[@]}" -eq 0 ] || [ "${#threads[@]}" -eq 0 ] || [ "${#profiles[@]}" -eq 0 ]; then
    printf "phase1 workloads/threads/profiles must not be empty\n" >&2
    exit 1
fi

profile_key() {
    case "$1" in
        P1) echo 16 ;;
        P2) echo 32 ;;
        P3) echo 32 ;;
        P4) echo 32 ;;
        *) printf "unknown profile: %s\n" "$1" >&2; exit 1 ;;
    esac
}

profile_val() {
    case "$1" in
        P1) echo 128 ;;
        P2) echo 1024 ;;
        P3) echo 4096 ;;
        P4) echo 16384 ;;
        *) printf "unknown profile: %s\n" "$1" >&2; exit 1 ;;
    esac
}

profile_prefill_tier_s() {
    case "$1" in
        P1) echo "${phase1_prefill_tier_s_p1}" ;;
        P2) echo "${phase1_prefill_tier_s_p2}" ;;
        P3) echo "${phase1_prefill_tier_s_p3}" ;;
        P4) echo "${phase1_prefill_tier_s_p4}" ;;
        *) printf "unknown profile: %s\n" "$1" >&2; exit 1 ;;
    esac
}

run_case() {
    local engine="$1"
    local workload="$2"
    local profile="$3"
    local t="$4"
    local repeat="$5"

    local key_size value_size prefill_keys run_path
    key_size="$(profile_key "${profile}")"
    value_size="$(profile_val "${profile}")"
    prefill_keys="$(profile_prefill_tier_s "${profile}")"
    run_path="$(mktemp -u -p "${db_root}" "${engine}_phase1_${workload}_${profile}_t${t}_r${repeat}_XXXXXX")"

    printf "[phase1][%s] repeat=%s workload=%s profile=%s threads=%s path=%s\n" \
      "${engine}" "${repeat}" "${workload}" "${profile}" "${t}" "${run_path}"

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
          --result-file "${result_file}"
    fi
}

for repeat in $(seq 1 "${repeats}"); do
    for workload in "${workloads[@]}"; do
        for profile in "${profiles[@]}"; do
            for t in "${threads[@]}"; do
                run_case mace "${workload}" "${profile}" "${t}" "${repeat}"
                run_case rocksdb "${workload}" "${profile}" "${t}" "${repeat}"
            done
        done
    done
done

"${python_bin}" "${script_dir}/phase1_eval.py" "${result_file}"
printf "Phase 1 finished. Results: %s\n" "${result_file}"
