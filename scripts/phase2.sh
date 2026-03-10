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
result_file="${2:-${script_dir}/phase2_results.csv}"

warmup_secs="${WARMUP_SECS:-120}"
measure_secs="${MEASURE_SECS:-300}"
repeats="${REPEATS:-5}"
read_path="${READ_PATH:-snapshot}"
run_tier_l_rep="${RUN_TIER_L_REPRESENTATIVE:-0}"
tier_l_repeats="${TIER_L_REPEATS:-1}"

phase2_workloads_tier_m_raw="${PHASE2_WORKLOADS_TIER_M:-W1 W2 W3 W4 W6}"
phase2_workloads_tier_l_rep_raw="${PHASE2_WORKLOADS_TIER_L_REP:-W1 W3 W6}"
phase2_threads_tier_m_raw="${PHASE2_THREADS_TIER_M:-}"
if [ -z "${phase2_threads_tier_m_raw}" ]; then
    phase2_threads_tier_m_raw="$(default_thread_points)"
fi
phase2_threads_tier_l_rep_raw="${PHASE2_THREADS_TIER_L_REP:-}"
if [ -z "${phase2_threads_tier_l_rep_raw}" ]; then
    phase2_threads_tier_l_rep_raw="$(default_thread_points)"
fi
phase2_profiles_raw="${PHASE2_PROFILES:-P1 P2 P3 P4}"
phase2_prefill_tier_m_p1="${PHASE2_PREFILL_TIER_M_P1:-134217728}"
phase2_prefill_tier_m_p2="${PHASE2_PREFILL_TIER_M_P2:-18302417}"
phase2_prefill_tier_m_p3="${PHASE2_PREFILL_TIER_M_P3:-4682013}"
phase2_prefill_tier_m_p4="${PHASE2_PREFILL_TIER_M_P4:-1177348}"
phase2_prefill_tier_l_p1="${PHASE2_PREFILL_TIER_L_P1:-208783132}"
phase2_prefill_tier_l_p2="${PHASE2_PREFILL_TIER_L_P2:-28470427}"
phase2_prefill_tier_l_p3="${PHASE2_PREFILL_TIER_L_P3:-7283132}"
phase2_prefill_tier_l_p4="${PHASE2_PREFILL_TIER_L_P4:-1831430}"

mkdir -p "${db_root}"
mkdir -p "$(dirname -- "${result_file}")"

cargo build --release --manifest-path "${root_dir}/Cargo.toml"
(cd "${root_dir}/rocksdb" && cmake --preset release)
(cd "${root_dir}/rocksdb" && cmake --build --preset release)

IFS=' ' read -r -a workloads_tier_m <<< "${phase2_workloads_tier_m_raw}"
IFS=' ' read -r -a workloads_tier_l_rep <<< "${phase2_workloads_tier_l_rep_raw}"
IFS=' ' read -r -a threads_tier_m <<< "${phase2_threads_tier_m_raw}"
IFS=' ' read -r -a threads_tier_l_rep <<< "${phase2_threads_tier_l_rep_raw}"
IFS=' ' read -r -a profiles <<< "${phase2_profiles_raw}"

if [ "${#workloads_tier_m[@]}" -eq 0 ] || [ "${#threads_tier_m[@]}" -eq 0 ] || [ "${#profiles[@]}" -eq 0 ]; then
    printf "phase2 tier-m workloads/threads/profiles must not be empty\n" >&2
    exit 1
fi

if [ "${run_tier_l_rep}" = "1" ] && { [ "${#workloads_tier_l_rep[@]}" -eq 0 ] || [ "${#threads_tier_l_rep[@]}" -eq 0 ]; }; then
    printf "phase2 tier-l representative workloads/threads must not be empty when enabled\n" >&2
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

prefill_for() {
    local tier="$1"
    local profile="$2"
    if [ "${tier}" = "tier-m" ]; then
        case "${profile}" in
            P1) echo "${phase2_prefill_tier_m_p1}" ;;
            P2) echo "${phase2_prefill_tier_m_p2}" ;;
            P3) echo "${phase2_prefill_tier_m_p3}" ;;
            P4) echo "${phase2_prefill_tier_m_p4}" ;;
            *) printf "unknown profile: %s\n" "${profile}" >&2; exit 1 ;;
        esac
    elif [ "${tier}" = "tier-l" ]; then
        case "${profile}" in
            P1) echo "${phase2_prefill_tier_l_p1}" ;;
            P2) echo "${phase2_prefill_tier_l_p2}" ;;
            P3) echo "${phase2_prefill_tier_l_p3}" ;;
            P4) echo "${phase2_prefill_tier_l_p4}" ;;
            *) printf "unknown profile: %s\n" "${profile}" >&2; exit 1 ;;
        esac
    else
        printf "unknown tier: %s\n" "${tier}" >&2
        exit 1
    fi
}

run_case() {
    local engine="$1"
    local tier="$2"
    local workload="$3"
    local profile="$4"
    local t="$5"
    local repeat="$6"

    local key_size value_size prefill_keys run_path
    key_size="$(profile_key "${profile}")"
    value_size="$(profile_val "${profile}")"
    prefill_keys="$(prefill_for "${tier}" "${profile}")"
    run_path="$(mktemp -u -p "${db_root}" "${engine}_phase2_${tier}_${workload}_${profile}_t${t}_r${repeat}_XXXXXX")"

    printf "[phase2][%s] tier=%s repeat=%s workload=%s profile=%s threads=%s path=%s\n" \
      "${engine}" "${tier}" "${repeat}" "${workload}" "${profile}" "${t}" "${run_path}"

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

# tier-m full matrix
for repeat in $(seq 1 "${repeats}"); do
    for workload in "${workloads_tier_m[@]}"; do
        for profile in "${profiles[@]}"; do
            for t in "${threads_tier_m[@]}"; do
                run_case mace tier-m "${workload}" "${profile}" "${t}" "${repeat}"
                run_case rocksdb tier-m "${workload}" "${profile}" "${t}" "${repeat}"
            done
        done
    done
done

# tier-l representative subset (optional)
if [ "${run_tier_l_rep}" = "1" ]; then
    for repeat in $(seq 1 "${tier_l_repeats}"); do
        for workload in "${workloads_tier_l_rep[@]}"; do
            for profile in "${profiles[@]}"; do
                for t in "${threads_tier_l_rep[@]}"; do
                    run_case mace tier-l "${workload}" "${profile}" "${t}" "${repeat}"
                    run_case rocksdb tier-l "${workload}" "${profile}" "${t}" "${repeat}"
                done
            done
        done
    done
fi

"${python_bin}" "${script_dir}/phase2_report.py" "${result_file}"
printf "Phase 2 finished. Results: %s\n" "${result_file}"
