#!/usr/bin/env bash

# Shared helpers for choosing default power-of-two thread points from host CPU count.

detect_logical_cpus() {
    local detected

    if detected="$(nproc 2>/dev/null)"; then
        :
    elif detected="$(getconf _NPROCESSORS_ONLN 2>/dev/null)"; then
        :
    else
        detected=1
    fi

    if [[ ! "${detected}" =~ ^[0-9]+$ ]] || [ "${detected}" -lt 1 ]; then
        detected=1
    fi

    printf "%s\n" "${detected}"
}

is_power_of_two() {
    local n="$1"
    [ "${n}" -gt 0 ] && [ $((n & (n - 1))) -eq 0 ]
}

default_thread_points() {
    local cpu_count="${1:-$(detect_logical_cpus)}"
    local points=()
    local t=1

    while [ "${t}" -le "${cpu_count}" ]; do
        points+=("${t}")
        t=$((t * 2))
    done

    printf "%s\n" "${points[*]}"
}
