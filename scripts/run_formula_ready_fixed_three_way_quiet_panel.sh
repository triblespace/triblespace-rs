#!/usr/bin/env bash
# Verify or execute the frozen Formula Ready B/D/C panel.
#
# `verify` is clock-free: it checks only source, lock, binary, and embedded
# identity. `run` is deliberately gated because the benchmark emits timing
# observations that are meaningful only after the operator has established a
# quiet machine. The timed order is the mirrored B-D-C-C-D-B schedule.

set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage:
  run_formula_ready_fixed_three_way_quiet_panel.sh verify CAPSULE
  FORMULA_READY_MACHINE_QUIET=YES \
    run_formula_ready_fixed_three_way_quiet_panel.sh run CAPSULE OUTPUT [REPETITIONS]
EOF
    exit 64
}

if [[ $# -lt 2 ]]; then
    usage
fi

mode=$1
capsule=$2

if [[ ! -d "$capsule" ]]; then
    echo "capsule is not a directory: $capsule" >&2
    exit 66
fi

capsule=$(cd "$capsule" && pwd -P)

labels=(B D C)
revisions=(
    0c9a0856+530fa951
    a80348b2+530fa951
    89171fab+530fa951
)
binaries=(
    B-0c9a0856+530fa951
    D-a80348b2+530fa951
    C-89171fab+530fa951
)
binary_sha256=(
    90e3f0a83d70aac4cd6f5a0ed462837703283abf31ab976bd393d2af3f9778da
    6a2845d855c7ed04da295b1e0ec230243d07173d73b54eae6ec2a6a5af207854
    7f298c5f3b0436877dbeee6bb2fe788665bba74d7ca85cc59058ef9929c5cb5c
)

harness_sha256=bc1ba64179986d674792ee106ead9f3654ad81fe78b7fdf1e810d855e0675f3d
lock_sha256=cb7af026a2ea4ff260e799fe880ab6ca9cd78869aa12a08bb82310e7179f1766
engine_line='engine: current whole-root residual'
oracle_line='oracle parity: all seven query/backend cells exact'

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

require_sha256() {
    local path=$1
    local expected=$2
    local actual

    if [[ ! -f "$path" ]]; then
        echo "missing frozen artifact: $path" >&2
        exit 66
    fi
    actual=$(sha256 "$path")
    if [[ "$actual" != "$expected" ]]; then
        echo "SHA-256 mismatch for $path: $actual != $expected" >&2
        exit 65
    fi
}

verify_capsule() {
    require_sha256 \
        "$capsule/source/query_engine_generation_bench.rs" \
        "$harness_sha256"
    require_sha256 "$capsule/source/Cargo.lock.frozen" "$lock_sha256"

    local index
    for index in "${!labels[@]}"; do
        local binary="$capsule/bin/${binaries[$index]}"
        require_sha256 "$binary" "${binary_sha256[$index]}"
        if [[ ! -x "$binary" ]]; then
            echo "frozen binary is not executable: $binary" >&2
            exit 65
        fi
        strings "$binary" | grep -F 'current whole-root residual' > /dev/null
        strings "$binary" | grep -F "${revisions[$index]}" > /dev/null
        strings "$binary" \
            | grep -F 'oracle parity: all seven query/backend cells exact' \
                > /dev/null
    done

    printf 'VERIFIED: fixed Formula Ready B/D/C capsule identity is exact\n'
    printf 'NOTICE: verify mode did not execute benchmark code or inspect clocks\n'
}

validate_log() {
    local label=$1
    local revision=$2
    local repetitions=$3
    local path=$4

    [[ $(wc -l < "$path" | tr -d ' ') == 68 ]]
    [[ $(grep -Fxc "$engine_line" "$path") == 1 ]]
    [[ $(grep -Fxc "revision: $revision" "$path") == 1 ]]
    [[ $(grep -Fxc "samples: $repetitions; hot cache; release profile" "$path") == 1 ]]
    [[ $(grep -Fxc "$oracle_line" "$path") == 1 ]]
    [[ $(grep -Ec '^fixture: 32 components x 64 nodes, fanout 2, 11776 tribles; built in .+; archive built in .+ \(excluded\)$' "$path") == 1 ]]
    [[ $(grep -Ec '^.+  \([0-9]+ rows\)$' "$path") == 7 ]]
    [[ $(grep -Ec '^  full drain.*checksum 0x[0-9a-f]{16}$' "$path") == 7 ]]

    grep -Fqx 'finite OR-of-AND / TribleSet  (2048 rows)' "$path"
    grep -Fqx 'finite OR-of-AND / SuccinctArchive  (2048 rows)' "$path"
    grep -Fqx 'recursive AND/OR / TribleSet  (4096 rows)' "$path"
    grep -Fqx 'recursive AND/OR / SuccinctArchive  (4096 rows)' "$path"
    grep -Fqx 'cyclic RPQ / TribleSet  (131072 rows)' "$path"
    grep -Fqx 'formula + cyclic RPQ / TribleSet sibling  (65536 rows)' "$path"
    grep -Fqx 'formula + cyclic RPQ / SuccinctArchive sibling  (65536 rows)' "$path"

    [[ $(grep -Ec '^  full drain.*checksum 0x53c1305cbef38e70$' "$path") == 2 ]]
    [[ $(grep -Ec '^  full drain.*checksum 0x0e6f207fe857fad7$' "$path") == 2 ]]
    [[ $(grep -Ec '^  full drain.*checksum 0x6971959d9651157d$' "$path") == 1 ]]
    [[ $(grep -Ec '^  full drain.*checksum 0x39b2288ac0b6c493$' "$path") == 2 ]]

    if grep -Eq '^(real|user|sys) ' "$path"; then
        echo "$label log unexpectedly contains an external time trailer: $path" >&2
        exit 65
    fi
}

case "$mode" in
    verify)
        if [[ $# -ne 2 ]]; then
            usage
        fi
        verify_capsule
        ;;
    run)
        if [[ $# -ne 3 && $# -ne 4 ]]; then
            usage
        fi
        if [[ "${FORMULA_READY_MACHINE_QUIET:-}" != YES ]]; then
            echo "refusing timing run before FORMULA_READY_MACHINE_QUIET=YES" >&2
            exit 64
        fi

        output=$3
        repetitions=${4:-21}
        if [[ ! "$repetitions" =~ ^[0-9]+$ ]] || (( repetitions < 3 )); then
            echo "REPETITIONS must be an integer >= 3" >&2
            exit 64
        fi
        if [[ -e "$output" ]]; then
            echo "output already exists: $output" >&2
            exit 73
        fi

        verify_capsule
        mkdir -p "$output/logs"
        output=$(cd "$output" && pwd -P)

        {
            echo "status=IN_PROGRESS"
            echo "fixture=32x64 fanout=2"
            echo "repetitions=$repetitions"
            echo "schedule=B-D-C-C-D-B"
            echo "engine=current whole-root residual"
            echo "B_revision=${revisions[0]}"
            echo "D_revision=${revisions[1]}"
            echo "C_revision=${revisions[2]}"
            echo "harness_sha256=$harness_sha256"
            echo "cargo_lock_sha256=$lock_sha256"
        } > "$output/protocol.env"

        printf 'label\trevision\tbinary\tsha256\n' > "$output/binary-manifest.tsv"
        for index in "${!labels[@]}"; do
            printf '%s\t%s\t%s\t%s\n' \
                "${labels[$index]}" \
                "${revisions[$index]}" \
                "$capsule/bin/${binaries[$index]}" \
                "${binary_sha256[$index]}" \
                >> "$output/binary-manifest.tsv"
        done

        schedule=(B D C C D B)
        for ordinal in "${!schedule[@]}"; do
            label=${schedule[$ordinal]}
            case "$label" in
                B) index=0 ;;
                D) index=1 ;;
                C) index=2 ;;
                *) exit 70 ;;
            esac

            run_number=$((ordinal + 1))
            log="$output/logs/$(printf '%02d' "$run_number")-$label-r$repetitions.log"
            printf 'running %d/6: %s\n' "$run_number" "$label"
            env -u ENGINE_PROFILE_CELL \
                "$capsule/bin/${binaries[$index]}" 32 64 2 "$repetitions" \
                > "$log"
            validate_log \
                "$label" "${revisions[$index]}" "$repetitions" "$log"
        done

        {
            echo "status=COMPLETE"
            echo "fixture=32x64 fanout=2"
            echo "repetitions=$repetitions"
            echo "schedule=B-D-C-C-D-B"
            echo "engine=current whole-root residual"
            echo "B_revision=${revisions[0]}"
            echo "D_revision=${revisions[1]}"
            echo "C_revision=${revisions[2]}"
            echo "harness_sha256=$harness_sha256"
            echo "cargo_lock_sha256=$lock_sha256"
        } > "$output/protocol.env"

        (
            cd "$output"
            find protocol.env binary-manifest.tsv logs -type f -print \
                | LC_ALL=C sort \
                | while IFS= read -r artifact; do
                    shasum -a 256 "$artifact"
                done
        ) > "$output/closure.sha256"

        printf 'COMPLETE: exact B-D-C-C-D-B logs sealed under %s\n' "$output"
        ;;
    *)
        usage
        ;;
esac
