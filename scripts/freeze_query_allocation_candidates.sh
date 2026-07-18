#!/usr/bin/env bash
# Source-only, deliberately unexecuted allocation-count harness for the exact
# B/P/M/C query-engine candidates frozen on 2026-07-18.
#
# The four revisions already contain the same benchmark source byte-for-byte.
# Its ENGINE_PROFILE_CELL path drains each query with the allocation-free
# scalar `tally` helper between allocator snapshots; clocks, formatting, and
# stdout are outside those snapshots. This script refuses any other benchmark
# source hash, then builds and runs all seven cells twice in exactly reversed
# order. Wall time is not collected or used as evidence.

set -euo pipefail

if [[ "${QUERY_ALLOCATION_LANE_RELEASED:-}" != "YES" ]]; then
    echo "refusing to build or run before QUERY_ALLOCATION_LANE_RELEASED=YES" >&2
    exit 64
fi

if [[ $# -ne 3 && $# -ne 4 ]]; then
    echo "usage: $0 REPOSITORY OUTPUT_ROOT CARGO_LOCKFILE [REPETITIONS]" >&2
    exit 64
fi

repository=$1
output_root=$2
lockfile=$3
repetitions=${4:-21}

if [[ ! "$repetitions" =~ ^[0-9]+$ ]] || (( repetitions < 3 )); then
    echo "REPETITIONS must be an integer >= 3" >&2
    exit 64
fi
if [[ ! -d "$repository/.git" && ! -f "$repository/.git" ]]; then
    echo "REPOSITORY is not a Git worktree: $repository" >&2
    exit 66
fi
if [[ ! -s "$lockfile" ]]; then
    echo "CARGO_LOCKFILE is missing or empty: $lockfile" >&2
    exit 66
fi
if [[ -e "$output_root" ]]; then
    echo "OUTPUT_ROOT already exists: $output_root" >&2
    exit 73
fi

repository=$(cd "$repository" && pwd -P)
lockfile_dir=$(cd "$(dirname "$lockfile")" && pwd -P)
lockfile="$lockfile_dir/$(basename "$lockfile")"
output_parent=$(cd "$(dirname "$output_root")" && pwd -P)
output_root="$output_parent/$(basename "$output_root")"

labels=(B P M C)
revisions=(
    fe914c29ad16895628bc6673d4e5033c6c3836b0
    e55f8646466278c56c71726687bfb268aa727d69
    bc99a79407fab5958c81b7d3ee495552817401d2
    35250752a6c92907754576efd9d5938508b2f14a
)
trees=(
    8ec72d340b70d878af183a17686f3b89be526ffc
    5e9cba5c574a425aa7d2a68392c30207ac1df104
    4c228d07d6e165073fd0d3e99903e66fe682d975
    7e32806cc3f7e8e93bf3b3ea6f1f3bab68384c4e
)
benchmark_sha=144d9eb2ef61ffc9f4525f17dfc413eac83ce3d4b5d17e52545b070a53c8aae1
workspace_manifest_sha=7fd6a37373d8ddae64ce14560bbc2b44c49220838c0f6f695b5e86a2745c0cae
cells=(
    finite-trible
    finite-succinct
    formula-trible
    formula-succinct
    cyclic-trible
    mixed-trible
    mixed-succinct
)

cargo_bin=$(command -v cargo)
rustc_bin=$(command -v rustc)
if rustup_bin=$(command -v rustup); then
    cargo_toolchain_bin=$($rustup_bin which cargo)
    rustc_toolchain_bin=$($rustup_bin which rustc)
else
    cargo_toolchain_bin=$cargo_bin
    rustc_toolchain_bin=$rustc_bin
fi
git_bin=$(command -v git)
tar_bin=$(command -v tar)
shasum_bin=$(command -v shasum)
stat_bin=$(command -v stat)
sanitized_path=$(dirname "$cargo_toolchain_bin"):$(dirname "$rustc_toolchain_bin"):/usr/bin:/bin
cargo_home=${CARGO_HOME:-$HOME/.cargo}
rustup_home=${RUSTUP_HOME:-$HOME/.rustup}

mkdir -p "$output_root/sources" "$output_root/targets" \
    "$output_root/source-archives" \
    "$output_root/binaries" "$output_root/build-logs" \
    "$output_root/run-logs" "$output_root/profiles" "$output_root/tmp"
chmod 0700 "$output_root"

lock_sha=$($shasum_bin -a 256 "$lockfile" | awk '{print $1}')
cargo_sha=$($shasum_bin -a 256 "$cargo_toolchain_bin" | awk '{print $1}')
rustc_sha=$($shasum_bin -a 256 "$rustc_toolchain_bin" | awk '{print $1}')
{
    echo "status=UNVERIFIED_UNTIL_COMPLETE"
    echo "benchmark_sha256=$benchmark_sha"
    echo "workspace_manifest_sha256=$workspace_manifest_sha"
    echo "cargo_lock_sha256=$lock_sha"
    echo "cargo_path=$cargo_toolchain_bin"
    echo "cargo_sha256=$cargo_sha"
    echo "rustc_path=$rustc_toolchain_bin"
    echo "rustc_sha256=$rustc_sha"
    echo "rustflags=--cfg engine_current_residual --cfg engine_allocation_probe"
    echo "cargo_profile=release"
    echo "cargo_features=default"
    echo "fixture=32x64 fanout=2"
    echo "repetitions=$repetitions"
    echo "cells=${cells[*]}"
} > "$output_root/protocol.env"

env -i HOME="$HOME" PATH="$sanitized_path" CARGO_HOME="$cargo_home" \
    RUSTUP_HOME="$rustup_home" RUSTC="$rustc_toolchain_bin" \
    "$cargo_toolchain_bin" --version --verbose \
    > "$output_root/cargo-version.txt"
env -i HOME="$HOME" PATH="$sanitized_path" CARGO_HOME="$cargo_home" \
    RUSTUP_HOME="$rustup_home" "$rustc_toolchain_bin" --version --verbose \
    > "$output_root/rustc-version.txt"

printf 'label\trevision\ttree\tarchive\tarchive_size\tarchive_sha256\tbenchmark_sha256\tworkspace_manifest_sha256\tcargo_lock_sha256\n' \
    > "$output_root/source-manifest.tsv"

# Close every source and lock identity gate before the first build.
for index in "${!labels[@]}"; do
    label=${labels[$index]}
    revision=${revisions[$index]}
    expected_tree=${trees[$index]}
    source_dir="$output_root/sources/$label"
    target_dir="$output_root/targets/$label"
    source_archive="$output_root/source-archives/$label.tar"

    actual_revision=$($git_bin -C "$repository" rev-parse "$revision^{commit}")
    actual_tree=$($git_bin -C "$repository" rev-parse "$revision^{tree}")
    if [[ "$actual_revision" != "$revision" || "$actual_tree" != "$expected_tree" ]]; then
        echo "$label source identity mismatch" >&2
        exit 65
    fi
    actual_benchmark_sha=$(
        $git_bin -C "$repository" show \
            "${revision}:examples/query_engine_generation_bench.rs" \
            | $shasum_bin -a 256 | awk '{print $1}'
    )
    if [[ "$actual_benchmark_sha" != "$benchmark_sha" ]]; then
        echo "$label benchmark source mismatch: $actual_benchmark_sha" >&2
        exit 65
    fi
    actual_manifest_sha=$(
        $git_bin -C "$repository" show "${revision}:Cargo.toml" \
            | $shasum_bin -a 256 | awk '{print $1}'
    )
    if [[ "$actual_manifest_sha" != "$workspace_manifest_sha" ]]; then
        echo "$label workspace manifest mismatch: $actual_manifest_sha" >&2
        exit 65
    fi

    mkdir -p "$source_dir" "$target_dir"
    $git_bin -C "$repository" archive --format=tar \
        --output="$source_archive" "$revision"
    archive_size=$($stat_bin -f '%z' "$source_archive")
    archive_sha=$($shasum_bin -a 256 "$source_archive" | awk '{print $1}')
    $tar_bin -xf "$source_archive" -C "$source_dir"
    extracted_benchmark_sha=$(
        $shasum_bin -a 256 "$source_dir/examples/query_engine_generation_bench.rs" \
            | awk '{print $1}'
    )
    extracted_manifest_sha=$(
        $shasum_bin -a 256 "$source_dir/Cargo.toml" | awk '{print $1}'
    )
    if [[ "$extracted_benchmark_sha" != "$benchmark_sha" \
        || "$extracted_manifest_sha" != "$workspace_manifest_sha" ]]; then
        echo "$label extracted source mismatch" >&2
        exit 65
    fi
    cp "$lockfile" "$source_dir/Cargo.lock"
    copied_lock_sha=$($shasum_bin -a 256 "$source_dir/Cargo.lock" | awk '{print $1}')
    if [[ "$copied_lock_sha" != "$lock_sha" ]]; then
        echo "$label Cargo.lock copy mismatch" >&2
        exit 65
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$revision" "$actual_tree" "$source_archive" \
        "$archive_size" "$archive_sha" "$extracted_benchmark_sha" \
        "$extracted_manifest_sha" "$copied_lock_sha" \
        >> "$output_root/source-manifest.tsv"
done

if [[ $($shasum_bin -a 256 "$cargo_toolchain_bin" | awk '{print $1}') != "$cargo_sha" \
    || $($shasum_bin -a 256 "$rustc_toolchain_bin" | awk '{print $1}') != "$rustc_sha" ]]; then
    echo "toolchain executables changed during source preflight" >&2
    exit 65
fi

printf 'label\trevision\ttree\tbenchmark_sha256\tcargo_lock_sha256\tbinary\tsize\tsha256\n' \
    > "$output_root/build-manifest.tsv"

for index in "${!labels[@]}"; do
    label=${labels[$index]}
    revision=${revisions[$index]}
    actual_tree=${trees[$index]}
    source_dir="$output_root/sources/$label"
    target_dir="$output_root/targets/$label"

    env -i HOME="$HOME" PATH="$sanitized_path" CARGO_HOME="$cargo_home" \
        RUSTUP_HOME="$rustup_home" TMPDIR="$output_root/tmp" \
        CARGO_INCREMENTAL=0 CARGO_TERM_COLOR=never \
        CARGO_TARGET_DIR="$target_dir" SOURCE_DATE_EPOCH=0 \
        RUSTC="$rustc_toolchain_bin" \
        RUSTFLAGS="--cfg engine_current_residual --cfg engine_allocation_probe" \
        ENGINE_REVISION="$revision" \
        "$cargo_toolchain_bin" build --manifest-path "$source_dir/Cargo.toml" \
            --frozen --release --example query_engine_generation_bench \
        > "$output_root/build-logs/$label.log" 2>&1

    binary="$output_root/binaries/${label}-${revision:0:8}-query_engine_generation_bench"
    cp "$target_dir/release/examples/query_engine_generation_bench" "$binary"
    chmod 0555 "$binary"
    binary_size=$($stat_bin -f '%z' "$binary")
    binary_sha=$($shasum_bin -a 256 "$binary" | awk '{print $1}')
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$revision" "$actual_tree" "$benchmark_sha" \
        "$lock_sha" "$binary" "$binary_size" "$binary_sha" \
        >> "$output_root/build-manifest.tsv"
done

revision_for_label() {
    case "$1" in
        B) echo "${revisions[0]}" ;;
        P) echo "${revisions[1]}" ;;
        M) echo "${revisions[2]}" ;;
        C) echo "${revisions[3]}" ;;
        *) return 1 ;;
    esac
}

run_profile() {
    pass=$1
    ordinal=$2
    label=$3
    cell=$4
    revision=$(revision_for_label "$label")
    binary="$output_root/binaries/${label}-${revision:0:8}-query_engine_generation_bench"
    log="$output_root/run-logs/${pass}-${ordinal}-${label}-${cell}.log"
    profile="$output_root/profiles/${pass}-${label}-${cell}.txt"
    expected_binary_sha=$(awk -F '\t' -v label="$label" \
        '$1 == label { print $8 }' "$output_root/build-manifest.tsv")
    actual_binary_sha=$($shasum_bin -a 256 "$binary" | awk '{print $1}')
    if [[ ! "$expected_binary_sha" =~ ^[0-9a-f]{64}$ \
        || "$actual_binary_sha" != "$expected_binary_sha" ]]; then
        echo "$pass/$label/$cell frozen binary hash mismatch" >&2
        exit 65
    fi

    env -i HOME="$HOME" PATH="$sanitized_path" \
        ENGINE_PROFILE_CELL="$cell" \
        "$binary" 32 64 2 "$repetitions" > "$log" 2>&1

    grep -Fx "engine: current residual" "$log" > /dev/null
    grep -Fx "revision: $revision" "$log" > /dev/null
    grep -Fx "oracle parity: all seven query/backend cells exact" "$log" > /dev/null
    grep -Fx "profile cell=\"$cell\" repetitions=$repetitions" "$log" > /dev/null
    if [[ $(grep -c '^alloc_profile ' "$log") -ne 1 ]]; then
        echo "$pass/$label/$cell did not emit one allocation profile" >&2
        exit 65
    fi
    awk '/^alloc_profile / || /^alloc_bin / { print }' "$log" > "$profile"
}

ordinal=0
for label in B P M C; do
    for cell in "${cells[@]}"; do
        ordinal=$((ordinal + 1))
        run_profile forward "$ordinal" "$label" "$cell"
    done
done

reverse_cells=(
    mixed-succinct
    mixed-trible
    cyclic-trible
    formula-succinct
    formula-trible
    finite-succinct
    finite-trible
)
ordinal=0
for label in C M P B; do
    for cell in "${reverse_cells[@]}"; do
        ordinal=$((ordinal + 1))
        run_profile reverse "$ordinal" "$label" "$cell"
    done
done

seal_closure() {
    (
        cd "$output_root"
        find STATUS protocol.env cargo-version.txt rustc-version.txt \
            source-manifest.tsv build-manifest.tsv source-archives binaries \
            build-logs run-logs profiles -type f -print \
            | LC_ALL=C sort \
            | while IFS= read -r artifact; do
                $shasum_bin -a 256 "$artifact"
            done
    ) > "$output_root/closure.sha256"
}

for label in B P M C; do
    for cell in "${cells[@]}"; do
        if ! cmp -s "$output_root/profiles/forward-$label-$cell.txt" \
            "$output_root/profiles/reverse-$label-$cell.txt"; then
            echo "INCONCLUSIVE: allocation counts changed for $label/$cell" \
                | tee "$output_root/STATUS" >&2
            seal_closure
            exit 2
        fi
    done
done

echo "ALLOC_PROFILE_REPEATS_EXACT" > "$output_root/STATUS"
seal_closure
