#!/usr/bin/env bash
set -euo pipefail

# Reproducible scalar-status-quo/current-frontier demand-curve experiment.
# Builds every subject before timing, then runs two pairwise ABBA blocks:
#   old scalar (A) versus current default frontier (B)
#   current width-one ablation (C) versus current default frontier (B)
#
# Usage:
#   scripts/run_query_engine_demand_curve.sh /absolute/output/directory
#
# Optional environment:
#   RUN_ID, OLD_REV, CURRENT_REV, SCALES, REPETITIONS, WARMUP,
#   OLD_CAUSAL_ROUTE_SCALES, CURRENT_CAUSAL_ROUTE_SCALES,
#   RAYON_NUM_THREADS, DEMAND_CURVE_CACHE_ROOT

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${1:?usage: run_query_engine_demand_curve.sh /absolute/output/directory}
case "$OUT" in
    /*) ;;
    *) echo "output directory must be absolute" >&2; exit 2 ;;
esac
if [[ -d "$OUT" ]] && [[ -n "$(find "$OUT" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "result directory must be new or empty: $OUT" >&2
    exit 2
fi
if [[ -e "$OUT" && ! -d "$OUT" ]]; then
    echo "result path is not a directory: $OUT" >&2
    exit 2
fi

OLD_REV=${OLD_REV:-2cd60807}
CURRENT_REV=${CURRENT_REV:-d75556ae}
SCALES=${SCALES:-"tiny below threshold above wide"}
OLD_CAUSAL_ROUTE_SCALES=${OLD_CAUSAL_ROUTE_SCALES:-""}
CURRENT_CAUSAL_ROUTE_SCALES=${CURRENT_CAUSAL_ROUTE_SCALES:-"threshold above wide"}
read -r -a SCALE_LIST <<<"$SCALES"
[[ ${#SCALE_LIST[@]} -gt 0 ]] || {
    echo "SCALES must name at least one scale" >&2
    exit 2
}
REPETITIONS=${REPETITIONS:-3}
WARMUP=${WARMUP:-1}
RUN_ID=${RUN_ID:-$(date -u +%Y-%m-%dT%H%M%SZ)}
DEMAND_CURVE_CACHE_ROOT=${DEMAND_CURVE_CACHE_ROOT:-/private/tmp/triblespace-query-demand-cache}
case "$DEMAND_CURVE_CACHE_ROOT" in
    /*) ;;
    *) echo "DEMAND_CURVE_CACHE_ROOT must be absolute" >&2; exit 2 ;;
esac
if [[ -z "${RAYON_NUM_THREADS:-}" ]]; then
    if command -v sysctl >/dev/null 2>&1; then
        RAYON_NUM_THREADS=$(sysctl -n hw.physicalcpu 2>/dev/null || true)
    fi
    RAYON_NUM_THREADS=${RAYON_NUM_THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)}
fi

OLD_REV=$(git -C "$ROOT" rev-parse "$OLD_REV^{commit}")
CURRENT_REV=$(git -C "$ROOT" rev-parse "$CURRENT_REV^{commit}")
HARNESS="$ROOT/examples/query_engine_demand_curve.rs"
RUNNER="$ROOT/scripts/run_query_engine_demand_curve.sh"
[[ -f "$HARNESS" ]] || {
    echo "missing harness: $HARNESS" >&2
    exit 2
}
HARNESS_SHA256=$(shasum -a 256 "$HARNESS" | awk '{print $1}')
RUNNER_SHA256=$(shasum -a 256 "$RUNNER" | awk '{print $1}')

mkdir -p "$OUT"/{bin,build-logs,locks,raw}
mkdir -p "$DEMAND_CURVE_CACHE_ROOT"/{subjects,targets}
cp "$HARNESS" "$OUT/harness.rs"
cp "$RUNNER" "$OUT/runner.sh"

prepare_subject() {
    local label=$1
    local revision=$2
    local worktree="$DEMAND_CURVE_CACHE_ROOT/subjects/$label-${revision:0:12}"
    if [[ ! -e "$worktree/.git" ]]; then
        git -C "$ROOT" worktree add --detach "$worktree" "$revision"
    fi
    local actual
    actual=$(git -C "$worktree" rev-parse HEAD)
    [[ "$actual" == "$revision" ]] || {
        echo "$label worktree is $actual, expected $revision" >&2
        exit 2
    }
    git -C "$worktree" diff --quiet HEAD -- . \
        ':(exclude)examples/query_engine_demand_curve.rs' || {
        echo "$label worktree has tracked changes outside the copied harness" >&2
        exit 2
    }
    local unexpected
    unexpected=$(git -C "$worktree" ls-files --others --exclude-standard |
        grep -Ev '^(Cargo.lock|examples/query_engine_demand_curve.rs)$' || true)
    [[ -z "$unexpected" ]] || {
        echo "$label worktree has unexpected untracked files:" >&2
        echo "$unexpected" >&2
        exit 2
    }
    cp "$HARNESS" "$worktree/examples/query_engine_demand_curve.rs"
    [[ "$(shasum -a 256 "$worktree/examples/query_engine_demand_curve.rs" | awk '{print $1}')" == "$HARNESS_SHA256" ]]
}

prepare_subject old "$OLD_REV"
prepare_subject current "$CURRENT_REV"
OLD_WORKTREE="$DEMAND_CURVE_CACHE_ROOT/subjects/old-${OLD_REV:0:12}"
CURRENT_WORKTREE="$DEMAND_CURVE_CACHE_ROOT/subjects/current-${CURRENT_REV:0:12}"

prepare_shared_lock() {
    local source_lock="$CURRENT_WORKTREE/Cargo.lock"
    if [[ ! -f "$source_lock" ]]; then
        echo "resolving one shared dependency lock outside the timed region" >&2
        cargo generate-lockfile --manifest-path "$CURRENT_WORKTREE/Cargo.toml" \
            >"$OUT/build-logs/dependency-lock.log" 2>&1
    else
        printf 'reusing %s\n' "$source_lock" >"$OUT/build-logs/dependency-lock.log"
    fi
    [[ -f "$source_lock" ]] || {
        echo "dependency resolution did not create $source_lock" >&2
        exit 2
    }
    cp "$source_lock" "$OLD_WORKTREE/Cargo.lock"
    cp "$source_lock" "$OUT/locks/Cargo.lock"
    shasum -a 256 "$OUT/locks/Cargo.lock" | awk '{print $1}'
}

DEPENDENCY_LOCK_SHA256=$(prepare_shared_lock)

build_subject() {
    local label=$1
    local worktree=$2
    local revision=$3
    local target_label=$4
    shift 4
    [[ "$(shasum -a 256 "$worktree/Cargo.lock" | awk '{print $1}')" == \
        "$DEPENDENCY_LOCK_SHA256" ]] || {
        echo "$label worktree does not contain the frozen dependency lock" >&2
        exit 2
    }
    echo "building $label ($revision)" >&2
    env \
        DEMAND_CURVE_ENGINE_REVISION="$revision" \
        DEMAND_CURVE_HARNESS_SHA256="$HARNESS_SHA256" \
        DEMAND_CURVE_LOCK_SHA256="$DEPENDENCY_LOCK_SHA256" \
        CARGO_TARGET_DIR="$DEMAND_CURVE_CACHE_ROOT/targets/$target_label-${revision:0:12}" \
        cargo rustc \
            --manifest-path "$worktree/Cargo.toml" \
            --locked \
            --release \
            --example query_engine_demand_curve -- "$@" \
            >"$OUT/build-logs/$label.log" 2>&1
    cp \
        "$DEMAND_CURVE_CACHE_ROOT/targets/$target_label-${revision:0:12}/release/examples/query_engine_demand_curve" \
        "$OUT/bin/query_engine_demand_curve-$label"
}

build_subject old "$OLD_WORKTREE" "$OLD_REV" old
build_subject current "$CURRENT_WORKTREE" "$CURRENT_REV" current \
    --cfg demand_frontier_stats
build_subject current-w1 "$CURRENT_WORKTREE" "$CURRENT_REV" current \
    --cfg demand_frontier_stats --cfg demand_frontier_w1

{
    printf 'key\tvalue\n'
    printf 'protocol\tquery-engine-demand-curve-v1\n'
    printf 'run_id\t%s\n' "$RUN_ID"
    printf 'old_revision\t%s\n' "$OLD_REV"
    printf 'current_revision\t%s\n' "$CURRENT_REV"
    printf 'harness_sha256\t%s\n' "$HARNESS_SHA256"
    printf 'runner_sha256\t%s\n' "$RUNNER_SHA256"
    printf 'dependency_lock_sha256\t%s\n' "$DEPENDENCY_LOCK_SHA256"
    printf 'build_cache_root\t%s\n' "$DEMAND_CURVE_CACHE_ROOT"
    printf 'old_binary_sha256\t%s\n' \
        "$(shasum -a 256 "$OUT/bin/query_engine_demand_curve-old" | awk '{print $1}')"
    printf 'current_binary_sha256\t%s\n' \
        "$(shasum -a 256 "$OUT/bin/query_engine_demand_curve-current" | awk '{print $1}')"
    printf 'current_w1_binary_sha256\t%s\n' \
        "$(shasum -a 256 "$OUT/bin/query_engine_demand_curve-current-w1" | awk '{print $1}')"
    printf 'rayon_num_threads\t%s\n' "$RAYON_NUM_THREADS"
    printf 'repetitions_per_invocation\t%s\n' "$REPETITIONS"
    printf 'warmups_per_invocation\t%s\n' "$WARMUP"
    printf 'scales\t%s\n' "$SCALES"
    printf 'old_causal_route_scales\t%s\n' "$OLD_CAUSAL_ROUTE_SCALES"
    printf 'current_causal_route_scales\t%s\n' "$CURRENT_CAUSAL_ROUTE_SCALES"
    printf 'rustc\t%s\n' "$(rustc --version)"
    printf 'cargo\t%s\n' "$(cargo --version)"
    printf 'uname\t%s\n' "$(uname -a)"
} >"$OUT/manifest.tsv"
if command -v system_profiler >/dev/null 2>&1; then
    system_profiler SPHardwareDataType SPDisplaysDataType -json \
        >"$OUT/system-profiler.json"
fi

route_expectation() {
    local label=$1
    local scale=$2
    local routed_scales=""
    case "$label" in
        old) routed_scales=$OLD_CAUSAL_ROUTE_SCALES ;;
        current) routed_scales=$CURRENT_CAUSAL_ROUTE_SCALES ;;
    esac
    if [[ " $routed_scales " == *" $scale "* ]]; then
        printf 'yes\n'
    else
        printf 'no\n'
    fi
}

INVOCATION_SEQUENCE=0
RAW_FILES=()
run_one() {
    local label=$1
    local revision=$2
    local variant=$3
    local scale=$4
    local position=$5
    local invocation_sequence=$INVOCATION_SEQUENCE
    INVOCATION_SEQUENCE=$((INVOCATION_SEQUENCE + 1))
    local ordinal
    printf -v ordinal '%03d' "$invocation_sequence"
    local route
    route=$(route_expectation "$label" "$scale")
    local stem="$ordinal-$scale-$position-$label"
    local raw_file="$OUT/raw/$stem.tsv"
    echo "running $stem (causal route: $route)" >&2
    env RAYON_NUM_THREADS="$RAYON_NUM_THREADS" \
        "$OUT/bin/query_engine_demand_curve-$label" \
        --expect-engine "$revision" \
        --expect-variant "$variant" \
        --expect-harness "$HARNESS_SHA256" \
        --expect-lock "$DEPENDENCY_LOCK_SHA256" \
        --run-id "$RUN_ID" \
        --abba-position "$position" \
        --invocation-sequence "$invocation_sequence" \
        --scale "$scale" \
        --repetitions "$REPETITIONS" \
        --warmup "$WARMUP" \
        --gpu \
        --expect-causal-route "$route" \
        >"$raw_file" \
        2>"$OUT/raw/$stem.stderr"
    RAW_FILES+=("$raw_file")
}

for scale in "${SCALE_LIST[@]}"; do
    # Practical status quo comparison: A B B A.
    run_one old "$OLD_REV" default "$scale" "primary-A1"
    run_one current "$CURRENT_REV" default "$scale" "primary-B1"
    run_one current "$CURRENT_REV" default "$scale" "primary-B2"
    run_one old "$OLD_REV" default "$scale" "primary-A2"

    # Same-source batching ablation: C B B C.
    run_one current-w1 "$CURRENT_REV" frontier-w1 "$scale" "ablation-C1"
    run_one current "$CURRENT_REV" default "$scale" "ablation-B1"
    run_one current "$CURRENT_REV" default "$scale" "ablation-B2"
    run_one current-w1 "$CURRENT_REV" frontier-w1 "$scale" "ablation-C2"
done

expected_files=$((8 * ${#SCALE_LIST[@]}))
[[ ${#RAW_FILES[@]} -eq $expected_files ]] || {
    echo "raw-file count mismatch: got ${#RAW_FILES[@]}, expected $expected_files" >&2
    exit 2
}

first=1
: >"$OUT/observations.tsv"
header=
for file in "${RAW_FILES[@]}"; do
    file_header=$(sed -n '1p' "$file")
    if [[ $first -eq 1 ]]; then
        header=$file_header
        printf '%s\n' "$header" >>"$OUT/observations.tsv"
        first=0
    elif [[ "$file_header" != "$header" ]]; then
        echo "header mismatch in $file" >&2
        exit 2
    fi
    sed -n '2,$p' "$file" >>"$OUT/observations.tsv"
done

awk -F '\t' \
    -v expected_run="$RUN_ID" \
    -v expected_harness="$HARNESS_SHA256" \
    -v old_revision="$OLD_REV" \
    -v current_revision="$CURRENT_REV" \
    -v expected_lock="$DEPENDENCY_LOCK_SHA256" '
    function value(name) { return $(column[name]) }
    function fail(message) {
        print message > "/dev/stderr"
        bad = 1
    }
    NR == 1 {
        columns = NF
        for (i = 1; i <= NF; i++) column[$i] = i
        next
    }
    NF != columns {
        fail(sprintf("column mismatch at aggregate line %d: got %d, expected %d", NR, NF, columns))
        next
    }
    {
        if (value("run_id") != expected_run)
            fail(sprintf("run_id mismatch at line %d", NR))
        if (value("harness") != expected_harness)
            fail(sprintf("harness mismatch at line %d", NR))
        if (value("dependency_lock") != expected_lock)
            fail(sprintf("dependency lock mismatch at line %d", NR))

        engine = value("engine")
        variant = value("engine_variant")
        if (engine == old_revision) {
            if (variant != "default")
                fail(sprintf("old revision has variant %s at line %d", variant, NR))
        } else if (engine == current_revision) {
            if (variant != "default" && variant != "frontier-w1")
                fail(sprintf("current revision has variant %s at line %d", variant, NR))
        } else {
            fail(sprintf("unexpected engine revision %s at line %d", engine, NR))
        }

        invocation = value("invocation_sequence") + 0
        if (seen_invocation && invocation < previous_invocation)
            fail(sprintf("invocation order regressed at line %d", NR))
        if (!seen_invocation || invocation != previous_invocation) {
            previous_invocation = invocation
            seen_invocation = 1
            seen_sample_sequence = 0
        }
        if (value("record") == "sample") {
            sequence = value("execution_sequence") + 0
            if (seen_sample_sequence && sequence <= previous_sample_sequence)
                fail(sprintf("sample sequence did not increase at line %d", NR))
            previous_sample_sequence = sequence
            seen_sample_sequence = 1
        }

        scale = value("scale")
        observed_corpus = value("corpus")
        if (!(scale in corpus)) corpus[scale] = observed_corpus
        else if (corpus[scale] != observed_corpus)
            fail(sprintf("corpus mismatch for scale %s at line %d", scale, NR))

        if (value("record") == "identity") {
            key = scale SUBSEP value("shape")
            digest = value("result_digest")
            if (!(key in identity)) identity[key] = digest
            else if (identity[key] != digest)
                fail(sprintf("identity digest mismatch for %s/%s at line %d", scale, value("shape"), NR))
        }
    }
    END { exit bad }
' "$OUT/observations.tsv"

echo "complete: $OUT/observations.tsv" >&2
