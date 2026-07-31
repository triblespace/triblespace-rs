#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
if [[ $# -ne 1 ]]; then
    echo "usage: OLD_REV=<rev> CURRENT_REV=<rev> $0 /absolute/output-directory" >&2
    exit 2
fi
OUT=$1
case "$OUT" in
    /*) ;;
    *)
        echo "output directory must be absolute" >&2
        exit 2
        ;;
esac
if [[ -e "$OUT" ]] && [[ -n "$(find "$OUT" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "refusing to mix a new run into non-empty output directory: $OUT" >&2
    exit 2
fi

OLD_REV=${OLD_REV:-28dee953}
CURRENT_REV=${CURRENT_REV:-HEAD}
OLD_VARIANT=${OLD_VARIANT:-double-geometric}
CURRENT_VARIANT=${CURRENT_VARIANT:-preyield-credit}
[[ "$OLD_VARIANT" == "double-geometric" ]] || {
    echo "OLD_VARIANT must be double-geometric" >&2
    exit 2
}
case "$CURRENT_VARIANT" in
    inherited-credit | preyield-credit) ;;
    *)
        echo "CURRENT_VARIANT must be inherited-credit or preyield-credit" >&2
        exit 2
        ;;
esac

VARIABLES=${VARIABLES:-"2 3 8 32"}
read -r -a VARIABLE_LIST <<<"$VARIABLES"
[[ ${#VARIABLE_LIST[@]} -gt 0 ]] || {
    echo "VARIABLES must not be empty" >&2
    exit 2
}
for variables in "${VARIABLE_LIST[@]}"; do
    case "$variables" in
        2 | 3 | 8 | 32) ;;
        *)
            echo "VARIABLES may contain only 2, 3, 8, and 32" >&2
            exit 2
            ;;
    esac
done
LATE_SCENARIOS=("late-2" "late-18" "late-146" "late-1170" "late-9362")
DENSE_DEMANDS="1 2 3 9 10 11 73 74 75 585 586 587 4681 4682 4683 full"

REPETITIONS=${REPETITIONS:-3}
WARMUP=${WARMUP:-1}
[[ "$REPETITIONS" =~ ^[0-9]+$ && "$REPETITIONS" -ge 3 ]] || {
    echo "REPETITIONS must be an integer >= 3" >&2
    exit 2
}
[[ "$WARMUP" =~ ^[0-9]+$ && "$WARMUP" -ge 1 ]] || {
    echo "WARMUP must be an integer >= 1" >&2
    exit 2
}

RUN_ID=${RUN_ID:-$(date -u +%Y-%m-%dT%H%M%SZ)}
SOURCE_CREDIT_CACHE_ROOT=${SOURCE_CREDIT_CACHE_ROOT:-/private/tmp/triblespace-source-credit-cache}
case "$SOURCE_CREDIT_CACHE_ROOT" in
    /*) ;;
    *)
        echo "SOURCE_CREDIT_CACHE_ROOT must be absolute" >&2
        exit 2
        ;;
esac

OLD_REV=$(git -C "$ROOT" rev-parse "$OLD_REV^{commit}")
CURRENT_REV=$(git -C "$ROOT" rev-parse "$CURRENT_REV^{commit}")
[[ "$OLD_REV" != "$CURRENT_REV" ]] || {
    echo "OLD_REV and CURRENT_REV resolve to the same commit" >&2
    exit 2
}

HARNESS="$ROOT/examples/query_engine_source_credit.rs"
RUNNER="$ROOT/scripts/run_query_engine_source_credit.sh"
ANALYZER="$ROOT/scripts/analyze_query_engine_source_credit.py"
[[ -f "$HARNESS" ]] || {
    echo "missing harness: $HARNESS" >&2
    exit 2
}
[[ -f "$ANALYZER" ]] || {
    echo "missing analyzer: $ANALYZER" >&2
    exit 2
}
HARNESS_SHA256=$(shasum -a 256 "$HARNESS" | awk '{print $1}')
RUNNER_SHA256=$(shasum -a 256 "$RUNNER" | awk '{print $1}')
ANALYZER_SHA256=$(shasum -a 256 "$ANALYZER" | awk '{print $1}')

mkdir -p "$OUT"/{bin,build-logs,locks,raw}
mkdir -p "$SOURCE_CREDIT_CACHE_ROOT"/{subjects,targets}
cp "$HARNESS" "$OUT/harness.rs"
cp "$RUNNER" "$OUT/runner.sh"
cp "$ANALYZER" "$OUT/analyzer.py"

prepare_subject() {
    local label=$1
    local revision=$2
    local worktree="$SOURCE_CREDIT_CACHE_ROOT/subjects/$label-${revision:0:12}"
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
        ':(exclude)Cargo.lock' \
        ':(exclude)examples/query_engine_source_credit.rs' || {
        echo "$label worktree has tracked changes outside the copied lock/harness" >&2
        exit 2
    }
    local unexpected
    unexpected=$(git -C "$worktree" ls-files --others --exclude-standard |
        grep -Ev '^(Cargo.lock|examples/query_engine_source_credit.rs)$' || true)
    [[ -z "$unexpected" ]] || {
        echo "$label worktree has unexpected untracked files:" >&2
        echo "$unexpected" >&2
        exit 2
    }
    cp "$HARNESS" "$worktree/examples/query_engine_source_credit.rs"
    [[ "$(shasum -a 256 "$worktree/examples/query_engine_source_credit.rs" | awk '{print $1}')" == "$HARNESS_SHA256" ]]
}

prepare_subject old "$OLD_REV"
prepare_subject current "$CURRENT_REV"
OLD_WORKTREE="$SOURCE_CREDIT_CACHE_ROOT/subjects/old-${OLD_REV:0:12}"
CURRENT_WORKTREE="$SOURCE_CREDIT_CACHE_ROOT/subjects/current-${CURRENT_REV:0:12}"

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
    local variant=$2
    local worktree=$3
    local revision=$4
    local target="$SOURCE_CREDIT_CACHE_ROOT/targets/$label-${revision:0:12}-${HARNESS_SHA256:0:12}"
    [[ "$(shasum -a 256 "$worktree/Cargo.lock" | awk '{print $1}')" == \
        "$DEPENDENCY_LOCK_SHA256" ]] || {
        echo "$label worktree does not contain the frozen dependency lock" >&2
        exit 2
    }
    echo "building $label ($revision, $variant)" >&2
    env \
        SOURCE_CREDIT_ENGINE_REVISION="$revision" \
        SOURCE_CREDIT_ENGINE_VARIANT="$variant" \
        SOURCE_CREDIT_HARNESS_SHA256="$HARNESS_SHA256" \
        SOURCE_CREDIT_LOCK_SHA256="$DEPENDENCY_LOCK_SHA256" \
        CARGO_TARGET_DIR="$target" \
        cargo rustc \
            --manifest-path "$worktree/Cargo.toml" \
            --locked \
            --release \
            --no-default-features \
            --example query_engine_source_credit -- \
            >"$OUT/build-logs/$label.log" 2>&1
    cp "$target/release/examples/query_engine_source_credit" \
        "$OUT/bin/query_engine_source_credit-$label"
    chmod a-w "$OUT/bin/query_engine_source_credit-$label"
}

build_subject old "$OLD_VARIANT" "$OLD_WORKTREE" "$OLD_REV"
build_subject current "$CURRENT_VARIANT" "$CURRENT_WORKTREE" "$CURRENT_REV"

{
    printf 'key\tvalue\n'
    printf 'protocol\tquery-engine-source-credit-v1\n'
    printf 'run_id\t%s\n' "$RUN_ID"
    printf 'old_revision\t%s\n' "$OLD_REV"
    printf 'current_revision\t%s\n' "$CURRENT_REV"
    printf 'old_variant\t%s\n' "$OLD_VARIANT"
    printf 'current_variant\t%s\n' "$CURRENT_VARIANT"
    printf 'harness_sha256\t%s\n' "$HARNESS_SHA256"
    printf 'runner_sha256\t%s\n' "$RUNNER_SHA256"
    printf 'analyzer_sha256\t%s\n' "$ANALYZER_SHA256"
    printf 'dependency_lock_sha256\t%s\n' "$DEPENDENCY_LOCK_SHA256"
    printf 'old_binary_sha256\t%s\n' \
        "$(shasum -a 256 "$OUT/bin/query_engine_source_credit-old" | awk '{print $1}')"
    printf 'current_binary_sha256\t%s\n' \
        "$(shasum -a 256 "$OUT/bin/query_engine_source_credit-current" | awk '{print $1}')"
    printf 'roots\t16384\n'
    printf 'scenarios\tdense-bijective %s\n' "${LATE_SCENARIOS[*]}"
    printf 'dense_variables\t%s\n' "$VARIABLES"
    printf 'late_variables\t3\n'
    printf 'dense_demands\t%s\n' "$DENSE_DEMANDS"
    printf 'late_demands\t1\n'
    printf 'repetitions_per_invocation\t%s\n' "$REPETITIONS"
    printf 'warmups_per_invocation\t%s\n' "$WARMUP"
    printf 'build_cache_root\t%s\n' "$SOURCE_CREDIT_CACHE_ROOT"
    printf 'rustc\t%s\n' "$(rustc --version)"
    printf 'cargo\t%s\n' "$(cargo --version)"
    printf 'uname\t%s\n' "$(uname -a)"
} >"$OUT/manifest.tsv"
if command -v system_profiler >/dev/null 2>&1; then
    system_profiler SPHardwareDataType -json >"$OUT/system-profiler.json"
fi

INVOCATION_SEQUENCE=0
RAW_FILES=()
run_one() {
    local label=$1
    local revision=$2
    local variant=$3
    local scenario=$4
    local variables=$5
    local position=$6
    local invocation_sequence=$INVOCATION_SEQUENCE
    INVOCATION_SEQUENCE=$((INVOCATION_SEQUENCE + 1))
    local ordinal
    printf -v ordinal '%03d' "$invocation_sequence"
    local stem="$ordinal-$scenario-v$variables-$position-$label"
    local raw_file="$OUT/raw/$stem.tsv"
    echo "running $stem" >&2
    "$OUT/bin/query_engine_source_credit-$label" \
        --expect-engine "$revision" \
        --expect-variant "$variant" \
        --expect-harness "$HARNESS_SHA256" \
        --expect-lock "$DEPENDENCY_LOCK_SHA256" \
        --run-id "$RUN_ID" \
        --abba-position "$position" \
        --invocation-sequence "$invocation_sequence" \
        --scenario "$scenario" \
        --variables "$variables" \
        --repetitions "$REPETITIONS" \
        --warmup "$WARMUP" \
        >"$raw_file" \
        2>"$OUT/raw/$stem.stderr"
    RAW_FILES+=("$raw_file")
}

for variables in "${VARIABLE_LIST[@]}"; do
    run_one old "$OLD_REV" "$OLD_VARIANT" dense-bijective "$variables" "A1"
    run_one current "$CURRENT_REV" "$CURRENT_VARIANT" dense-bijective "$variables" "B1"
    run_one current "$CURRENT_REV" "$CURRENT_VARIANT" dense-bijective "$variables" "B2"
    run_one old "$OLD_REV" "$OLD_VARIANT" dense-bijective "$variables" "A2"
done
for scenario in "${LATE_SCENARIOS[@]}"; do
    run_one old "$OLD_REV" "$OLD_VARIANT" "$scenario" 3 "A1"
    run_one current "$CURRENT_REV" "$CURRENT_VARIANT" "$scenario" 3 "B1"
    run_one current "$CURRENT_REV" "$CURRENT_VARIANT" "$scenario" 3 "B2"
    run_one old "$OLD_REV" "$OLD_VARIANT" "$scenario" 3 "A2"
done

expected_files=$((4 * (${#VARIABLE_LIST[@]} + ${#LATE_SCENARIOS[@]})))
[[ ${#RAW_FILES[@]} -eq $expected_files ]] || {
    echo "raw-file count mismatch: got ${#RAW_FILES[@]}, expected $expected_files" >&2
    exit 2
}

first=1
: >"$OUT/observations.tsv"
header=
expected_header=$'record\trun_id\tabba_position\tinvocation_sequence\tengine\tengine_variant\tharness\tdependency_lock\troots\tscenario\tvariables\tdemand\trepetition\telapsed_ns\trows\tresult_digest\texpansions\tfrontier_rows\tvariable_groups\tproposals\twidest\tinplace_descents\tcopied_descents\tconstraint\tconstraint_kind\tpropose_calls\tpropose_frontier_rows\tpropose_outputs\tpropose_max_width\tpropose_widths'
for file in "${RAW_FILES[@]}"; do
    [[ -s "$file" ]] || {
        echo "raw output is empty: $file" >&2
        exit 2
    }
    file_header=$(sed -n '1p' "$file")
    [[ "$file_header" == "$expected_header" ]] || {
        echo "unexpected header in $file" >&2
        printf 'observed: %s\n' "$file_header" >&2
        printf 'expected: %s\n' "$expected_header" >&2
        exit 2
    }
    [[ -n "$(sed -n '2p' "$file")" ]] || {
        echo "raw output contains a header but no records: $file" >&2
        exit 2
    }
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
    -v expected_lock="$DEPENDENCY_LOCK_SHA256" \
    -v old_revision="$OLD_REV" \
    -v current_revision="$CURRENT_REV" \
    -v old_variant="$OLD_VARIANT" \
    -v current_variant="$CURRENT_VARIANT" \
    -v repetitions="$REPETITIONS" \
    -v dense_demands="$DENSE_DEMANDS" '
    function value(name) { return $(column[name]) }
    function fail(message) {
        print message > "/dev/stderr"
        bad = 1
    }
    function late_target(scenario) {
        if (scenario == "late-2") return 2
        if (scenario == "late-18") return 18
        if (scenario == "late-146") return 146
        if (scenario == "late-1170") return 1170
        if (scenario == "late-9362") return 9362
        return -1
    }
    function late_processed(target) {
        if (target == 2) return 9
        if (target == 18) return 73
        if (target == 146) return 585
        if (target == 1170) return 4681
        if (target == 9362) return 16384
        return -1
    }
    BEGIN {
        count = split(dense_demands, demand_list, " ")
        for (i = 1; i <= count; i++) dense_demand[demand_list[i]] = 1
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
        if (value("roots") + 0 != 16384)
            fail(sprintf("root count mismatch at line %d", NR))

        engine = value("engine")
        variant = value("engine_variant")
        if (engine == old_revision) {
            if (variant != old_variant)
                fail(sprintf("old revision has variant %s at line %d", variant, NR))
        } else if (engine == current_revision) {
            if (variant != current_variant)
                fail(sprintf("current revision has variant %s at line %d", variant, NR))
        } else {
            fail(sprintf("unexpected engine revision %s at line %d", engine, NR))
        }

        scenario = value("scenario")
        variables = value("variables") + 0
        demand = value("demand")
        target = late_target(scenario)
        if (scenario == "dense-bijective") {
            if (variables != 2 && variables != 3 && variables != 8 && variables != 32)
                fail(sprintf("unexpected dense variable count %d at line %d", variables, NR))
            if (!(demand in dense_demand))
                fail(sprintf("unexpected dense demand %s at line %d", demand, NR))
            identity_cell = (demand == "full")
        } else if (target >= 0) {
            if (variables != 3 || demand != "1")
                fail(sprintf("invalid late matrix %s/V=%d/demand=%s at line %d",
                             scenario, variables, demand, NR))
            identity_cell = 1
        } else {
            fail(sprintf("unexpected scenario %s at line %d", scenario, NR))
            identity_cell = 0
        }

        invocation = value("invocation_sequence") + 0
        key = invocation SUBSEP scenario SUBSEP demand
        invocation_demand[key] = variables

        expected_rows = (demand == "full") ? 16384 : demand + 0
        if (value("rows") + 0 != expected_rows)
            fail(sprintf("row count mismatch for invocation %d demand %s at line %d",
                         invocation, demand, NR))

        record = value("record")
        if (record == "diagnostic" || record == "identity") {
            summaries[key]++
            expected_work[key] = value("proposals") + 0
        } else if (record == "calls") {
            call_rows[key]++
            call_sum[key] += value("propose_outputs") + 0
        } else if (record == "sample") {
            samples[key]++
        } else {
            fail(sprintf("unknown record type %s at line %d", record, NR))
        }

        if (scenario == "dense-bijective" && demand == "full") {
            expected_proposals = 16384 * variables
            if (value("proposals") + 0 != expected_proposals)
                fail(sprintf("full proposal count mismatch at line %d", NR))
        } else if (target >= 0) {
            expected_proposals = 16384 + late_processed(target) + target + 1
            if (value("proposals") + 0 != expected_proposals)
                fail(sprintf("late proposal count mismatch for %s at line %d: got %d expected %d",
                             scenario, NR, value("proposals") + 0, expected_proposals))
        }

        if (record == "identity") {
            if (!identity_cell)
                fail(sprintf("non-identity cell labelled identity at line %d", NR))
            digest = value("result_digest")
            if (digest == "-")
                fail(sprintf("identity summary lacks digest at line %d", NR))
            identity_key = scenario SUBSEP variables
            if (!(identity_key in identity_digest))
                identity_digest[identity_key] = digest
            else if (identity_digest[identity_key] != digest)
                fail(sprintf("identity digest mismatch for %s/V=%d at line %d",
                             scenario, variables, NR))
            identity_count[identity_key]++
        } else {
            if (value("result_digest") != "-")
                fail(sprintf("only diagnostic identity rows may carry a digest at line %d", NR))
            if (record == "diagnostic" && identity_cell)
                fail(sprintf("identity cell was labelled diagnostic at line %d", NR))
        }
    }
    END {
        for (key in invocation_demand) {
            variables = invocation_demand[key]
            if (summaries[key] != 1)
                fail(sprintf("summary count for invocation/demand %s is %d, expected 1",
                             key, summaries[key]))
            if (samples[key] != repetitions)
                fail(sprintf("sample count for invocation/demand %s is %d, expected %d",
                             key, samples[key], repetitions))
            if (call_rows[key] != variables)
                fail(sprintf("call-ledger rows for invocation/demand %s is %d, expected %d",
                             key, call_rows[key], variables))
            if (call_sum[key] != expected_work[key])
                fail(sprintf("call-ledger proposal sum for invocation/demand %s is %d, expected %d",
                             key, call_sum[key], expected_work[key]))
        }
        for (identity_key in identity_count) {
            if (identity_count[identity_key] != 4)
                fail(sprintf("identity count for %s is %d, expected 4",
                             identity_key, identity_count[identity_key]))
        }
        exit bad
    }
' "$OUT/observations.tsv"

"$OUT/analyzer.py" "$OUT"
echo "complete: $OUT/observations.tsv" >&2
