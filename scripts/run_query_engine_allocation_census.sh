#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
if [[ $# -ne 1 ]]; then
    echo "usage: OLD_REV=<rev> CURRENT_REV=<rev> [OLD_PROTOCOL=scalar|frontier] $0 /absolute/output-directory" >&2
    exit 2
fi
OUT=$1
case "$OUT" in
    /*) ;;
    *) echo "output directory must be absolute" >&2; exit 2 ;;
esac
if [[ -e "$OUT" ]] && [[ -n "$(find "$OUT" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "refusing non-empty output directory: $OUT" >&2
    exit 2
fi

OLD_REV=$(git -C "$ROOT" rev-parse "${OLD_REV:-2cd60807}^{commit}")
CURRENT_REV=$(git -C "$ROOT" rev-parse "${CURRENT_REV:-HEAD}^{commit}")
OLD_PROTOCOL=${OLD_PROTOCOL:-scalar}
case "$OLD_PROTOCOL" in
    scalar)
        OLD_RUSTFLAGS="--cfg allocation_census_old"
        DEFAULT_OLD_VARIANT=scalar-dfs
        ;;
    frontier)
        OLD_RUSTFLAGS=""
        DEFAULT_OLD_VARIANT=frontier
        ;;
    *)
        echo "OLD_PROTOCOL must be scalar or frontier, got: $OLD_PROTOCOL" >&2
        exit 2
        ;;
esac
OLD_VARIANT=${OLD_VARIANT:-$DEFAULT_OLD_VARIANT}
CURRENT_VARIANT=${CURRENT_VARIANT:-frontier}
HARNESS="$ROOT/examples/query_engine_allocation_census.rs"
ANALYZER="$ROOT/scripts/analyze_query_engine_allocation_census.py"
HARNESS_SHA256=$(shasum -a 256 "$HARNESS" | awk '{print $1}')
RUNNER_SHA256=$(shasum -a 256 "$0" | awk '{print $1}')
ANALYZER_SHA256=$(shasum -a 256 "$ANALYZER" | awk '{print $1}')
CACHE=${ALLOCATION_CENSUS_CACHE_ROOT:-/private/tmp/triblespace-allocation-census-cache}

mkdir -p "$OUT"/{bin,build-logs,locks,raw}
mkdir -p "$CACHE"/{subjects,targets}
cp "$HARNESS" "$OUT/harness.rs"
cp "$0" "$OUT/runner.sh"
cp "$ANALYZER" "$OUT/analyzer.py"

prepare_subject() {
    local label=$1 revision=$2
    local subject="$CACHE/subjects/$label-${revision:0:12}"
    if [[ ! -e "$subject/.git" ]]; then
        git -C "$ROOT" worktree add --detach "$subject" "$revision"
    fi
    [[ "$(git -C "$subject" rev-parse HEAD)" == "$revision" ]]
    git -C "$subject" diff --quiet HEAD -- . \
        ':(exclude)Cargo.lock' \
        ':(exclude)examples/query_engine_allocation_census.rs'
    local unexpected
    unexpected=$(git -C "$subject" ls-files --others --exclude-standard |
        grep -Ev '^(Cargo.lock|examples/query_engine_allocation_census.rs)$' || true)
    [[ -z "$unexpected" ]] || {
        echo "$subject has unexpected untracked files: $unexpected" >&2
        exit 2
    }
    cp "$HARNESS" "$subject/examples/query_engine_allocation_census.rs"
}

prepare_subject old "$OLD_REV"
prepare_subject current "$CURRENT_REV"
OLD_SUBJECT="$CACHE/subjects/old-${OLD_REV:0:12}"
CURRENT_SUBJECT="$CACHE/subjects/current-${CURRENT_REV:0:12}"

if [[ ! -f "$CURRENT_SUBJECT/Cargo.lock" ]]; then
    cargo generate-lockfile --manifest-path "$CURRENT_SUBJECT/Cargo.toml" \
        >"$OUT/build-logs/dependency-lock.log" 2>&1
else
    printf 'reused %s\n' "$CURRENT_SUBJECT/Cargo.lock" >"$OUT/build-logs/dependency-lock.log"
fi
cp "$CURRENT_SUBJECT/Cargo.lock" "$OLD_SUBJECT/Cargo.lock"
cp "$CURRENT_SUBJECT/Cargo.lock" "$OUT/locks/Cargo.lock"
LOCK_SHA256=$(shasum -a 256 "$OUT/locks/Cargo.lock" | awk '{print $1}')

build() {
    local label=$1 variant=$2 subject=$3 revision=$4 rustflags=$5
    local target="$CACHE/targets/$label-${revision:0:12}-${HARNESS_SHA256:0:12}"
    echo "building $label $revision ($variant)" >&2
    env \
        RUSTFLAGS="$rustflags" \
        ALLOCATION_CENSUS_ENGINE_REVISION="$revision" \
        ALLOCATION_CENSUS_ENGINE_VARIANT="$variant" \
        ALLOCATION_CENSUS_HARNESS_SHA256="$HARNESS_SHA256" \
        ALLOCATION_CENSUS_LOCK_SHA256="$LOCK_SHA256" \
        CARGO_TARGET_DIR="$target" \
        cargo build --manifest-path "$subject/Cargo.toml" --locked --release \
            --no-default-features --example query_engine_allocation_census \
            >"$OUT/build-logs/$label.log" 2>&1
    cp "$target/release/examples/query_engine_allocation_census" "$OUT/bin/$label"
    chmod a-w "$OUT/bin/$label"
}

build old "$OLD_VARIANT" "$OLD_SUBJECT" "$OLD_REV" "$OLD_RUSTFLAGS"
build current "$CURRENT_VARIANT" "$CURRENT_SUBJECT" "$CURRENT_REV" ""

# Invocation order is deliberately A/B/B/A even though the census contains no
# timing: duplicate invocations expose nondeterministic/background allocations.
"$OUT/bin/old" >"$OUT/raw/00-A1-old.tsv"
"$OUT/bin/current" >"$OUT/raw/01-B1-current.tsv"
"$OUT/bin/current" >"$OUT/raw/02-B2-current.tsv"
"$OUT/bin/old" >"$OUT/raw/03-A2-old.tsv"
diff -u "$OUT/raw/00-A1-old.tsv" "$OUT/raw/03-A2-old.tsv" >"$OUT/raw/old-repeat.diff"
diff -u "$OUT/raw/01-B1-current.tsv" "$OUT/raw/02-B2-current.tsv" >"$OUT/raw/current-repeat.diff"
python3 "$ANALYZER" "$OUT/raw/00-A1-old.tsv" "$OUT/raw/01-B1-current.tsv" \
    >"$OUT/summary.tsv"

{
    printf 'key\tvalue\n'
    printf 'protocol\tquery-engine-allocation-census-v1\n'
    printf 'old_revision\t%s\n' "$OLD_REV"
    printf 'current_revision\t%s\n' "$CURRENT_REV"
    printf 'old_protocol\t%s\n' "$OLD_PROTOCOL"
    printf 'old_variant\t%s\n' "$OLD_VARIANT"
    printf 'current_variant\t%s\n' "$CURRENT_VARIANT"
    printf 'harness_sha256\t%s\n' "$HARNESS_SHA256"
    printf 'runner_sha256\t%s\n' "$RUNNER_SHA256"
    printf 'analyzer_sha256\t%s\n' "$ANALYZER_SHA256"
    printf 'lock_sha256\t%s\n' "$LOCK_SHA256"
    printf 'old_binary_sha256\t%s\n' "$(shasum -a 256 "$OUT/bin/old" | awk '{print $1}')"
    printf 'current_binary_sha256\t%s\n' "$(shasum -a 256 "$OUT/bin/current" | awk '{print $1}')"
    printf 'allocator_metric\trequested_bytes\n'
    printf 'invocation_order\tA/B/B/A\n'
    printf 'repeat_identity\tbyte-exact\n'
    printf 'rustc\t%s\n' "$(rustc --version)"
    printf 'uname\t%s\n' "$(uname -a)"
} >"$OUT/manifest.tsv"

shasum -a 256 "$OUT"/{harness.rs,runner.sh,analyzer.py,summary.tsv,manifest.tsv} \
    "$OUT"/bin/{old,current} "$OUT"/raw/*.tsv >"$OUT/SHA256SUMS"
echo "$OUT"
