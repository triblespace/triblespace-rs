#!/bin/sh
# bench.sh — THE single command: bench any subject rev or checkout.
#
#   ./bench.sh <rev-or-path> [runner args…]
#
#   <rev-or-path>  a git rev in the enclosing repo (a detached worktree
#                  is created/reused under ./subjects/<rev12>), or a
#                  path to any triblespace checkout.
#   runner args    forwarded verbatim to the tribleset-bench binary
#                  (--results/--label/--data/…; run with none to see
#                  usage).
#
# MECHANISM (dep repointing): the `subject` dependency in Cargo.toml is
# a path dep on the `subjects/current` symlink; this script points the
# symlink at the requested checkout and runs `cargo run --release`.
# Cargo `[patch]`/`--config` patching was considered and rejected: a
# patch section replaces a *source* (registry or git URL) and cannot
# retarget a path dependency, and declaring the subject as a registry
# dep so it becomes patchable would let cargo unify it with the results
# LEDGER (also package `triblespace`) — one crate instance instead of
# the two the suite's design requires. The symlink keeps the manifest
# static, needs no cargo nightly features, and cargo fingerprints the
# resolved target, so switching subjects triggers exactly the rebuild
# it should.
#
# The default target (checked in) is ../.. — the enclosing repo, so a
# fresh clone benches the current checkout with no setup. `./bench.sh
# .. …` repoints back at it after benching another subject (or
# `git checkout -- subjects/current`).

set -eu
cd "$(dirname "$0")"

[ $# -ge 1 ] || {
    echo "usage: ./bench.sh <rev-or-path> [runner args…]" >&2
    exit 2
}
arg="$1"
shift

mkdir -p subjects
if [ -d "$arg" ]; then
    target=$(cd "$arg" && pwd)
else
    rev=$(git -C .. rev-parse --verify --short=12 "$arg^{commit}") || {
        echo "bench.sh: '$arg' is neither a directory nor a rev in the enclosing repo" >&2
        exit 2
    }
    wt="subjects/$rev"
    if [ ! -d "$wt" ]; then
        git -C .. worktree add --detach "$(pwd)/$wt" "$rev"
    fi
    target=$(cd "$wt" && pwd)
fi

ln -sfn "$target" subjects/current
echo "bench.sh : subject -> $target" >&2

# Capability probe: every feature below couples the suite to an engine ERA,
# so we ask the SUBJECT what it can do instead of assuming. Getting this
# wrong is not a soft failure — it is a compile error against older revs,
# which is exactly how the era-portability property gets lost.
SUBJECT="$(cd subjects/current && pwd -P)"
FEATURES=""
# gpu: the subject must actually ship the triblespace-gpu crate (F10 reads
# DEFAULT_MIN_CONFIRM_BATCH_RANGE out of it rather than copying the number).
if [ -d "$SUBJECT/triblespace-gpu" ] && grep -q '^gpu = ' "$SUBJECT/Cargo.toml" 2>/dev/null; then
  FEATURES="$FEATURES gpu"
fi
# protocol-v2: F11 implements Constraint by hand, so it needs the
# post-Candidates protocol (engine/owned-mask onward).
if grep -q 'pub struct Candidates' "$SUBJECT/triblespace-core/src/query.rs" 2>/dev/null; then
  FEATURES="$FEATURES protocol-v2"
fi
# frontier: propose/confirm take a batch of parent bindings rather than
# one (engine/batched-frontier onward). Refines protocol-v2, so it is
# only ever probed for alongside it.
if grep -q 'pub struct Frontier' "$SUBJECT/triblespace-core/src/query.rs" 2>/dev/null; then
  FEATURES="$FEATURES frontier"
  # frontier-widest: FrontierStats gained widest/inplace_descents/
  # copied_descents after the batched protocol landed, so the ceiling
  # half of the census is probed separately from the protocol itself.
  if grep -q 'pub fn widest' "$SUBJECT/triblespace-core/src/query.rs" 2>/dev/null; then
    FEATURES="$FEATURES frontier-widest"
  fi
fi
# rpq: only when the subject still has a regular-path constraint.
if [ -f "$SUBJECT/triblespace-core/src/query/regularpathconstraint.rs" ]; then
  FEATURES="$FEATURES rpq"
fi
FEATURES="${FEATURES# }"
if [ -n "$FEATURES" ]; then
  echo "bench.sh: subject capabilities -> ${FEATURES// /, }" >&2
  exec cargo run --release --features "${FEATURES// /,}" -- "$@"
else
  echo "bench.sh: subject capabilities -> none (baseline suite only)" >&2
  exec cargo run --release -- "$@"
fi
