#!/usr/bin/env python3
"""Validate and reduce a query-engine demand-curve result directory.

The runner records repetitions in interleaved A/B/B/A blocks.  This analysis
first takes the median inside each process invocation, then combines the two
bookends for an arm with a geometric mean.  Ratios therefore retain the ABBA
drift control instead of pooling chronologically distinct samples.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Mapping, Sequence


CELL_FIELDS = (
    "scale",
    "batch_parents",
    "fanout",
    "backend",
    "substrate",
    "parallelism",
    "shape",
    "demand",
)
POSITIONS = (
    "primary-A1",
    "primary-B1",
    "primary-B2",
    "primary-A2",
    "ablation-C1",
    "ablation-B1",
    "ablation-B2",
    "ablation-C2",
)
WORK_FIELDS = (
    "frontier_expansions",
    "frontier_rows",
    "variable_groups",
    "proposals",
    "widest_frontier",
    "inplace_descents",
    "copied_descents",
    "gpu_confirms",
    "gpu_candidates",
    "cpu_fallback_confirms",
    "cpu_fallback_candidates",
    "gpu_errors",
)
REQUIRED_FIELDS = {
    "record",
    "run_id",
    "abba_position",
    "invocation_sequence",
    "repetition",
    "engine",
    "engine_variant",
    "harness",
    "dependency_lock",
    "corpus",
    "elapsed_ns",
    "rows",
    "result_digest",
    *CELL_FIELDS,
    *WORK_FIELDS,
}


def geometric_mean(values: Iterable[float]) -> float:
    values = tuple(values)
    if not values or any(value <= 0 for value in values):
        raise ValueError(f"geometric mean requires positive values: {values}")
    return math.exp(sum(math.log(value) for value in values) / len(values))


def quantile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("empty quantile")
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        missing = REQUIRED_FIELDS - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path}: missing columns: {sorted(missing)}")
        return list(reader)


def read_manifest(path: Path) -> dict[str, str]:
    with path.open(newline="") as stream:
        rows = csv.DictReader(stream, delimiter="\t")
        result = {row["key"]: row["value"] for row in rows}
    if result.get("protocol") != "query-engine-demand-curve-v1":
        raise ValueError(f"{path}: unexpected protocol {result.get('protocol')!r}")
    return result


def one(values: Iterable[str], description: str) -> str:
    distinct = set(values)
    if len(distinct) != 1:
        raise ValueError(f"expected one {description}, found {sorted(distinct)}")
    return next(iter(distinct))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_artifact(result_dir: Path, manifest: Mapping[str, str]) -> None:
    expected = {
        "harness.rs": manifest["harness_sha256"],
        "runner.sh": manifest["runner_sha256"],
        "locks/Cargo.lock": manifest["dependency_lock_sha256"],
        "bin/query_engine_demand_curve-old": manifest["old_binary_sha256"],
        "bin/query_engine_demand_curve-current": manifest["current_binary_sha256"],
        "bin/query_engine_demand_curve-current-w1": manifest[
            "current_w1_binary_sha256"
        ],
    }
    for relative, expected_hash in expected.items():
        path = result_dir / relative
        if not path.is_file():
            raise ValueError(f"artifact is missing {relative}")
        observed_hash = sha256(path)
        if observed_hash != expected_hash:
            raise ValueError(
                f"artifact hash mismatch for {relative}: "
                f"observed={observed_hash} expected={expected_hash}"
            )


def validate(
    rows: Sequence[Mapping[str, str]], manifest: Mapping[str, str]
) -> dict[str, int]:
    if not rows:
        raise ValueError("observations are empty")
    observed_run = one((row["run_id"] for row in rows), "run id")
    if observed_run != manifest["run_id"]:
        raise ValueError("manifest/observation run-id mismatch")
    observed_harness = one((row["harness"] for row in rows), "harness hash")
    if observed_harness != manifest["harness_sha256"]:
        raise ValueError("manifest/observation harness mismatch")
    observed_lock = one((row["dependency_lock"] for row in rows), "dependency lock")
    if observed_lock != manifest["dependency_lock_sha256"]:
        raise ValueError("manifest/observation dependency-lock mismatch")
    if any(int(row["gpu_errors"]) != 0 for row in rows):
        raise ValueError("at least one observation reports a GPU error")

    expected_arms = {
        "primary-A1": (manifest["old_revision"], "default"),
        "primary-B1": (manifest["current_revision"], "default"),
        "primary-B2": (manifest["current_revision"], "default"),
        "primary-A2": (manifest["old_revision"], "default"),
        "ablation-C1": (manifest["current_revision"], "frontier-w1"),
        "ablation-B1": (manifest["current_revision"], "default"),
        "ablation-B2": (manifest["current_revision"], "default"),
        "ablation-C2": (manifest["current_revision"], "frontier-w1"),
    }
    scales = manifest["scales"].split()
    scale_ordinals = {scale: ordinal for ordinal, scale in enumerate(scales)}
    position_ordinals = {
        position: ordinal for ordinal, position in enumerate(POSITIONS)
    }
    for row in rows:
        position = row["abba_position"]
        if position not in expected_arms:
            raise ValueError(f"unexpected ABBA position {position!r}")
        observed_arm = (row["engine"], row["engine_variant"])
        if observed_arm != expected_arms[position]:
            raise ValueError(
                f"ABBA arm mismatch at {position}: "
                f"observed={observed_arm} expected={expected_arms[position]}"
            )
        scale = row["scale"]
        if scale not in scale_ordinals:
            raise ValueError(f"observation has unmanifested scale {scale!r}")
        expected_invocation = (
            scale_ordinals[scale] * len(POSITIONS) + position_ordinals[position]
        )
        if int(row["invocation_sequence"]) != expected_invocation:
            raise ValueError(
                f"invocation mismatch for {scale}/{position}: "
                f"observed={row['invocation_sequence']} expected={expected_invocation}"
            )

    corpora: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        corpora[row["scale"]].add(row["corpus"])
    bad_corpora = {
        scale: values for scale, values in corpora.items() if len(values) != 1
    }
    if bad_corpora:
        raise ValueError(f"corpus identity differs within scales: {bad_corpora}")

    identity: dict[tuple[str, ...], set[str]] = defaultdict(set)
    identity_arms: dict[tuple[str, ...], set[tuple[str, str]]] = defaultdict(set)
    for row in rows:
        if row["record"] != "identity":
            continue
        key = tuple(row[field] for field in CELL_FIELDS[:-1])
        identity[key].add(row["result_digest"])
        identity_arms[key].add((row["engine"], row["engine_variant"]))
    mismatches = {
        key: digests for key, digests in identity.items() if len(digests) != 1
    }
    if mismatches:
        raise ValueError(f"identity digest mismatch in {len(mismatches)} cells")
    if any(len(arms) != 3 for arms in identity_arms.values()):
        raise ValueError("an identity cell does not contain all three engine arms")

    expected_repetitions = int(manifest["repetitions_per_invocation"])
    samples: dict[tuple[str, ...], list[int]] = defaultdict(list)
    repetitions: dict[tuple[str, ...], set[int]] = defaultdict(set)
    for row in rows:
        if row["record"] != "sample":
            continue
        key = tuple(row[field] for field in CELL_FIELDS) + (row["abba_position"],)
        samples[key].append(int(row["elapsed_ns"]))
        repetitions[key].add(int(row["repetition"]))
    expected_indices = set(range(expected_repetitions))
    for key, values in samples.items():
        if len(values) != expected_repetitions or repetitions[key] != expected_indices:
            raise ValueError(
                f"incomplete repetitions for {key}: {len(values)} {repetitions[key]}"
            )

    record_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        record_counts[row["record"]] += 1
    return dict(record_counts)


def reduce_samples(rows: Sequence[Mapping[str, str]]) -> list[dict[str, object]]:
    samples: dict[tuple[str, ...], list[int]] = defaultdict(list)
    observed_rows: dict[tuple[str, ...], set[int]] = defaultdict(set)
    for row in rows:
        if row["record"] != "sample":
            continue
        key = tuple(row[field] for field in CELL_FIELDS) + (row["abba_position"],)
        samples[key].append(int(row["elapsed_ns"]))
        observed_rows[key].add(int(row["rows"]))

    medians = {key: statistics.median(values) for key, values in samples.items()}
    cells = sorted({key[:-1] for key in samples}, key=cell_sort_key)
    reduced: list[dict[str, object]] = []
    for cell in cells:
        absent = [
            position for position in POSITIONS if cell + (position,) not in medians
        ]
        if absent:
            raise ValueError(f"cell {cell} lacks ABBA positions {absent}")
        row_counts = set()
        for position in POSITIONS:
            row_counts.update(observed_rows[cell + (position,)])
        if len(row_counts) != 1:
            raise ValueError(f"row-count mismatch across arms for {cell}: {row_counts}")

        old_ns = geometric_mean(
            medians[cell + (position,)] for position in ("primary-A1", "primary-A2")
        )
        current_primary_ns = geometric_mean(
            medians[cell + (position,)] for position in ("primary-B1", "primary-B2")
        )
        current_ablation_ns = geometric_mean(
            medians[cell + (position,)] for position in ("ablation-B1", "ablation-B2")
        )
        width_one_ns = geometric_mean(
            medians[cell + (position,)] for position in ("ablation-C1", "ablation-C2")
        )
        item: dict[str, object] = dict(zip(CELL_FIELDS, cell, strict=True))
        item.update(
            rows=next(iter(row_counts)),
            old_ns=old_ns,
            current_primary_ns=current_primary_ns,
            current_ablation_ns=current_ablation_ns,
            frontier_w1_ns=width_one_ns,
            current_over_old=current_primary_ns / old_ns,
            default_over_w1=current_ablation_ns / width_one_ns,
            current_block_ratio=current_ablation_ns / current_primary_ns,
        )
        reduced.append(item)
    return reduced


def demand_rank(demand: str) -> tuple[int, int]:
    if demand == "construct":
        return (0, 0)
    if demand == "full":
        return (2, 0)
    return (1, int(demand))


def cell_sort_key(cell: tuple[str, ...]) -> tuple[object, ...]:
    values = dict(zip(CELL_FIELDS, cell, strict=True))
    return (
        int(values["batch_parents"]),
        values["backend"],
        values["substrate"],
        values["parallelism"],
        values["shape"],
        demand_rank(values["demand"]),
    )


def write_tsv(
    path: Path, rows: Sequence[Mapping[str, object]], fields: Sequence[str]
) -> None:
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=fields, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({field: format_value(row[field]) for field in fields})


def format_value(value: object) -> object:
    if isinstance(value, float):
        return f"{value:.9g}"
    return value


def aggregate_ratio(
    reduced: Sequence[Mapping[str, object]],
    *,
    parallelism: str,
    demand: str,
) -> list[dict[str, object]]:
    groups: dict[tuple[str, str, str], list[Mapping[str, object]]] = defaultdict(list)
    for row in reduced:
        if row["parallelism"] == parallelism and row["demand"] == demand:
            groups[
                (str(row["backend"]), str(row["substrate"]), str(row["shape"]))
            ].append(row)
    output = []
    for (backend, substrate, shape), group in sorted(groups.items()):
        primary = [float(row["current_over_old"]) for row in group]
        ablation = [float(row["default_over_w1"]) for row in group]
        output.append(
            {
                "backend": backend,
                "substrate": substrate,
                "shape": shape,
                "scales": len(group),
                "current_over_old_gmean": geometric_mean(primary),
                "current_over_old_min": min(primary),
                "current_over_old_max": max(primary),
                "default_over_w1_gmean": geometric_mean(ablation),
                "default_over_w1_min": min(ablation),
                "default_over_w1_max": max(ablation),
            }
        )
    return output


def affine_residuals(
    reduced: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Fit endpoint-calibrated affine curves and score held-out middle points."""

    curve_fields = CELL_FIELDS[:-1]
    groups: dict[tuple[str, ...], list[Mapping[str, object]]] = defaultdict(list)
    for row in reduced:
        if row["parallelism"] == "sequential":
            groups[tuple(str(row[field]) for field in curve_fields)].append(row)

    arms = {
        "old": "old_ns",
        "current": "current_primary_ns",
        "frontier-w1": "frontier_w1_ns",
    }
    calibration_labels = {"construct", "1", "2", "4096", "16384", "full"}
    output: list[dict[str, object]] = []
    for key, group in sorted(groups.items()):
        by_demand = {str(row["demand"]): row for row in group}
        held_out = [
            row
            for row in group
            if str(row["demand"]).isdigit() and 4 <= int(str(row["demand"])) <= 1024
        ]
        if not held_out:
            continue
        for arm, timing_field in arms.items():
            calibration = [
                (float(row["rows"]), float(row[timing_field]))
                for demand, row in by_demand.items()
                if demand in calibration_labels
            ]
            if len(calibration) < 2:
                continue
            count = len(calibration)
            sx = sum(x for x, _ in calibration)
            sy = sum(y for _, y in calibration)
            sxx = sum(x * x for x, _ in calibration)
            sxy = sum(x * y for x, y in calibration)
            denominator = count * sxx - sx * sx
            if denominator == 0:
                continue
            slope = (count * sxy - sx * sy) / denominator
            intercept = (sy - slope * sx) / count
            errors = []
            for row in held_out:
                observed = float(row[timing_field])
                predicted = intercept + slope * float(row["rows"])
                errors.append(abs(predicted - observed) / observed)
            item: dict[str, object] = dict(zip(curve_fields, key, strict=True))
            item.update(
                arm=arm,
                calibration_points=count,
                held_out_points=len(errors),
                intercept_ns=intercept,
                slope_ns_per_row=slope,
                median_abs_relative_error=statistics.median(errors),
                max_abs_relative_error=max(errors),
            )
            output.append(item)
    return output


def work_summary(rows: Sequence[Mapping[str, str]]) -> list[dict[str, object]]:
    groups: dict[tuple[str, ...], list[Mapping[str, str]]] = defaultdict(list)
    for row in rows:
        if row["record"] != "work":
            continue
        key = tuple(
            row[field]
            for field in (
                "scale",
                "batch_parents",
                "fanout",
                "backend",
                "substrate",
                "parallelism",
                "shape",
                "demand",
                "engine",
                "engine_variant",
            )
        )
        groups[key].append(row)

    output = []
    key_fields = (
        "scale",
        "batch_parents",
        "fanout",
        "backend",
        "substrate",
        "parallelism",
        "shape",
        "demand",
        "engine",
        "engine_variant",
    )
    for key, group in sorted(groups.items(), key=lambda item: item[0]):
        item: dict[str, object] = dict(zip(key_fields, key, strict=True))
        item["observations"] = len(group)
        for field in WORK_FIELDS:
            values = [int(row[field]) for row in group]
            item[f"{field}_median"] = statistics.median(values)
            item[f"{field}_min"] = min(values)
            item[f"{field}_max"] = max(values)
        output.append(item)
    return output


def markdown_table(
    headers: Sequence[str], rows: Iterable[Sequence[object]]
) -> list[str]:
    result = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    result.extend("| " + " | ".join(str(value) for value in row) + " |" for row in rows)
    return result


def pct_ratio(value: object) -> str:
    return f"{(float(value) - 1.0) * 100:+.1f}%"


def render_summary(
    manifest: Mapping[str, str],
    rows: Sequence[Mapping[str, str]],
    reduced: Sequence[Mapping[str, object]],
    record_counts: Mapping[str, int],
    sequential_full: Sequence[Mapping[str, object]],
    rayon_full: Sequence[Mapping[str, object]],
    work: Sequence[Mapping[str, object]],
    affine: Sequence[Mapping[str, object]],
) -> str:
    def work_arm(row: Mapping[str, object]) -> str:
        if row["engine"] == manifest["old_revision"]:
            return "old/default"
        if row["engine_variant"] == "frontier-w1":
            return "current/W1"
        return "current/default"

    arm_order = {"old/default": 0, "current/default": 1, "current/W1": 2}
    scale_count = len(set(row["scale"] for row in rows))
    scale_noun = "scale" if scale_count == 1 else "scales"
    lines = [
        "# Query-engine demand curve",
        "",
        f"Run `{manifest['run_id']}` compares `{manifest['old_revision'][:8]}` with "
        f"`{manifest['current_revision'][:8]}` and the same-source width-one ablation.",
        "",
        "## Integrity",
        "",
        f"- {len(rows):,} observations: "
        + ", ".join(
            f"{count:,} {kind}" for kind, count in sorted(record_counts.items())
        )
        + ".",
        f"- One dependency lock (`{manifest['dependency_lock_sha256'][:12]}…`) is baked into every arm.",
        f"- {scale_count} {scale_noun}, each with one corpus digest; "
        "every identity cell has one exact projected-tuple digest across all three arms.",
        "- No observation reports a GPU error.",
        "",
        "## Method",
        "",
        "Each number is the median within an invocation, followed by the geometric mean of "
        "the two ABBA bookends. `current/old` above 1 means the current engine is slower; "
        "`default/W1` below 1 means batching is faster than the same current engine pinned to width one.",
        "Sequential `take(k)` and Rayon `take_any(k)` are separate experiments: Rayon may do "
        "speculative in-flight work and need not return the same prefix. Compare demand points only "
        "within a parallelism setting.",
        "",
        "## Full-drain sequential ratios across scales",
        "",
    ]
    lines += markdown_table(
        ("backend", "shape", "current/old gmean [range]", "default/W1 gmean [range]"),
        (
            (
                f"{row['backend']}/{row['substrate']}",
                row["shape"],
                f"{float(row['current_over_old_gmean']):.3f} "
                f"[{float(row['current_over_old_min']):.3f}, {float(row['current_over_old_max']):.3f}]",
                f"{float(row['default_over_w1_gmean']):.3f} "
                f"[{float(row['default_over_w1_min']):.3f}, {float(row['default_over_w1_max']):.3f}]",
            )
            for row in sequential_full
        ),
    )

    lines += ["", "## Full-drain Rayon ratios across scales", ""]
    lines += markdown_table(
        ("backend", "shape", "current/old gmean [range]", "default/W1 gmean [range]"),
        (
            (
                f"{row['backend']}/{row['substrate']}",
                row["shape"],
                f"{float(row['current_over_old_gmean']):.3f} "
                f"[{float(row['current_over_old_min']):.3f}, {float(row['current_over_old_max']):.3f}]",
                f"{float(row['default_over_w1_gmean']):.3f} "
                f"[{float(row['default_over_w1_min']):.3f}, {float(row['default_over_w1_max']):.3f}]",
            )
            for row in rayon_full
        ),
    )

    wgpu_curve = [
        row
        for row in reduced
        if row["backend"] == "succinct"
        and row["substrate"] == "wgpu"
        and row["parallelism"] == "sequential"
        and row["shape"] == "parent_batch_confirm"
        and row["scale"] in {"threshold", "wide"}
        and row["demand"] in {"1", "512", "4096", "16384", "full"}
    ]
    if wgpu_curve:
        lines += ["", "## WGPU demand points around widening and routing", ""]
        lines += markdown_table(
            ("scale", "demand", "current/old", "default/W1", "current ms"),
            (
                (
                    row["scale"],
                    row["demand"],
                    f"{float(row['current_over_old']):.3f}",
                    f"{float(row['default_over_w1']):.3f}",
                    f"{float(row['current_primary_ns']) / 1e6:.3f}",
                )
                for row in wgpu_curve
            ),
        )

    parent = [
        row
        for row in reduced
        if row["parallelism"] == "sequential"
        and row["shape"] == "parent_batch_confirm"
        and row["demand"] == "full"
    ]
    lines += ["", "## Causal parent-batch full drain", ""]
    lines += markdown_table(
        ("scale", "backend", "old ms", "current ms", "current/old", "default/W1"),
        (
            (
                row["scale"],
                f"{row['backend']}/{row['substrate']}",
                f"{float(row['old_ns']) / 1e6:.3f}",
                f"{float(row['current_primary_ns']) / 1e6:.3f}",
                f"{float(row['current_over_old']):.3f}",
                f"{float(row['default_over_w1']):.3f}",
            )
            for row in parent
        ),
    )

    route_rows = sorted(
        (
            row
            for row in work
            if row["backend"] == "succinct"
            and row["substrate"] == "wgpu"
            and row["parallelism"] == "sequential"
            and row["shape"] == "parent_batch_confirm"
            and row["demand"] == "full"
        ),
        key=lambda row: (
            int(str(row["batch_parents"])),
            arm_order[work_arm(row)],
        ),
    )
    lines += ["", "## Sequential WGPU route receipts", ""]
    lines += markdown_table(
        (
            "scale",
            "arm",
            "widest",
            "GPU confirms/candidates",
            "CPU confirms/candidates",
        ),
        (
            (
                row["scale"],
                work_arm(row),
                int(row["widest_frontier_median"]),
                f"{int(row['gpu_confirms_median'])}/{int(row['gpu_candidates_median'])}",
                f"{int(row['cpu_fallback_confirms_median'])}/{int(row['cpu_fallback_candidates_median'])}",
            )
            for row in route_rows
        ),
    )

    rayon_rows = sorted(
        (
            row
            for row in work
            if row["backend"] == "succinct"
            and row["substrate"] == "wgpu"
            and row["parallelism"] == "rayon"
            and row["shape"] == "parent_batch_confirm"
            and row["demand"] == "full"
        ),
        key=lambda row: (
            int(str(row["batch_parents"])),
            arm_order[work_arm(row)],
        ),
    )
    lines += ["", "## Rayon batching retained at full drain", ""]
    lines += markdown_table(
        ("scale", "arm", "widest median [range]", "expansions median [range]"),
        (
            (
                row["scale"],
                work_arm(row),
                f"{int(row['widest_frontier_median'])} "
                f"[{int(row['widest_frontier_min'])}, {int(row['widest_frontier_max'])}]",
                f"{int(row['frontier_expansions_median'])} "
                f"[{int(row['frontier_expansions_min'])}, {int(row['frontier_expansions_max'])}]",
            )
            for row in rayon_rows
        ),
    )

    affine_rows = sorted(
        (
            row
            for row in affine
            if row["shape"] == "parent_batch_confirm" and row["arm"] == "current"
        ),
        key=lambda row: (
            int(str(row["batch_parents"])),
            str(row["backend"]),
            str(row["substrate"]),
        ),
    )
    lines += [
        "",
        "## Preregistered affine-fit check",
        "",
        "The affine model is calibrated only on `construct`, `1`, `2`, `4096`, `16384`, "
        "and `full` where present; numeric demands `4..1024` are held out. The residuals "
        "below are descriptive rather than folded back into the timing ratios.",
        "",
    ]
    lines += markdown_table(
        ("scale", "backend", "held-out median error", "held-out max error"),
        (
            (
                row["scale"],
                f"{row['backend']}/{row['substrate']}",
                f"{float(row['median_abs_relative_error']) * 100:.1f}%",
                f"{float(row['max_abs_relative_error']) * 100:.1f}%",
            )
            for row in affine_rows
        ),
    )
    lines += [
        "",
        "The middle of most parent-batch curves is visibly non-affine, so the raw demand "
        "curve—not an intercept/slope headline—is the primary result.",
    ]

    spreads: dict[tuple[str, str], list[float]] = defaultdict(list)
    grouped: dict[tuple[str, ...], list[int]] = defaultdict(list)
    for row in rows:
        if row["record"] == "sample":
            key = tuple(row[field] for field in CELL_FIELDS) + (row["abba_position"],)
            grouped[key].append(int(row["elapsed_ns"]))
    for key, values in grouped.items():
        parallelism = key[CELL_FIELDS.index("parallelism")]
        median = statistics.median(values)
        spread = statistics.median(abs(value - median) for value in values) / median
        spreads[(parallelism, "all")].append(spread)
        if median >= 1_000_000:
            spreads[(parallelism, ">=1 ms")].append(spread)
    lines += ["", "## Repeat spread", ""]
    lines += markdown_table(
        ("parallelism", "cells", "median within-invocation MAD/median", "p90", "p99"),
        (
            (
                parallelism,
                scope,
                f"{statistics.median(values) * 100:.1f}%",
                f"{quantile(values, 0.90) * 100:.1f}%",
                f"{quantile(values, 0.99) * 100:.1f}%",
            )
            for (parallelism, scope), values in sorted(spreads.items())
        ),
    )
    lines += [
        "",
        "The complete cell-level reduction is `cells.tsv`; `work.tsv` preserves median/min/max "
        "diagnostic counters because Rayon scheduling may vary them between invocations.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    result_dir: Path = args.result_dir.resolve()
    observations = result_dir / "observations.tsv"
    manifest_path = result_dir / "manifest.tsv"
    rows = read_tsv(observations)
    manifest = read_manifest(manifest_path)
    validate_artifact(result_dir, manifest)
    record_counts = validate(rows, manifest)
    reduced = reduce_samples(rows)
    work = work_summary(rows)
    affine = affine_residuals(reduced)

    analysis_dir = result_dir / "analysis"
    analysis_dir.mkdir(exist_ok=True)
    cell_fields = (
        *CELL_FIELDS,
        "rows",
        "old_ns",
        "current_primary_ns",
        "current_ablation_ns",
        "frontier_w1_ns",
        "current_over_old",
        "default_over_w1",
        "current_block_ratio",
    )
    write_tsv(analysis_dir / "cells.tsv", reduced, cell_fields)
    full = [row for row in reduced if row["demand"] == "full"]
    write_tsv(analysis_dir / "full.tsv", full, cell_fields)

    work_fields = tuple(work[0].keys()) if work else ()
    if work_fields:
        write_tsv(analysis_dir / "work.tsv", work, work_fields)
    sequential_full = aggregate_ratio(reduced, parallelism="sequential", demand="full")
    rayon_full = aggregate_ratio(reduced, parallelism="rayon", demand="full")
    aggregate_fields = tuple(sequential_full[0].keys()) if sequential_full else ()
    if aggregate_fields:
        write_tsv(
            analysis_dir / "sequential-full-summary.tsv",
            sequential_full,
            aggregate_fields,
        )
        write_tsv(analysis_dir / "rayon-full-summary.tsv", rayon_full, aggregate_fields)
    affine_fields = tuple(affine[0].keys()) if affine else ()
    if affine_fields:
        write_tsv(analysis_dir / "affine-residuals.tsv", affine, affine_fields)

    summary = render_summary(
        manifest,
        rows,
        reduced,
        record_counts,
        sequential_full,
        rayon_full,
        work,
        affine,
    )
    (analysis_dir / "summary.md").write_text(summary)
    print(summary)


if __name__ == "__main__":
    main()
