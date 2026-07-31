#!/usr/bin/env python3
"""Validate and reduce a query-engine source-credit benchmark artifact.

The runner executes the double-geometric and inherited-credit engines in
sequential A/B/B/A order for each scenario. Timings are reduced in two stages:
take the median inside each process invocation, then take the geometric mean of
each arm's two invocations. This preserves the runner's drift control.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable, Mapping, Sequence


PROTOCOL = "query-engine-source-credit-v1"
POSITIONS = ("A1", "B1", "B2", "A2")
DENSE_SCENARIO = "dense-bijective"
DENSE_DEMANDS = (
    "1",
    "2",
    "3",
    "9",
    "10",
    "11",
    "73",
    "74",
    "75",
    "585",
    "586",
    "587",
    "4681",
    "4682",
    "4683",
    "full",
)
LATE_TARGETS = (2, 18, 146, 1170, 9362)
LATE_SCENARIOS = tuple(f"late-{target}" for target in LATE_TARGETS)
SCENARIOS = (DENSE_SCENARIO, *LATE_SCENARIOS)
LATE_PROCESSED_ROOTS = {
    2: 9,
    18: 73,
    146: 585,
    1170: 4681,
    9362: 16384,
}
LATE_SOURCE_PAGES = {2: 3, 18: 6, 146: 10, 1170: 15, 9362: 21}
LATE_LAST_WIDTH = {2: 7, 18: 55, 146: 439, 1170: 3511, 9362: 7022}
POSITION_ARM = {
    "A1": "double-geometric",
    "A2": "double-geometric",
    "B1": "inherited-credit",
    "B2": "inherited-credit",
}
ARM_POSITIONS = {
    "double-geometric": ("A1", "A2"),
    "inherited-credit": ("B1", "B2"),
}
WORK_FIELDS = (
    "expansions",
    "frontier_rows",
    "variable_groups",
    "proposals",
    "widest",
    "inplace_descents",
    "copied_descents",
)
CALL_FIELDS = (
    "propose_calls",
    "propose_frontier_rows",
    "propose_outputs",
    "propose_max_width",
)
REQUIRED_FIELDS = {
    "record",
    "run_id",
    "abba_position",
    "invocation_sequence",
    "engine",
    "engine_variant",
    "harness",
    "dependency_lock",
    "roots",
    "scenario",
    "variables",
    "demand",
    "repetition",
    "elapsed_ns",
    "rows",
    "result_digest",
    *WORK_FIELDS,
    "constraint",
    "constraint_kind",
    *CALL_FIELDS,
    "propose_widths",
}
REQUIRED_MANIFEST = {
    "protocol",
    "run_id",
    "old_revision",
    "current_revision",
    "old_variant",
    "current_variant",
    "harness_sha256",
    "runner_sha256",
    "analyzer_sha256",
    "dependency_lock_sha256",
    "old_binary_sha256",
    "current_binary_sha256",
    "roots",
    "scenarios",
    "dense_variables",
    "late_variables",
    "dense_demands",
    "late_demands",
    "repetitions_per_invocation",
    "warmups_per_invocation",
}


def geometric_mean(values: Iterable[float]) -> float:
    items = tuple(values)
    if not items or any(value <= 0 for value in items):
        raise ValueError(f"geometric mean requires positive values: {items}")
    return math.exp(sum(math.log(value) for value in items) / len(items))


def quantile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot take a quantile of an empty sequence")
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def one(values: Iterable[str], description: str) -> str:
    distinct = set(values)
    if len(distinct) != 1:
        raise ValueError(f"expected one {description}, found {sorted(distinct)}")
    return next(iter(distinct))


def parse_int(value: str, description: str) -> int:
    try:
        result = int(value)
    except ValueError as error:
        raise ValueError(f"{description} is not an integer: {value!r}") from error
    return result


def read_manifest(path: Path) -> dict[str, str]:
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        if reader.fieldnames != ["key", "value"]:
            raise ValueError(
                f"{path}: expected manifest columns ['key', 'value'], "
                f"found {reader.fieldnames}"
            )
        result: dict[str, str] = {}
        for row in reader:
            key = row["key"]
            if key in result:
                raise ValueError(f"{path}: duplicate manifest key {key!r}")
            result[key] = row["value"]
    missing = REQUIRED_MANIFEST - set(result)
    if missing:
        raise ValueError(f"{path}: missing manifest keys: {sorted(missing)}")
    if result["protocol"] != PROTOCOL:
        raise ValueError(
            f"{path}: expected protocol {PROTOCOL!r}, " f"found {result['protocol']!r}"
        )
    return result


def read_observations(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)):
            raise ValueError(f"{path}: duplicate observation column")
        missing = REQUIRED_FIELDS - set(fields)
        if missing:
            raise ValueError(f"{path}: missing columns: {sorted(missing)}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path}: observations are empty")
    return rows


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
        "analyzer.py": manifest["analyzer_sha256"],
        "locks/Cargo.lock": manifest["dependency_lock_sha256"],
        "bin/query_engine_source_credit-old": manifest["old_binary_sha256"],
        "bin/query_engine_source_credit-current": manifest["current_binary_sha256"],
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


def expected_rows(demand: str, roots: int) -> int:
    if demand == "full":
        return roots
    rows = parse_int(demand, f"demand {demand!r}")
    if rows < 0 or rows > roots:
        raise ValueError(f"demand {demand!r} is outside 0..={roots}")
    return rows


def manifest_matrix(
    manifest: Mapping[str, str],
) -> list[tuple[str, int, tuple[str, ...]]]:
    scenarios = tuple(manifest["scenarios"].split())
    if scenarios != SCENARIOS:
        raise ValueError(
            f"manifest scenarios are {scenarios}, expected exact matrix {SCENARIOS}"
        )
    dense_variables = tuple(
        parse_int(value, "dense variable")
        for value in manifest["dense_variables"].split()
    )
    if (
        not dense_variables
        or len(dense_variables) != len(set(dense_variables))
        or any(value not in {2, 3, 8, 32} for value in dense_variables)
    ):
        raise ValueError(
            "dense_variables must be a nonempty unique subset of 2, 3, 8, and 32"
        )
    late_variables = tuple(
        parse_int(value, "late variable")
        for value in manifest["late_variables"].split()
    )
    if late_variables != (3,):
        raise ValueError("late_variables must be exactly 3")
    dense_demands = tuple(manifest["dense_demands"].split())
    if dense_demands != DENSE_DEMANDS:
        raise ValueError(f"dense demands are {dense_demands}, expected {DENSE_DEMANDS}")
    late_demands = tuple(manifest["late_demands"].split())
    if late_demands != ("1",):
        raise ValueError("late demands must be exactly take(1)")

    matrix = [
        (DENSE_SCENARIO, variable_count, dense_demands)
        for variable_count in dense_variables
    ]
    matrix.extend((scenario, 3, late_demands) for scenario in LATE_SCENARIOS)
    return matrix


def is_identity_cell(scenario: str, demand: str) -> bool:
    return (scenario == DENSE_SCENARIO and demand == "full") or (
        scenario in LATE_SCENARIOS and demand == "1"
    )


def exact_proposals(
    scenario: str, variable_count: int, demand: str, roots: int
) -> int | None:
    if scenario == DENSE_SCENARIO and demand == "full":
        return roots * variable_count
    if scenario in LATE_SCENARIOS:
        target = int(scenario.removeprefix("late-"))
        return roots + LATE_PROCESSED_ROOTS[target] + target + 1
    return None


def expected_constraint_kind(
    scenario: str, constraint: int, variable_count: int
) -> str:
    if scenario == DENSE_SCENARIO:
        return "root" if constraint == 0 else f"hop-{constraint - 1}"
    if scenario in LATE_SCENARIOS and constraint < variable_count:
        return f"layer-{constraint}"
    raise ValueError(f"no constraint {constraint} in {scenario}/V={variable_count}")


def validate(
    rows: Sequence[Mapping[str, str]], manifest: Mapping[str, str]
) -> dict[str, int]:
    roots = parse_int(manifest["roots"], "manifest roots")
    if roots != 16_384:
        raise ValueError(f"source-credit protocol requires 16384 roots, found {roots}")
    matrix = manifest_matrix(manifest)
    matrix_ordinal = {
        (scenario, variable_count): ordinal
        for ordinal, (scenario, variable_count, _demands) in enumerate(matrix)
    }
    matrix_demands = {
        (scenario, variable_count): demands
        for scenario, variable_count, demands in matrix
    }
    for _scenario, _variable_count, demands in matrix:
        for demand in demands:
            expected_rows(demand, roots)
    repetitions = parse_int(
        manifest["repetitions_per_invocation"], "manifest repetitions"
    )
    if repetitions <= 0:
        raise ValueError("manifest repetitions must be positive")
    if manifest["old_variant"] != "double-geometric":
        raise ValueError("old arm is not labelled double-geometric")
    if manifest["current_variant"] != "inherited-credit":
        raise ValueError("current arm is not labelled inherited-credit")

    observed_run = one((row["run_id"] for row in rows), "run id")
    observed_harness = one((row["harness"] for row in rows), "harness hash")
    observed_lock = one(
        (row["dependency_lock"] for row in rows), "dependency-lock hash"
    )
    if observed_run != manifest["run_id"]:
        raise ValueError("manifest/observation run-id mismatch")
    if observed_harness != manifest["harness_sha256"]:
        raise ValueError("manifest/observation harness mismatch")
    if observed_lock != manifest["dependency_lock_sha256"]:
        raise ValueError("manifest/observation dependency-lock mismatch")

    position_ordinal = {value: index for index, value in enumerate(POSITIONS)}
    groups: dict[tuple[str, int, str, str], list[Mapping[str, str]]] = defaultdict(list)
    record_counts: Counter[str] = Counter()

    for line, row in enumerate(rows, start=2):
        context = f"observation line {line}"
        record = row["record"]
        if record not in {"diagnostic", "identity", "calls", "sample"}:
            raise ValueError(f"{context}: unknown record {record!r}")
        record_counts[record] += 1

        scenario = row["scenario"]
        variable_count = parse_int(row["variables"], f"{context} variables")
        matrix_key = (scenario, variable_count)
        if matrix_key not in matrix_ordinal:
            raise ValueError(f"{context}: unmanifested scenario/V cell {matrix_key}")
        demand = row["demand"]
        if demand not in matrix_demands[matrix_key]:
            raise ValueError(
                f"{context}: unmanifested demand {demand!r} for {matrix_key}"
            )
        position = row["abba_position"]
        if position not in position_ordinal:
            raise ValueError(f"{context}: unexpected ABBA position {position!r}")

        invocation = parse_int(
            row["invocation_sequence"], f"{context} invocation sequence"
        )
        expected_invocation = (
            matrix_ordinal[matrix_key] * len(POSITIONS) + position_ordinal[position]
        )
        if invocation != expected_invocation:
            raise ValueError(
                f"{context}: invocation={invocation}, expected {expected_invocation} "
                f"for {scenario}/V={variable_count}/{position}"
            )

        if position.startswith("A"):
            expected_engine = manifest["old_revision"]
            expected_variant = manifest["old_variant"]
        else:
            expected_engine = manifest["current_revision"]
            expected_variant = manifest["current_variant"]
        if (row["engine"], row["engine_variant"]) != (
            expected_engine,
            expected_variant,
        ):
            raise ValueError(
                f"{context}: arm mismatch, observed "
                f"{(row['engine'], row['engine_variant'])}, expected "
                f"{(expected_engine, expected_variant)}"
            )

        if parse_int(row["roots"], f"{context} roots") != roots:
            raise ValueError(f"{context}: root count differs from manifest")
        wanted_rows = expected_rows(demand, roots)
        if parse_int(row["rows"], f"{context} rows") != wanted_rows:
            raise ValueError(
                f"{context}: demand {demand} returned {row['rows']}, "
                f"expected {wanted_rows}"
            )

        for field in WORK_FIELDS:
            if parse_int(row[field], f"{context} {field}") < 0:
                raise ValueError(f"{context}: negative {field}")
        wanted_proposals = exact_proposals(scenario, variable_count, demand, roots)
        if wanted_proposals is not None:
            proposals = parse_int(row["proposals"], f"{context} proposals")
            if proposals != wanted_proposals:
                raise ValueError(
                    f"{context}: exact proposals={proposals}, "
                    f"expected {wanted_proposals}"
                )
        if scenario == "late-2":
            exact_work = {
                "expansions": 6,
                "variable_groups": 6,
                "widest": 8,
                "proposals": 16_396,
            }
            if row["engine_variant"] == "double-geometric":
                exact_work.update(
                    frontier_rows=13,
                    inplace_descents=2,
                    copied_descents=4,
                )
            else:
                exact_work.update(
                    frontier_rows=19,
                    inplace_descents=1,
                    copied_descents=5,
                )
            for field, expected in exact_work.items():
                observed = parse_int(row[field], f"{context} {field}")
                if observed != expected:
                    raise ValueError(
                        f"{context}: late-2 {field}={observed}, expected {expected}"
                    )
        identity_cell = is_identity_cell(scenario, demand)
        if record == "identity":
            if not identity_cell or row["result_digest"] == "-":
                raise ValueError(f"{context}: invalid diagnostic identity")
        elif row["result_digest"] != "-":
            raise ValueError(
                f"{context}: only diagnostic identity rows may carry a digest"
            )

        if record == "sample":
            repetition = parse_int(row["repetition"], f"{context} repetition")
            if repetition < 0 or repetition >= repetitions:
                raise ValueError(f"{context}: repetition is out of range")
            if parse_int(row["elapsed_ns"], f"{context} elapsed_ns") <= 0:
                raise ValueError(f"{context}: timed sample is not positive")
        else:
            if row["repetition"] != "-":
                raise ValueError(f"{context}: non-sample has a repetition")
            if parse_int(row["elapsed_ns"], f"{context} elapsed_ns") != 0:
                raise ValueError(f"{context}: non-sample has nonzero elapsed time")

        if record == "identity" and not identity_cell:
            raise ValueError(f"{context}: non-identity cell labelled identity")
        if record == "diagnostic" and identity_cell:
            raise ValueError(f"{context}: identity cell labelled diagnostic")
        if record == "calls":
            constraint = parse_int(row["constraint"], f"{context} constraint")
            if constraint < 0 or constraint >= variable_count:
                raise ValueError(f"{context}: constraint index is out of range")
            for field in CALL_FIELDS:
                if parse_int(row[field], f"{context} {field}") < 0:
                    raise ValueError(f"{context}: negative {field}")
        else:
            for field in (
                "constraint",
                "constraint_kind",
                *CALL_FIELDS,
                "propose_widths",
            ):
                if row[field] != "-":
                    raise ValueError(f"{context}: non-call record sets {field}")

        groups[(scenario, variable_count, demand, position)].append(row)

    expected_group_keys = {
        (scenario, variable_count, demand, position)
        for scenario, variable_count, demands in matrix
        for demand in demands
        for position in POSITIONS
    }
    if set(groups) != expected_group_keys:
        missing = sorted(expected_group_keys - set(groups))
        extra = sorted(set(groups) - expected_group_keys)
        raise ValueError(
            f"incomplete benchmark cells: missing={missing}, extra={extra}"
        )

    expected_repetitions = set(range(repetitions))
    for key, group in groups.items():
        scenario, variable_count, _demand, _position = key
        summaries = [
            row for row in group if row["record"] in {"diagnostic", "identity"}
        ]
        samples = [row for row in group if row["record"] == "sample"]
        calls = [row for row in group if row["record"] == "calls"]
        if len(summaries) != 1:
            raise ValueError(f"{key}: expected one diagnostic summary")
        observed_repetitions = {
            parse_int(row["repetition"], f"{key} repetition") for row in samples
        }
        if len(samples) != repetitions or observed_repetitions != expected_repetitions:
            raise ValueError(
                f"{key}: incomplete samples: count={len(samples)}, "
                f"repetitions={sorted(observed_repetitions)}"
            )
        constraints = [
            parse_int(row["constraint"], f"{key} constraint") for row in calls
        ]
        if len(calls) != variable_count or sorted(constraints) != list(
            range(variable_count)
        ):
            raise ValueError(f"{key}: call ledger does not cover every constraint once")
        for call in calls:
            constraint = parse_int(call["constraint"], f"{key} constraint")
            expected_kind = expected_constraint_kind(
                scenario, constraint, variable_count
            )
            if call["constraint_kind"] != expected_kind:
                raise ValueError(
                    f"{key}: constraint {constraint} kind "
                    f"{call['constraint_kind']!r}, expected {expected_kind!r}"
                )
        call_outputs = sum(
            parse_int(row["propose_outputs"], f"{key} propose outputs") for row in calls
        )
        summary_proposals = parse_int(
            summaries[0]["proposals"], f"{key} summary proposals"
        )
        if call_outputs != summary_proposals:
            raise ValueError(
                f"{key}: call-ledger outputs={call_outputs}, "
                f"summary proposals={summary_proposals}"
            )
        if scenario in LATE_SCENARIOS:
            target = int(scenario.removeprefix("late-"))
            by_constraint = {int(row["constraint"]): row for row in calls}
            exact_calls = {
                0: {
                    "propose_calls": 1,
                    "propose_frontier_rows": 1,
                    "propose_outputs": roots,
                    "propose_max_width": 1,
                },
                1: {
                    "propose_calls": LATE_SOURCE_PAGES[target],
                    "propose_frontier_rows": LATE_PROCESSED_ROOTS[target],
                    "propose_outputs": LATE_PROCESSED_ROOTS[target],
                    "propose_max_width": LATE_LAST_WIDTH[target],
                },
            }
            if target == 2:
                exact_calls[2] = {
                    "propose_calls": 3,
                    "propose_frontier_rows": 3,
                    "propose_outputs": 3,
                    "propose_max_width": 1,
                }
            for constraint, fields in exact_calls.items():
                for field, expected in fields.items():
                    observed = parse_int(
                        by_constraint[constraint][field],
                        f"{key} constraint {constraint} {field}",
                    )
                    if observed != expected:
                        raise ValueError(
                            f"{key}: constraint {constraint} {field}={observed}, "
                            f"expected {expected}"
                        )
            terminal = by_constraint[2]
            for field in ("propose_frontier_rows", "propose_outputs"):
                observed = parse_int(terminal[field], f"{key} terminal {field}")
                if observed != target + 1:
                    raise ValueError(
                        f"{key}: terminal {field}={observed}, expected {target + 1}"
                    )

    for scenario, variable_count, demands in matrix:
        identity_demand = next(
            demand for demand in demands if is_identity_cell(scenario, demand)
        )
        identities = [
            row
            for row in rows
            if row["scenario"] == scenario
            and parse_int(row["variables"], "identity variables") == variable_count
            and row["demand"] == identity_demand
            and row["record"] == "identity"
        ]
        digests = {row["result_digest"] for row in identities}
        if len(digests) != 1:
            raise ValueError(
                f"{scenario}/V={variable_count}: diagnostic identity differs across arms: "
                f"{sorted(digests)}"
            )
        if len(identities) != len(POSITIONS):
            raise ValueError(
                f"{scenario}/V={variable_count}: expected four ABBA identity records, "
                f"found {len(identities)}"
            )

    return dict(record_counts)


def reduce_timings(
    rows: Sequence[Mapping[str, str]],
    manifest: Mapping[str, str],
) -> list[dict[str, object]]:
    samples: dict[tuple[str, int, str, str], list[int]] = defaultdict(list)
    observed_rows: dict[tuple[str, int, str, str], set[int]] = defaultdict(set)
    for row in rows:
        if row["record"] != "sample":
            continue
        key = (
            row["scenario"],
            int(row["variables"]),
            row["demand"],
            row["abba_position"],
        )
        samples[key].append(int(row["elapsed_ns"]))
        observed_rows[key].add(int(row["rows"]))

    reduced: list[dict[str, object]] = []
    for scenario, variable_count, demands in manifest_matrix(manifest):
        for demand in demands:
            medians = {
                position: statistics.median(
                    samples[(scenario, variable_count, demand, position)]
                )
                for position in POSITIONS
            }
            row_counts = set().union(
                *(
                    observed_rows[(scenario, variable_count, demand, position)]
                    for position in POSITIONS
                )
            )
            if len(row_counts) != 1:
                raise ValueError(
                    f"{scenario}/V={variable_count}/{demand}: "
                    "row count differs across arms"
                )
            double_ns = geometric_mean(medians[position] for position in ("A1", "A2"))
            inherited_ns = geometric_mean(
                medians[position] for position in ("B1", "B2")
            )
            reduced.append(
                {
                    "scenario": scenario,
                    "variables": variable_count,
                    "demand": demand,
                    "rows": next(iter(row_counts)),
                    "double_a1_median_ns": medians["A1"],
                    "double_a2_median_ns": medians["A2"],
                    "double_ns": double_ns,
                    "double_ms": double_ns / 1_000_000,
                    "inherited_b1_median_ns": medians["B1"],
                    "inherited_b2_median_ns": medians["B2"],
                    "inherited_ns": inherited_ns,
                    "inherited_ms": inherited_ns / 1_000_000,
                    "inherited_over_double": inherited_ns / double_ns,
                }
            )
    return reduced


def summarize_numbers(prefix: str, values: Sequence[int]) -> dict[str, object]:
    return {
        f"{prefix}_median": statistics.median(values),
        f"{prefix}_min": min(values),
        f"{prefix}_max": max(values),
    }


def detail_value(values: Sequence[int]) -> str:
    median = statistics.median(values)
    rendered = format_value(median)
    if min(values) == max(values):
        return str(rendered)
    return f"{rendered}[{min(values)},{max(values)}]"


def reduce_identity_work(
    rows: Sequence[Mapping[str, str]],
    manifest: Mapping[str, str],
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, int, str, str, str], list[Mapping[str, str]]] = (
        defaultdict(list)
    )
    for row in rows:
        if is_identity_cell(row["scenario"], row["demand"]):
            grouped[
                (
                    row["scenario"],
                    int(row["variables"]),
                    row["demand"],
                    row["abba_position"],
                    row["record"],
                )
            ].append(row)

    output: list[dict[str, object]] = []
    for scenario, variable_count, demands in manifest_matrix(manifest):
        demand = next(value for value in demands if is_identity_cell(scenario, value))
        for arm in ("double-geometric", "inherited-credit"):
            positions = ARM_POSITIONS[arm]
            summaries = [
                grouped[(scenario, variable_count, demand, position, "identity")][0]
                for position in positions
            ]
            calls_by_position = {
                position: sorted(
                    grouped[(scenario, variable_count, demand, position, "calls")],
                    key=lambda row: int(row["constraint"]),
                )
                for position in positions
            }
            engine = (
                manifest["old_revision"]
                if arm == "double-geometric"
                else manifest["current_revision"]
            )
            variant = (
                manifest["old_variant"]
                if arm == "double-geometric"
                else manifest["current_variant"]
            )
            item: dict[str, object] = {
                "scenario": scenario,
                "variables": variable_count,
                "demand": demand,
                "arm": arm,
                "engine": engine,
                "engine_variant": variant,
                "invocations": len(positions),
            }
            for field in WORK_FIELDS:
                output_field = (
                    "copying_descent_events" if field == "copied_descents" else field
                )
                item.update(
                    summarize_numbers(
                        output_field,
                        [int(summary[field]) for summary in summaries],
                    )
                )
            for field in ("propose_calls", "propose_frontier_rows", "propose_outputs"):
                values = [
                    sum(int(row[field]) for row in calls_by_position[position])
                    for position in positions
                ]
                item.update(summarize_numbers(f"{field}_sum", values))

            calls_detail = []
            width_detail = []
            for constraint in range(variable_count):
                kinds = {
                    calls_by_position[position][constraint]["constraint_kind"]
                    for position in positions
                }
                kind = one(
                    kinds,
                    f"{scenario}/V={variable_count}/{arm} constraint kind",
                )
                calls_detail.append(
                    f"{constraint}:{kind}="
                    + detail_value(
                        [
                            int(
                                calls_by_position[position][constraint]["propose_calls"]
                            )
                            for position in positions
                        ]
                    )
                )
                width_detail.append(
                    f"{constraint}:{kind}="
                    + detail_value(
                        [
                            int(
                                calls_by_position[position][constraint][
                                    "propose_max_width"
                                ]
                            )
                            for position in positions
                        ]
                    )
                )
            item["propose_calls_by_constraint"] = ";".join(calls_detail)
            item["propose_max_width_by_constraint"] = ";".join(width_detail)
            output.append(item)
    return output


def repeat_spread(
    rows: Sequence[Mapping[str, str]],
) -> list[dict[str, object]]:
    samples: dict[tuple[str, int, str, str], list[int]] = defaultdict(list)
    for row in rows:
        if row["record"] == "sample":
            samples[
                (
                    row["scenario"],
                    int(row["variables"]),
                    row["demand"],
                    row["abba_position"],
                )
            ].append(int(row["elapsed_ns"]))

    spread_groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    for (_scenario, _variables, _demand, position), values in samples.items():
        median = statistics.median(values)
        spread = statistics.median(abs(value - median) for value in values) / median
        arm = POSITION_ARM[position]
        spread_groups[(arm, "all")].append(spread)
        spread_groups[("all", "all")].append(spread)
        if median >= 1_000_000:
            spread_groups[(arm, ">=1 ms")].append(spread)
            spread_groups[("all", ">=1 ms")].append(spread)

    output = []
    arm_order = {"double-geometric": 0, "inherited-credit": 1, "all": 2}
    scope_order = {"all": 0, ">=1 ms": 1}
    for (arm, scope), values in sorted(
        spread_groups.items(),
        key=lambda item: (arm_order[item[0][0]], scope_order[item[0][1]]),
    ):
        output.append(
            {
                "arm": arm,
                "scope": scope,
                "cells": len(values),
                "mad_over_median": statistics.median(values),
                "p90": quantile(values, 0.90),
                "p99": quantile(values, 0.99),
            }
        )
    return output


def format_value(value: object) -> object:
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.9g}"
    return value


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


def markdown_table(
    headers: Sequence[str], rows: Iterable[Sequence[object]]
) -> list[str]:
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    output.extend("| " + " | ".join(str(value) for value in row) + " |" for row in rows)
    return output


def format_ms(value: object) -> str:
    milliseconds = float(value)
    if milliseconds >= 100:
        return f"{milliseconds:.2f}"
    if milliseconds >= 1:
        return f"{milliseconds:.3f}"
    return f"{milliseconds:.6f}"


def counter_range(row: Mapping[str, object], field: str) -> str:
    median = format_value(row[f"{field}_median"])
    low = row[f"{field}_min"]
    high = row[f"{field}_max"]
    if low == high:
        return str(median)
    return f"{median} [{low}, {high}]"


def render_summary(
    manifest: Mapping[str, str],
    rows: Sequence[Mapping[str, str]],
    record_counts: Mapping[str, int],
    cells: Sequence[Mapping[str, object]],
    work: Sequence[Mapping[str, object]],
    spreads: Sequence[Mapping[str, object]],
) -> str:
    matrix_count = len(manifest_matrix(manifest))
    lines = [
        "# Query-engine inherited source credit",
        "",
        f"Run `{manifest['run_id']}` compares double geometric widening at "
        f"`{manifest['old_revision'][:8]}` with inherited source-page credit at "
        f"`{manifest['current_revision'][:8]}`.",
        "",
        "## Integrity",
        "",
        f"- {len(rows):,} observations: "
        + ", ".join(
            f"{count:,} {record}" for record, count in sorted(record_counts.items())
        )
        + ".",
        f"- One run id, harness (`{manifest['harness_sha256'][:12]}…`), and dependency "
        f"lock (`{manifest['dependency_lock_sha256'][:12]}…`) occur throughout the artifact.",
        "- The copied harness, runner, analyzer, dependency lock, and both immutable "
        "binaries match their manifest SHA-256 hashes.",
        f"- Every one of the {matrix_count} scenario/variable fixtures has four ordered "
        "A/B/B/A invocations, complete repetitions, exact row counts, and one exact "
        "diagnostic identity digest across both arms.",
        "- Every dense full drain proposes exactly `N × V`; every late-first witness "
        "matches its boundary-derived proposal count; each diagnostic call ledger sums "
        "back to the engine proposal counter.",
        "",
        "## Method",
        "",
        "Each invocation is reduced to its repetition median. The A1/A2 temporal "
        "bookends and B1/B2 middle repeats are then combined separately by geometric mean. "
        "`inherited/double < 1` means inherited credit is faster.",
        "Timed samples carry no identity digest: identity is established only by the "
        "untimed exact diagnostic in each scenario/variable fixture.",
        "",
        "## Demand curve",
        "",
    ]
    lines += markdown_table(
        (
            "scenario",
            "V",
            "demand",
            "rows",
            "double ms",
            "inherited ms",
            "inherited/double",
        ),
        (
            (
                row["scenario"],
                row["variables"],
                row["demand"],
                row["rows"],
                format_ms(row["double_ms"]),
                format_ms(row["inherited_ms"]),
                f"{float(row['inherited_over_double']):.3f}",
            )
            for row in cells
        ),
    )

    lines += ["", "## Exact diagnostic work", ""]
    lines += markdown_table(
        (
            "scenario",
            "V",
            "demand",
            "arm",
            "proposals",
            "expansions",
            "propose calls",
            "copying events / in-place events",
            "widest",
        ),
        (
            (
                row["scenario"],
                row["variables"],
                row["demand"],
                row["arm"],
                counter_range(row, "proposals"),
                counter_range(row, "expansions"),
                counter_range(row, "propose_calls_sum"),
                f"{counter_range(row, 'copying_descent_events')} / "
                f"{counter_range(row, 'inplace_descents')}",
                counter_range(row, "widest"),
            )
            for row in work
        ),
    )

    lines += [
        "",
        "`copying descent events` counts engine descent operations which used copying; "
        "it is not a row or byte count.",
        "",
        "## Exact diagnostic constraint geometry",
        "",
    ]
    lines += markdown_table(
        (
            "scenario",
            "V",
            "arm",
            "calls by constraint",
            "maximum width by constraint",
        ),
        (
            (
                row["scenario"],
                row["variables"],
                row["arm"],
                f"`{row['propose_calls_by_constraint']}`",
                f"`{row['propose_max_width_by_constraint']}`",
            )
            for row in work
        ),
    )

    lines += ["", "## Repeat spread", ""]
    lines += markdown_table(
        ("arm", "scope", "cells", "median MAD/median", "p90", "p99"),
        (
            (
                row["arm"],
                row["scope"],
                row["cells"],
                f"{float(row['mad_over_median']) * 100:.1f}%",
                f"{float(row['p90']) * 100:.1f}%",
                f"{float(row['p99']) * 100:.1f}%",
            )
            for row in spreads
        ),
    )
    lines += [
        "",
        "`analysis/cells.tsv` contains the complete ABBA timing reduction. "
        "`analysis/work.tsv` contains exact-diagnostic aggregate counters and "
        "per-constraint call geometry.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="validate and reduce a source-credit benchmark artifact"
    )
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()

    result_dir = args.result_dir.resolve()
    manifest = read_manifest(result_dir / "manifest.tsv")
    rows = read_observations(result_dir / "observations.tsv")
    validate_artifact(result_dir, manifest)
    record_counts = validate(rows, manifest)
    cells = reduce_timings(rows, manifest)
    work = reduce_identity_work(rows, manifest)
    spreads = repeat_spread(rows)

    analysis_dir = result_dir / "analysis"
    analysis_dir.mkdir(exist_ok=True)
    cell_fields = (
        "scenario",
        "variables",
        "demand",
        "rows",
        "double_a1_median_ns",
        "double_a2_median_ns",
        "double_ns",
        "double_ms",
        "inherited_b1_median_ns",
        "inherited_b2_median_ns",
        "inherited_ns",
        "inherited_ms",
        "inherited_over_double",
    )
    work_fields = (
        "scenario",
        "variables",
        "demand",
        "arm",
        "engine",
        "engine_variant",
        "invocations",
        *(
            f"{field}_{suffix}"
            for field in (
                "expansions",
                "frontier_rows",
                "variable_groups",
                "proposals",
                "widest",
                "inplace_descents",
                "copying_descent_events",
            )
            for suffix in ("median", "min", "max")
        ),
        *(
            f"{field}_sum_{suffix}"
            for field in ("propose_calls", "propose_frontier_rows", "propose_outputs")
            for suffix in ("median", "min", "max")
        ),
        "propose_calls_by_constraint",
        "propose_max_width_by_constraint",
    )
    write_tsv(analysis_dir / "cells.tsv", cells, cell_fields)
    write_tsv(analysis_dir / "work.tsv", work, work_fields)
    summary = render_summary(manifest, rows, record_counts, cells, work, spreads)
    (analysis_dir / "summary.md").write_text(summary)
    print(summary, end="")


if __name__ == "__main__":
    main()
