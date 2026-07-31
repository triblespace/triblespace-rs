#!/usr/bin/env python3
"""Reduce one validated old/current allocation-census pair to absolute deltas."""

import csv
import pathlib
import sys


def read(path):
    with pathlib.Path(path).open(newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    keyed = {(row["shape"], row["representation"], row["phase"]): row for row in rows}
    if len(rows) != 16 or len(keyed) != 16:
        raise SystemExit(f"{path}: expected 16 unique cells, got {len(rows)}/{len(keyed)}")
    return keyed


if len(sys.argv) != 3:
    raise SystemExit(f"usage: {sys.argv[0]} OLD.tsv CURRENT.tsv")

old = read(sys.argv[1])
current = read(sys.argv[2])
if old.keys() != current.keys():
    raise SystemExit("old/current cell sets differ")

for key in old:
    for field in ("protocol", "harness_sha256", "lock_sha256", "rows", "exact_mask"):
        if old[key][field] != current[key][field]:
            raise SystemExit(f"{key}: semantic/provenance field {field} differs")

fields = ("alloc_ops", "allocated_bytes", "dealloc_ops", "deallocated_bytes", "net_bytes")
writer = csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")
writer.writerow(
    ["shape", "representation", "phase", "rows"]
    + [item for field in fields for item in (f"old_{field}", f"current_{field}", f"delta_{field}")]
)
for key in sorted(old):
    left, right = old[key], current[key]
    output = [*key, left["rows"]]
    for field in fields:
        old_value = int(left[field])
        current_value = int(right[field])
        output += [old_value, current_value, current_value - old_value]
    writer.writerow(output)
