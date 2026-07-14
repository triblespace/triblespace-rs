//! Shared topology-scaled planner for coalescing exact row-local variable choices.

use super::{estimate_magnitude, VariableId, VariableSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct AgglomerativePlan {
    pub(super) preferred_groups: usize,
    pub(super) scheduled_groups: usize,
    pub(super) preferred_estimate_sum: u128,
    pub(super) scheduled_estimate_sum: u128,
    pub(super) row_inflation: usize,
}

#[inline]
fn estimate_inflation(estimate: usize, baseline: usize) -> usize {
    if baseline == 0 {
        return if estimate == 0 { 1 } else { usize::MAX };
    }
    let estimate = estimate as u128;
    let baseline = baseline as u128;
    let rounded_up = estimate.div_ceil(baseline);
    usize::try_from(rounded_up).unwrap_or(usize::MAX).max(1)
}

/// Builds a deterministic minimum-regret merge hierarchy over the exact
/// per-row preferred-variable groups and returns its coarsest compatible level.
///
/// A whole source group may be absorbed by target `v` only when every row's
/// estimate-magnitude regret fits `v`'s influence neighborhood. The allowance
/// is the bit length of `{v} ∪ (influence(v) ∩ unbound)`: the same logarithmic
/// cardinality resolution used by variable ordering, scaled by how many still
/// relevant downstream estimates binding `v` can change. Zero-estimate rows
/// remain compatible only with zero work. This replaces a workload-specific
/// fixed ratio with query topology.
///
/// Among admissible absorptions each hierarchy edge minimizes total estimated
/// candidate work; every successful edge removes one proposal group. Exact
/// grouping is the first level and remains the result when no groups are
/// compatible.
#[allow(clippy::too_many_arguments)]
pub(super) fn plan_agglomerative_partition(
    est: &[usize],
    rows: usize,
    unbound: &[VariableId],
    influences: &[VariableSet; 128],
    preferred: &[u32],
    preferred_counts: &[usize],
    owners: &mut Vec<u32>,
    best_assignment: &mut Vec<u32>,
    group_sums: &mut Vec<u128>,
    compatible: &mut Vec<bool>,
    active: &mut Vec<bool>,
) -> AgglomerativePlan {
    let variables = unbound.len();
    debug_assert_eq!(est.len(), variables * rows);
    debug_assert_eq!(preferred.len(), rows);
    debug_assert_eq!(preferred_counts.len(), variables);

    let preferred_groups = preferred_counts.iter().filter(|&&count| count > 0).count();
    debug_assert!(preferred_groups > 1);

    // The matrices hold sufficient statistics for evaluating every row
    // currently owned by `source` on `target`.
    group_sums.clear();
    group_sums.resize(variables * variables, 0);
    compatible.clear();
    compatible.resize(variables * variables, true);
    let mut unbound_set = VariableSet::new_empty();
    for &variable in unbound {
        unbound_set.set(variable);
    }
    let mut magnitude_allowances = [0u64; 128];
    for target in 0..variables {
        let variable = unbound[target];
        let neighborhood = influences[variable]
            .intersect(unbound_set)
            .union(VariableSet::new_singleton(variable));
        magnitude_allowances[target] = estimate_magnitude(neighborhood.count());
    }
    for (row, &source) in preferred.iter().enumerate() {
        let source = source as usize;
        let baseline = est[source * rows + row];
        let baseline_magnitude = estimate_magnitude(baseline);
        for target in 0..variables {
            let estimate = est[target * rows + row];
            let slot = source * variables + target;
            group_sums[slot] += estimate as u128;
            compatible[slot] &= if baseline == 0 {
                estimate == 0
            } else {
                estimate_magnitude(estimate).saturating_sub(baseline_magnitude)
                    <= magnitude_allowances[target]
            };
        }
    }

    active.clear();
    active.extend(preferred_counts.iter().map(|&count| count > 0));
    owners.clear();
    owners.extend((0..variables).map(|variable| variable as u32));

    let preferred_estimate_sum: u128 = (0..variables)
        .filter(|&source| active[source])
        .map(|source| group_sums[source * variables + source])
        .sum();
    let mut work = preferred_estimate_sum;
    let mut groups = preferred_groups;
    best_assignment.clear();
    best_assignment.extend_from_slice(owners);
    let mut best = AgglomerativePlan {
        preferred_groups,
        scheduled_groups: preferred_groups,
        preferred_estimate_sum,
        scheduled_estimate_sum: preferred_estimate_sum,
        row_inflation: 1,
    };
    while groups > 1 {
        // Every candidate level has the same `groups - 1`; the least-work
        // directed absorption is therefore the least-regret edge in this
        // hierarchy. Work ties use stable variable ids.
        let mut merge: Option<(u128, VariableId, VariableId, usize, usize)> = None;
        for source in 0..variables {
            if !active[source] {
                continue;
            }
            let source_self_slot = source * variables + source;
            for target in 0..variables {
                if source == target || !active[target] {
                    continue;
                }
                let source_target_slot = source * variables + target;
                if !compatible[source_target_slot] {
                    continue;
                }
                let new_work = work - group_sums[source_self_slot] + group_sums[source_target_slot];
                let candidate = (new_work, unbound[target], unbound[source], source, target);
                if merge.as_ref().is_none_or(|current| candidate < *current) {
                    merge = Some(candidate);
                }
            }
        }
        let Some((new_work, _, _, source, target)) = merge else {
            break;
        };

        for variable in 0..variables {
            let target_slot = target * variables + variable;
            let source_slot = source * variables + variable;
            group_sums[target_slot] += group_sums[source_slot];
            compatible[target_slot] &= compatible[source_slot];
        }
        active[source] = false;
        for owner in owners.iter_mut() {
            if *owner as usize == source {
                *owner = target as u32;
            }
        }
        work = new_work;
        groups -= 1;
        best.scheduled_groups = groups;
        best.scheduled_estimate_sum = work;
        best_assignment.clear();
        best_assignment.extend_from_slice(owners);
    }

    // Expand the retained group-owner map to one scheduled variable per row.
    owners.clear();
    owners.reserve(rows);
    owners.extend(
        preferred
            .iter()
            .map(|&source| best_assignment[source as usize]),
    );
    std::mem::swap(owners, best_assignment);

    best.row_inflation = preferred
        .iter()
        .zip(best_assignment.iter())
        .enumerate()
        .map(|(row, (&source, &target))| {
            estimate_inflation(
                est[target as usize * rows + row],
                est[source as usize * rows + row],
            )
        })
        .max()
        .unwrap_or(1);
    best
}
