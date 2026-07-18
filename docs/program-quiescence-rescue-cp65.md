# Program cp65 quiescence/dead-page probe

This branch-local diagnostic accompanies the probe-only counters introduced by
`85ff8f01` and strengthened by `4ffb6ce3`. It records both the pre-run static
hypothesis and the decisive stats-only counter receipt. No production
accounting change is proposed.

## Frozen prefix algebra

For `formula + cyclic RPQ / TribleSet sibling`, B and Program with both proven
compatibility flags have identical physical RPQ work at checkpoint 65:

| checkpoint / engine | width | transition dead | transition negative | completed activations | nonterminal calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 / B | 512 | 3 | 0 | 3 | 28 |
| 64 / BOTH | 512 | 3 | 0 | 4 | 29 |
| 65 / B | 512 | 9 | 0 | 5 | 45 |
| 65 / BOTH | 1024 | 10 | 1 | 7 | 47 |

At checkpoint 65 both engines report exactly 195 transition pages, 34
transition cohorts, 780 examined transition candidates, and maximum cohort 8.
Their checkpoint-64 values are likewise identical. Therefore the 64-to-65
increments are:

| increment | B | BOTH | differential |
| --- | ---: | ---: | ---: |
| transition pages | +130 | +130 | 0 |
| transition cohorts | +17 | +17 | 0 |
| transition examined | +520 | +520 | 0 |
| transition dead | +6 | +7 | +1 |
| transition negative steps | 0 | +1 | +1 |
| completed activations | +2 | +3 | +1 |
| nonterminal calls | +17 | +18 | +1 |

Already at checkpoint 64, BOTH has exactly one extra completed activation and
one extra nonterminal call with no source/transition telemetry difference. The
64-to-65 differential adds exactly one more telemetry-free Program call and
completion, and exactly one nominal transition-dead/negative receipt.

## Pre-run hypothesis

The leading hypothesis was that Program's generic local predicate

```text
page_dead = !page_had_program_effect && !task_effects.has_effect()
```

mislabelled an effectless, zero-telemetry finite CandidateFilter input as a
dead transition page. The preregistered exact signature predicted one empty
Candidates completion and one zero-telemetry finite local-dead input at the
64-to-65 boundary. Two alternatives were retained: a receipt-local dead Search
page, or a lost/delayed stable completion. The counter falsified all three.

## Decisive counter receipt

The stats-only prefix ran at revision `4ffb6ce3f315792fe18746c64267d76e50339f6d`
with both compatibility flags and `engine_program_effect_probe`. Oracle parity
remained exact. For the mixed TribleSet cell:

| Program counter | checkpoint 64 | checkpoint 65 | increment |
| --- | ---: | ---: | ---: |
| all inputs | 66 | 197 | +131 |
| transition pages / cohorts / examined | 65 / 17 / 260 | 195 / 34 / 780 | +130 / +17 / +520 |
| zero-telemetry finite inputs | 1 | 2 | +1 |
| finite raw / stable / quiescent | 1 / 1 / 1 | 2 / 2 / 2 | +1 / +1 / +1 |
| finite examined / assigned limit | 1 / 512 | 513 / 1024 | +512 / +512 |
| finite saturated / resumed | 0 / 0 | 1 / 0 | +1 / 0 |
| finite local-dead | 0 | 0 | 0 |
| receipt-local dead Search pages | 0 | 0 | 0 |
| all raw-effect inputs | 62 | 184 | +122 |
| all stable-effect / quiescent inputs | 2 / 2 | 5 / 5 | +3 / +3 |
| nonempty / empty Candidates completions | 2 / 0 | 5 / 0 | +3 / 0 |
| local-dead inputs / cohorts | 3 / 1 | 10 / 3 | +7 / +2 |
| stable-effect / quiescent cohorts | 2 / 2 | 4 / 4 | +2 / +2 |

The additional finite input is therefore a saturated 512-candidate filter
that produces a raw effect, a stable effect, quiescence, and a nonempty result.
It is productive and never locally dead. There are no dead Search receipts,
and every new completion is nonempty. This directly falsifies the finite
misattribution, structured-join, and lost-completion accounts.

## Exact causal interpretation

The remaining arithmetic is exhaustive for this query:

1. The +131 Program inputs are exactly +130 telemetry-bearing transition pages
   plus the one productive finite input.
2. Subtracting that finite raw effect leaves 121 of the 130 transition inputs
   with an admitted child or endpoint. Nine transition inputs have no raw
   Program effect.
3. Subtracting the finite stable effect and quiescence leaves two stable
   transition inputs and two transition quiescences. Therefore two of the nine
   raw-empty transition pages are rescued at input level and exactly seven are
   locally dead, matching the observed +7 transition-dead pages.
4. Those seven dead inputs occupy two transition cohorts. The two new stable
   transition inputs occupy only one new stable transition cohort, and the two
   transition quiescences likewise occupy one cohort. Since outer feedback is
   suppressed when the aggregate physical step progresses, one dead cohort is
   rescued globally and the other produces exactly one transition-negative
   step and the 512-to-1024 width promotion.

Legacy B executes the identical +130 pages, +17 cohorts, and +520 examined
transitions, but reports six dead pages and no negative step. With identical
semantic output and physical totals, the remaining freedom is how novelty and
stable completions are distributed across individual pages and physical
cohorts. Task ordering and compatible activation batching can concentrate the
same useful effects into fewer inputs/cohorts. B's zero negative count proves
that each of its steps containing a dead transition page also progressed;
Program BOTH has one real dead transition cohort without a stable handoff.

Thus dead-page and negative-step counts are not invariants of semantic output
or even of total physical RPQ work. They are measurements of the chosen
physical cohortization. The extra negative is correctly classified under the
current Program execution and feedback law; it is not a fabricated transition,
a missing completion, or an RPQ correctness defect.

## Recommendation

No production code change is warranted for this residual. In particular:

- do not special-case zero-telemetry finite work; the suspected finite input is
  productive and unrelated to the negative;
- do not copy B's dead count into Program accounting; that would make feedback
  emulate another scheduler's effect packing rather than describe this one;
- do not change global widening from this receipt alone. The promotion follows
  the current law from a real, non-progressing transition cohort, and no timing
  evidence here establishes a worse policy outcome.

Close the cp65 discrepancy as explained scheduler-sensitive feedback. Future
feedback work should first choose a scheduler-independent logical unit (for
example activation- or receipt-lineage-local evidence), preregister its law,
and benchmark it as a separate scheduling-policy change. The probe-only
instrumentation need not enter production.
