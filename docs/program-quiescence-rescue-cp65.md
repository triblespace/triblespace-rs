# Program cp65 quiescence/dead-page probe

This branch-local diagnostic accompanies the probe-only counters introduced by
`85ff8f01`. It records the static result before the final counter receipt. No
production accounting change is proposed here.

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

## Static localization

Legacy `step_transitions` can increment `transition_dead_pages` only for an
actual paged RPQ transition. Program `step_program` instead applies its local
predicate to every typed input:

```text
page_dead = !page_had_program_effect && !task_effects.has_effect()
```

When a whole Program cohort reports neither source nor transition telemetry,
`source_telemetry_cohort` is false, so an effectless finite input falls through
to `transition_dead_pages`. This is sufficient to manufacture the exact
counter signature without any additional RPQ page.

Receipt-local `dead_search_pages` cannot explain the observed transition
negative. Those receipts contribute to generic `dead_pages`; source telemetry
may additionally contribute to source-dead accounting, but the search-receipt
path never increments `transition_dead_pages`. This statically falsifies the
structured-join barrier hypothesis for the unmatched negative.

The RPQ Program has only two zero-telemetry finite step families. Optimistic
partial `Support` always emits a raw `supported` effect and therefore cannot be
the dead input. `CandidateFilter` may examine a page, admit no candidate, have
no resume, quiesce with an empty confirmation result, and then satisfy the
generic `page_dead` predicate. Since this query's only typed family is the RPQ,
the exhaustive state split localizes the second telemetry-free call to that
candidate-filter shape, subject to the final probe counters.

The lost/delayed-completion hypothesis would require a real transition page to
lose a stable completion even though physical page/cohort/examined totals are
identical and the complete differential is already accounted for by one extra
telemetry-free call and activation. It remains a formal falsifier only until
the probe reports the completion kind; a zero-telemetry finite local-dead input
with `Candidates([])` and no dead-search receipt closes it.

## Provisional classification

The residual is a physical-feedback attribution mismatch, not a semantic RPQ
or quiescence correctness failure. The generic dead-page disposition and its
transition label must be kept separate: `account_delta_feedback` grows global
width from `dead_pages`, while `source_dead_pages` and
`transition_dead_pages` only select diagnostic negative counters. A finite
candidate filter that actually examines a page and admits nothing can be a
valid dead structural page even though calling it a transition page is false.
Consequently, merely repairing the attribution would not remove the observed
512-to-1024 promotion, and the static evidence does not justify changing that
geometric scheduling law. Production should eventually distinguish finite
structural Program work from source/transition search telemetry; whether to do
that now or let the planned Program-effects refactor erase the ambiguous label
is deliberately left to the post-lane counter receipt.
