# Philosophy

Triblespace was designed to feel approachable without sacrificing rigor. This
chapter collects the guiding values that shape everything from the storage
format to the public APIs. Treat it as a companion to the rest of the deep-dive
sections: when you wonder "why does it work this way?", the answer usually
traces back to one of these principles.

## Clarity before cleverness

We favour predictable, mechanically simple components over opaque heuristics.
Each subsystem should be understandable on its own, with behaviours that are
obvious when composed with the rest of the stack. When a trade-off appears
between a clever optimisation and debuggability, we err on the side of the
latter and document the costs so future work can revisit the decision with
better evidence.

## Productive developer experience

APIs should read like regular Rust. Where backends demand asynchronous
capabilities—such as object-store repositories—we wrap them in blocking entry
points so typical workflows stay synchronous while still supporting advanced
integrations. Well-documented patterns and composable macros let readers
experiment in a REPL or test harness without extra scaffolding, and examples in
the book mirror the crates users import so copy-and-paste snippets behave as
advertised.

## Soundness and data integrity

The engine must reject malformed data early, surface explicit error paths, and
make invariants easy to audit. Safety checks live close to the data structures
that rely on them, and proofs or tests accompany subtle invariants when
feasible. Correctness remains the baseline for every optimisation.

## Performance with headroom

Efficient data structures keep the core fast, but we prioritise predictable
latency over micro-benchmarks that complicate maintenance. Hot paths receive
focused tuning backed by benchmarks so we can understand the impact of each
change.

## Practical implications

These principles surface in day-to-day workflows:

- Documentation favours runnable snippets and end-to-end walkthroughs, lowering
  the barrier to experimentation.
- Internal abstractions expose minimal, intentional APIs, reducing the amount of
  context a contributor needs before making a change.
- Tooling—such as the `preflight.sh` convenience script, targeted Kani
  verification harnesses, and runnable doc tests—keeps quality checks
  accessible so they are run regularly rather than only in CI.

Taken together, the philosophy is simple: build a reliable system whose pieces
are easy to reason about, teach, and extend.
