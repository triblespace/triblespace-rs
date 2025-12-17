# Introduction

Welcome to the **Tribles Book**. This first chapter is your map to Trible
Space—the problems it tackles, the core ideas behind the `triblespace` crate, and
the kinds of projects it was built to support. By the end of these opening
pages you should be able to recognize when Trible Space is the right tool,
whether you are prototyping something new, extending an existing system, or
adapting it for your own research purposes.

## Design philosophy

Trible Space favors predictable, easy-to-audit building blocks over clever
abstractions. Four principles guide every subsystem:

* **Clarity before cleverness.** Prefer mechanically simple components whose
  behaviour is easy to reason about even when composed with the rest of the
  stack.
* **Productive developer experience.** APIs should read like regular Rust and
  stay synchronous by default so experiments run naturally in tests and REPLs.
* **Soundness and data integrity.** Validation and safety checks live close to
  the data they protect so invariants are explicit and auditable.
* **Performance with headroom.** Tune the hot paths, but keep latency
  predictable and avoid optimisations that would obscure correctness.

You will see these themes echoed throughout the architecture and query engine
chapters whenever a design trade-off appears.

## Why Trible Space exists

Trible Space exists because teams need to steward complex, diverse, and
interconnected datasets without losing context. Research groups, startups, and
digital libraries must pair large binary payloads with fine-grained facts,
synchronize findings across laptops, servers, and mobile devices, and prove that
derived results trace back to their inputs. Combining a conventional database,
object store, and version-control workflow rarely delivers that outcome: blobs
drift away from their provenance, merges become brittle, and auditing which
observations justified a decision or model is tedious.

Trible Space closes those gaps with a single substrate that stores heavyweight
assets and the relationships that explain them. Think of it as a library catalog
for blobs and a lab notebook for the annotations, measurements, and discussions
that give those blobs meaning. Because facts and payloads travel together,
features like version control, verifiability, and provenance fall naturally out
of the data model instead of bolting on as afterthoughts.

That same structure also lets you support offline edits, reconcile concurrent
changes safely, and ship datasets to partners with the evidence needed to trust
them.

To deliver those outcomes, Trible Space blends ideas from databases, version
control, and content-addressed storage. Information is encoded as fixed-width
*tribles*: 64-byte entity–attribute–value facts. Each trible stores two 16-byte
extrinsic identifiers plus a 32-byte typed value. When a value exceeds the
inline slot it becomes a schema-qualified hash pointing to an immutable
content-addressed blob. The blob holds the heavyweight payload, while the trible
remains a compact fact that fits neatly into indexes, caches, and query engines.
Because both tribles and blobs are immutable, you can keep them in memory, on
disk, or in remote object stores without transformation. Content hashes serve as
identifiers, so every payload has a stable address and integrity is easy to
verify when data is shared or synchronized.

This design unlocks capabilities that are difficult to achieve together in
traditional stacks:

* **Trustworthy collaboration** – hashes and immutable histories provide the
  audit trail needed to review changes, merge branches, and reproduce results
  across teams.
* **Content-addressed storage** – values are stored and looked up by their
  contents, making caches and replicas safe even when they live on untrusted
  infrastructure.
* **Flexible querying** – the query engine blends indexes on the fly, letting a
  single query range across trible sets, succinct indexes, and familiar Rust
  collections such as hash maps in one pass.

Taken together, these traits make it feasible to build systems with rich
histories, reproducible computations, and verifiable data exchange while keeping
the developer experience approachable.

## Who this book is for

If you are new to Tribles, the opening chapters build vocabulary and provide a
guided tour of the core data structures. Developers who already understand the
problem space can skim ahead to detailed sections on schema design, query
semantics, and the Atreides join algorithm. The book also points to the API
documentation whenever you are ready to explore the crate directly.

## How to read this book

The book is organized so you can either read it front-to-back or jump straight
to the material that answers your questions. Each chapter layers new ideas onto
the previous ones:

1. **Getting Started** walks through installing the tooling, creating your first
   trible store, and issuing simple queries.
2. **Architecture** and **Query Engine** explain how the runtime is structured
   so you can reason about performance and extensibility.
3. Later sections explore schema design, incremental queries, repository
   workflows, and formal verification so you can grow from experiments to
   production systems.

Inline links lead to deeper resources and code samples. The
[Glossary](glossary.md) offers quick refreshers on terminology, while
[Developing Locally](contributing.md) covers how to set up a development
environment and contribute back to the project. Whenever the book introduces a
new concept, look for references to the crate documentation so you can inspect
the corresponding APIs and examples.

By the end of this chapter you should have a mental model for why Trible Space
is structured the way it is. From there, head to the chapters that match your
goals—whether that means learning to query data effectively or integrating
Tribles into a larger system.
