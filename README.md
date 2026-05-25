![Crates.io Version](https://img.shields.io/crates/v/triblespace)
![docs.rs](https://img.shields.io/docsrs/triblespace)
![Discord Shield](https://discordapp.com/api/guilds/795317845181464651/widget.png?style=shield)

![The mascot of trible.space, a cute fluffy trible with three eyes.](sticker.png)

# About

> “We need to abolish names and places, and replace them with hashes.”
> — Joe Armstrong, [The Mess We’re In](https://www.youtube.com/watch?v=lKXe3HUG2l4)

**TribleSpace** is an embedded knowledge graph with built-in version control. It combines the queryability of a database with the distributed semantics of a content-addressed storage system — all in a single append-only file or S3-compatible endpoint.

Designed from first principles to overcome the shortcomings of prior triple-store technologies, TribleSpace focuses on simplicity, cryptographic identifiers, and clean CRDT semantics to provide a lightweight yet powerful toolkit for knowledge representation, data management, and data exchange.

## Features

- **Scales from memory to cloud**: In-memory datasets, local pile files, and S3-compatible blob storage all use the same API.
- **Distributed by default**: Eventually consistent CRDT semantics (based on the CALM principle), compressed zero-copy archives, and built-in version control with branch/merge workflows.
- **Predictable performance**: An optimizer-free query engine using novel algorithms and data structures removes the need for manual query-tuning and delivers single-digit microsecond latency.
- **Datasets as values**: Cheap copy-on-write (COW) semantics and fast set operations let you treat entire datasets as ordinary values — diff, merge, and compose them freely.
- **Compile-time typed queries**: Automatic type inference, type-checking, and auto-completion make writing queries a breeze. Queries can span multiple datasets and native Rust data structures.
- **Serverless**: No background process needed. A single pile file is completely self-sufficient for local use; add an S3-compatible service when you need distribution.

## Getting Started

Add the crate to your project:

```bash
cargo add triblespace
```

Once the crate is installed, you can experiment immediately with the
quick-start program below. It showcases the attribute macros, workspace
staging, queries, and pushing commits to a repository.

```rust
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::prelude::*;

mod literature {
    use triblespace::prelude::*;
    use triblespace::prelude::blobencodings::LongString;
    use triblespace::prelude::inlineencodings::{GenId, Handle, R256, ShortString};

    attributes! {
        /// The title of a work.
        ///
        /// Small doc paragraph used in the book examples.
        "A74AA63539354CDA47F387A4C3A8D54C" as pub title: ShortString;

        /// A quote from a work.
        "6A03BAF6CFB822F04DA164ADAAEB53F6" as pub quote: Handle<LongString>;

        /// The author of a work.
        "8F180883F9FD5F787E9E0AF0DF5866B9" as pub author: GenId;

        /// The first name of an author.
        "0DBB530B37B966D137C50B943700EDB2" as pub firstname: ShortString;

        /// The last name of an author.
        "6BAA463FD4EAF45F6A103DB9433E4545" as pub lastname: ShortString;

        /// The number of pages in the work.
        "FCCE870BECA333D059D5CD68C43B98F0" as pub page_count: R256;

        /// A throwaway prototype field; omit the id to derive it from the name and encoding.
        pub prototype_note: Handle<LongString>;
    }
}

// The examples pin explicit ids for shared encodings. For quick prototypes you
// can omit the hex literal and `attributes!` will derive a deterministic id
// from the attribute name and encoding via the entity-core mechanism —
// `Attribute::<S>::from(entity!{ metadata::name: <name handle>,
// metadata::value_encoding: <S as MetaDescribe>::id() })`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Repositories manage shared history; MemoryRepo keeps everything in-memory
    // for quick experiments. Swap in a `Pile` when you need durable storage.
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new())?;
    let branch_id = repo
        .create_branch("main", None)
        .expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull workspace");

    // The entity! macro returns a Fragment carrying both facts and any
    // blob payloads it auto-put while building. Accumulate into another
    // Fragment with `+=` so blobs flow through into the commit; commit
    // accepts anything `Into<Fragment>`.
    let herbert = ufoid();
    let dune = ufoid();
    let mut library = Fragment::empty();

    library += entity! { &herbert @
        literature::firstname: "Frank",
        literature::lastname: "Herbert",
    };

    library += entity! { &dune @
        literature::title: "Dune",
        literature::author: &herbert,
        literature::quote: "I must not fear. Fear is the mind-killer.",
    };

    ws.commit(library, "import dune");

    // `checkout(..)` returns a Checkout — a TribleSet paired with the
    // commits that produced it, usable for incremental delta queries.
    let catalog = ws.checkout(..)?;
    let title = "Dune";

    // Multi-entity join: find quotes by authors of a given title.
    // `_?author` is a pattern-local variable that joins without projecting.
    for (f, l, quote) in find!(
        (first: String, last: String, quote),
        pattern!(&catalog, [
            { _?author @
                literature::firstname: ?first,
                literature::lastname: ?last
            },
            { _?book @
                literature::title: title,
                literature::author: _?author,
                literature::quote: ?quote
            }
        ])
    ) {
        let quote: View<str> = ws.get(quote)?;
        let quote = quote.as_ref();
        println!("'{quote}'\n - from {title} by {f} {l}.");
    }

    repo.push(&mut ws).expect("publish initial library");

    // ── Concurrent commits ─────────────────────────────────────────
    // We stage a new author; a collaborator independently stages a
    // different new author. The writes don't overlap semantically,
    // but they both started from the same head — try_push detects
    // the lineage divergence so we can fold the collaborator's
    // history in.

    let butler = ufoid();
    ws.commit(
        entity! { &butler @
            literature::firstname: "Octavia",
            literature::lastname: "Butler",
        },
        "add Butler",
    );

    let mut collaborator = repo.pull(*branch_id).expect("pull");
    let leguin = ufoid();
    collaborator.commit(
        entity! { &leguin @
            literature::firstname: "Ursula",
            literature::lastname: "Le Guin",
        },
        "add Le Guin",
    );
    repo.push(&mut collaborator).expect("publish collaborator");

    // Our push fails because the branch advanced while we were
    // working. The returned workspace carries the collaborator's
    // history; we replay our pending addition on top of it.
    if let Some(mut conflict_ws) = repo
        .try_push(&mut ws)
        .expect("attempt push")
    {
        conflict_ws.commit(
            entity! { &butler @
                literature::firstname: "Octavia",
                literature::lastname: "Butler",
            },
            "add Butler (rebased)",
        );
        repo.push(&mut conflict_ws).expect("publish resolution");
        ws = conflict_ws;
    }

    // Final catalog: all three authors present, no overwrites.
    let catalog = ws.checkout(..)?;
    let mut names: Vec<(String, String)> = find!(
        (first: String, last: String),
        pattern!(&catalog, [
            { _?author @
                literature::firstname: ?first,
                literature::lastname: ?last
            }
        ])
    ).collect();
    names.sort();
    println!("Catalog after merge ({} authors):", names.len());
    for (f, l) in names {
        println!("  - {f} {l}");
    }

    Ok(())
}
```


The [Getting Started](https://triblespace.github.io/triblespace-rs/getting-started.html)
chapter of the book breaks this example down line by line, covers project
scaffolding, and introduces more background on how repositories, workspaces,
and queries interact.

## Learn More

The [Tribles Book](https://triblespace.github.io/triblespace-rs/) is the best place to go deeper:

1. [Introduction](https://triblespace.github.io/triblespace-rs/introduction.html)
2. [Getting Started](https://triblespace.github.io/triblespace-rs/getting-started.html)
3. [Architecture](https://triblespace.github.io/triblespace-rs/architecture.html)
4. [Query Language](https://triblespace.github.io/triblespace-rs/query-language.html)
5. [Incremental Queries](https://triblespace.github.io/triblespace-rs/incremental-queries.html)
6. [Encodings](https://triblespace.github.io/triblespace-rs/encodings.html)
7. [Repository Workflows](https://triblespace.github.io/triblespace-rs/repository-workflows.html)
8. [Commit Selectors](https://triblespace.github.io/triblespace-rs/commit-selectors.html)
9. [Philosophy](https://triblespace.github.io/triblespace-rs/deep-dive/philosophy.html)
10. [Identifiers](https://triblespace.github.io/triblespace-rs/deep-dive/identifiers.html)
11. [Trible Structure](https://triblespace.github.io/triblespace-rs/deep-dive/trible-structure.html)
12. [Pile Format](https://triblespace.github.io/triblespace-rs/pile-format.html)

To build the book locally: `cargo install mdbook && ./scripts/build_book.sh`

For development setup, see [Contributing](book/src/contributing.md).

## Community

Questions or ideas? Join the [Discord](https://discord.gg/v7AezPywZS).

## License

Licensed under either of

* MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.
