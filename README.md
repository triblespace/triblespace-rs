![Crates.io Version](https://img.shields.io/crates/v/triblespace)
![docs.rs](https://img.shields.io/docsrs/triblespace)
![Discord Shield](https://discordapp.com/api/guilds/795317845181464651/widget.png?style=shield)

🚧🚧🚧 Please note that this is work in progress, so while a lot of things have settled by now, we still favour breaking backwards compatiblity for seeminly minor improvements. 🚧🚧🚧


<img src="https://github.com/triblespace/triblespace-rs/blob/main/sticker.png?raw=true" width="300"
 alt="The mascot of the trible.space a cute fluffy trible with three eyes."/>


# About

> “We need to abolish names and places, and replace them with hashes.”
> — Joe Armstrong, [The Mess We’re In](https://www.youtube.com/watch?v=lKXe3HUG2l4)

**Trible Space** is a data space and knowledge graph standard. It offers metadata management capabilities similar to file- and version-control systems, combined with the queryability and convenience of an embedded database, tailored towards use with simple blob storage. It is designed to be a holistic yet lightweight data storage solution that can be used in a variety of contexts, from embedded systems to distributed cloud services.

Our goal is to re-invent data storage from first principles and overcome the shortcomings of prior "Semantic Web"/triple-store technologies. By focusing on simplicity, canonical data formats, cryptographic identifiers, and clean distributed semantics, we aim to provide a lean, lightweight yet powerful toolkit for knowledge representation, database management, and data exchange use cases.

## Features

- **Lean, Lightweight & Flexible**: Data storage seamlessly scales from in-memory data organization to large-scale blob and metadata storage on S3 like services.
- **Distributed**: Eventually consistent CRDT semantics (based on the CALM principle), compressed zero-copy archives, and built-in version control.
- **Predictable Performance**: An optimizer-free design using novel algorithms and data structures removes the need for manual query-tuning and enables single-digit microsecond latency.  
- **Fast In-Memory Datasets**: Enjoy cheap copy-on-write (COW) semantics and speedy set operations, allowing you to treat entire datasets as values.
- **Compile-Time Typed Queries**: Automatic type inference, type-checking, and auto-completion make writing queries a breeze. You can even create queries that span multiple datasets and native Rust data structures.
- **Low Overall Complexity**: We aim for a design that feels obvious (in the best way) and makes good use of existing language facilities. A serverless design makes it completely self-sufficient for local use and requires only an S3-compatible service for distribution.
- **Easy Implementation**: The spec is designed to be friendly to high- and low-level languages, or even hardware implementations.
- **Lock-Free Blob Writes**: Blob data is appended with a single `O_APPEND` write. Each handle advances an in-memory `applied_length` only if no other writer has appended in between, scanning any gap to ingest missing records. Concurrent writers may duplicate blobs, but hashes guarantee consistency. Updating branch heads uses a short `flush → refresh → lock → refresh → append → unlock` sequence.
- **Coordinated Refresh**: `refresh` acquires a shared file lock while scanning to avoid races with `restore` truncating the pile.

# Community

If you have any questions or want to chat about graph databases hop into our [discord](https://discord.gg/v7AezPywZS).

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
use triblespace::prelude::blobschemas::LongString;
use triblespace::core::repo::{memoryrepo::MemoryRepo, Repository};

mod literature {
    use triblespace::prelude::*;
    use triblespace::prelude::blobschemas::LongString;
    use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, R256, ShortString};

    attributes! {
        /// The title of a work.
        ///
        /// Small doc paragraph used in the book examples.
        "A74AA63539354CDA47F387A4C3A8D54C" as pub title: ShortString;

        /// A quote from a work.
        "6A03BAF6CFB822F04DA164ADAAEB53F6" as pub quote: Handle<Blake3, LongString>;

        /// The author of a work.
        "8F180883F9FD5F787E9E0AF0DF5866B9" as pub author: GenId;

        /// The first name of an author.
        "0DBB530B37B966D137C50B943700EDB2" as pub firstname: ShortString;

        /// The last name of an author.
        "6BAA463FD4EAF45F6A103DB9433E4545" as pub lastname: ShortString;

        /// The number of pages in the work.
        "FCCE870BECA333D059D5CD68C43B98F0" as pub page_count: R256;

        /// A pen name or alternate spelling for an author.
        "D2D1B857AC92CEAA45C0737147CA417E" as pub alias: ShortString;

        /// A throwaway prototype field; omit the id to derive it from the name and schema.
        pub prototype_note: Handle<Blake3, LongString>;
    }
}

// The examples pin explicit ids for shared schemas. For quick prototypes you
// can omit the hex literal and `attributes!` will derive a deterministic id
// from the attribute name and schema (via Attribute::from_name).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Repositories manage shared history; MemoryRepo keeps everything in-memory
    // for quick experiments. Swap in a `Pile` when you need durable storage.
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo
        .create_branch("main", None)
        .expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull workspace");

    // Workspaces stage TribleSets before committing them. The entity! macro
    // returns sets that merge cheaply into our current working set.
    let author_id = ufoid();
    let mut library = TribleSet::new();

    library += entity! { &author_id @
        literature::firstname: "Frank",
        literature::lastname: "Herbert",
    };

    library += entity! { &author_id @
        literature::title: "Dune",
        literature::author: &author_id,
        literature::quote: ws.put::<LongString, _>(
            "Deep in the human unconscious is a pervasive need for a logical              universe that makes sense. But the real universe is always one              step beyond logic."
        ),
        literature::quote: ws.put::<LongString, _>(
            "I must not fear. Fear is the mind-killer. Fear is the little-death              that brings total obliteration. I will face my fear. I will permit              it to pass over me and through me. And when it has gone past I will              turn the inner eye to see its path. Where the fear has gone there              will be nothing. Only I will remain."
        ),
    };

    ws.commit(library, None, Some("import dune"));

    // `checkout(..)` returns the accumulated TribleSet for the branch.
    let catalog = ws.checkout(..)?;
    let title = "Dune";

    // Use `_?ident` when you need a fresh variable scoped to this macro call
    // without declaring it in the find! projection list.
    for (f, l, quote) in find!(
        (first: String, last: Value<_>, quote),
        pattern!(&catalog, [
            { _?author @
                literature::firstname: ?first,
                literature::lastname: ?last
            },
            {
                literature::title: title,
                literature::author: _?author,
                literature::quote: ?quote
            }
        ])
    ) {
        let quote: View<str> = ws.get(quote)?;
        let quote = quote.as_ref();
        println!("'{quote}'
 - from {title} by {f} {}.", l.from_value::<&str>());
    }

    // Use `push` when you want automatic retries that merge concurrent history
    // into the workspace before publishing.
    repo.push(&mut ws).expect("publish initial library");

    // Stage a non-monotonic update that we plan to reconcile manually.
    ws.commit(
        entity! { &author_id @ literature::firstname: "Francis" },
        None,
        Some("use pen name"),
    );

    // Simulate a collaborator racing us with a different update.
    let mut collaborator = repo
        .pull(*branch_id)
        .expect("pull collaborator workspace");
    collaborator.commit(
        entity! { &author_id @ literature::firstname: "Franklin" },
        None,
        Some("record legal first name"),
    );
    repo.push(&mut collaborator)
        .expect("publish collaborator history");

    // `try_push` returns a conflict workspace when the CAS fails, letting us
    // inspect divergent history and decide how to merge it.
    if let Some(mut conflict_ws) = repo
        .try_push(&mut ws)
        .expect("attempt manual conflict resolution")
    {
        let conflict_catalog = conflict_ws.checkout(..)?;

        for (first,) in find!(
            (first: Value<_>),
            pattern!(&conflict_catalog, [{
                literature::author: &author_id,
                literature::firstname: ?first
            }])
        ) {
            println!("Collaborator kept the name '{}'.", first.from_value::<&str>());
        }

        ws.merge(&mut conflict_ws)
            .expect("merge conflicting history");

        ws.commit(
            entity! { &author_id @ literature::alias: "Francis" },
            None,
            Some("keep pen-name as an alias"),
        );

        repo.push(&mut ws)
            .expect("publish merged aliases");
    }

    Ok(())
}
```


The [Getting Started](https://triblespace.github.io/triblespace-rs/getting-started.html)
chapter of the book breaks this example down line by line, covers project
scaffolding, and introduces more background on how repositories, workspaces,
and queries interact.

## Tribles Book

For a step-by-step narrative guide, see the [Tribles Book](book/README.md).
To build the HTML locally, first install `mdbook` with `cargo install mdbook`
and then run:

```bash
./scripts/build_book.sh
```

For details on setting up a development environment, see [Developing Locally](book/src/contributing.md).

# Learn More

The best way to get started is to read the [Tribles Book](https://triblespace.github.io/triblespace-rs/). The following links mirror the book's chapter order so you can progress from the basics to more advanced topics:

1. [Introduction](https://triblespace.github.io/triblespace-rs/introduction.html)
2. [Getting Started](https://triblespace.github.io/triblespace-rs/getting-started.html)
3. [Architecture](https://triblespace.github.io/triblespace-rs/architecture.html)
4. [Query Language](https://triblespace.github.io/triblespace-rs/query-language.html)
5. [Incremental Queries](https://triblespace.github.io/triblespace-rs/incremental-queries.html)
6. [Schemas](https://triblespace.github.io/triblespace-rs/schemas.html)
7. [Repository Workflows](https://triblespace.github.io/triblespace-rs/repository-workflows.html)
8. [Commit Selectors](https://triblespace.github.io/triblespace-rs/commit-selectors.html)
9. [Philosophy](https://triblespace.github.io/triblespace-rs/deep-dive/philosophy.html)
10. [Identifiers](https://triblespace.github.io/triblespace-rs/deep-dive/identifiers.html)
11. [Trible Structure](https://triblespace.github.io/triblespace-rs/deep-dive/trible-structure.html)
12. [Pile Format](https://triblespace.github.io/triblespace-rs/pile-format.html)
## License

Licensed under either of

* MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.
