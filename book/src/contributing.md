# Developing Locally

Tribles is developed with stable Rust tooling and a small collection of helper
scripts. This chapter walks through preparing a development environment,
running tests, and rebuilding the documentation so you can iterate with
confidence.

## Prerequisites

1. Install a recent Rust toolchain from [rustup.rs](https://rustup.rs/). The
   default `stable` channel is what the project targets.
2. Clone the repository and switch into it:

   ```shell
   git clone https://github.com/TribleSpace/triblespace-rs.git
   cd triblespace-rs
   ```

3. (Optional) Install [`mdbook`](https://rust-lang.github.io/mdBook/) with
   `cargo install mdbook` if you would like to preview the rendered
   documentation locally.

## Everyday workflows

The repository includes several scripts that keep formatting, tests, and book
builds in sync:

- `./scripts/devtest.sh` executes the most relevant unit and integration tests
  for fast feedback while you are iterating.
- `./scripts/preflight.sh` runs formatting, the full test suite, and rebuilds
  this book. Run it before committing to ensure your branch is in good shape.
- `./scripts/build_book.sh` regenerates the documentation after you modify
  Markdown chapters or code snippets.

You can always fall back to the standard Cargo commands (`cargo fmt`,
`cargo test`, etc.) if you prefer to run specific tools by hand.

## Rebuilding the book

Once `mdbook` is installed you can rebuild the documentation with:

```shell
./scripts/build_book.sh
```

This script compiles the chapters into `book/book`, allowing you to open the
HTML output in a browser. Rebuilding regularly helps catch stale examples and
keeps the published copy aligned with the codebase.
