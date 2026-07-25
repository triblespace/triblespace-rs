#!/usr/bin/env bash
set -euo pipefail

# Move to repository root
cd "$(dirname "$0")/.."

# Ensure rustfmt is installed
rustup component add rustfmt

# Ensure mdBook is installed
if ! command -v mdbook >/dev/null 2>&1; then
    cargo install mdbook
fi

# Run formatting check, tests, and build the book
cargo fmt --all -- --check
cargo test
./scripts/build_book.sh
