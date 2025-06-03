#!/bin/sh

set -e

RUST_LOG="${RUST_LOG:-debug}"
export RUST_LOG="${RUST_LOG},bollard=off,hyper=off"

# Run the tests inside every directory with a Cargo.toml
find ./examples -name 'Cargo.toml' | while read -r file; do
    dir=$(dirname "$file")
    echo "Running tests in $dir"
    (cd "$dir" && cargo test -- --nocapture --test-threads 1) || {
        echo "Tests failed in $dir"
        exit 1
    }
done
