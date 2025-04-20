#!/bin/sh

set -e

RUST_LOG="${RUST_LOG:-debug}"
export RUST_LOG="${RUST_LOG},bollard=off,hyper=off"

# Run the tests inside every directory with
# a Cargo.tom
find ./examples -name 'Cargo.toml' -exec sh -c '
    dir=$(dirname "$1")
    echo "Running tests in $dir"
    (cd "$dir" && cargo test -- --test-threads 1)
  ' sh {} \;
