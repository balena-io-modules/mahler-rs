use anyhow::Result;
use assert_cmd::cargo;
use assert_cmd::prelude::*; // Add methods on commands
use predicates::prelude::*;
use std::process::{Command, Output}; // Run programs

#[test]
fn run_example() -> Result<()> {
    let mut cmd = Command::new(cargo::cargo_bin!(env!("CARGO_PKG_NAME")));
    let assert = cmd.env("RUST_LOG", "debug").assert();

    let Output { stdout, stderr, .. } = cmd.unwrap();
    println!("{}", String::from_utf8_lossy(&stderr));
    println!("{}", String::from_utf8_lossy(&stdout));

    assert.success().stdout(predicate::str::diff(
        "The system state is now Counters({\"a\": 1, \"b\": 2})\n",
    ));

    Ok(())
}
