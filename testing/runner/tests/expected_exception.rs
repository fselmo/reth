//! Checks that a block expected to fail must fail for the exception the fixture names.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// A Paris fixture with one empty block on genesis whose state root is wrong, in the given
/// format, expecting `BlockException.INVALID_STATE_ROOT`.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

/// Runs the runner with `args` on `fixture` and returns its one result.
fn run(args: &[&str], fixture: &Path) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_ef-test-runner"))
        .args(&args[..1])
        .arg(fixture)
        .args(&args[1..])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let mut results: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(results.len(), 1);
    results.remove(0)
}

/// Runs the fixture as is, which passes, and with its expected exception swapped for another,
/// which fails and names both.
fn check(args: &[&str], name: &str) {
    let fixture = fixture(name);
    let result = run(args, &fixture);
    assert_eq!(result["pass"], true, "{}", result["error"]);

    let dir = tempfile::tempdir().unwrap();
    let swapped = dir.path().join(name);
    let json = fs::read_to_string(&fixture).unwrap().replace(
        "BlockException.INVALID_STATE_ROOT",
        "TransactionException.INSUFFICIENT_ACCOUNT_FUNDS",
    );
    fs::write(&swapped, json).unwrap();

    let result = run(args, &swapped);
    assert_eq!(result["pass"], false);
    let error = result["error"].as_str().unwrap();
    assert!(error.contains("TransactionException.INSUFFICIENT_ACCOUNT_FUNDS"), "{error}");
    assert!(error.contains("BlockException.INVALID_STATE_ROOT"), "{error}");
}

#[test]
fn blocktest_checks_the_reason() {
    check(&["blocktest"], "blocktest_bad_state_root.json");
}

#[test]
fn enginetest_checks_the_reason() {
    let datadir_root = tempfile::tempdir().unwrap();
    let datadir_root = datadir_root.path().to_str().unwrap();
    check(
        &["enginetest", "--workers", "1", "--datadir-root", datadir_root],
        "enginetest_bad_state_root.json",
    );
}
