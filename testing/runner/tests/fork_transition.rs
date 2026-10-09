//! Checks that fixtures on a fork transition network run: two empty blocks, one on each side of
//! the fork at time 15k, on a genesis with excess blob gas. Each block's excess blob gas follows
//! the blob target of its own fork, so the blocks are only valid with the BPO forks' blob
//! parameters scheduled.

use std::{path::Path, process::Command};

/// Runs the runner with `args` on the fixture `name` and checks that it passes.
fn check(args: &[&str], name: &str) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name);
    let output = Command::new(env!("CARGO_BIN_EXE_ef-test-runner"))
        .args(&args[..1])
        .arg(&fixture)
        .args(&args[1..])
        .output()
        .unwrap();
    let results: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["pass"], true, "{}", results[0]["error"]);
    assert_eq!(output.status.code(), Some(0), "{}", String::from_utf8_lossy(&output.stderr));
}

fn check_enginetest(name: &str) {
    check(&["enginetest", "--workers", "1"], name);
}

#[test]
fn blocktest_bpo2_to_bpo3() {
    check(&["blocktest"], "blocktest_bpo2_to_bpo3.json");
}

#[test]
fn enginetest_bpo2_to_bpo3() {
    check_enginetest("enginetest_bpo2_to_bpo3.json");
}

#[test]
fn blocktest_bpo2_to_amsterdam() {
    check(&["blocktest"], "blocktest_bpo2_to_amsterdam.json");
}

#[test]
fn enginetest_bpo2_to_amsterdam() {
    check_enginetest("enginetest_bpo2_to_amsterdam.json");
}
