//! Checks that `balExecution` lines are printed only with `--bal-report`.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

/// A Paris fixture with one empty block on genesis, in the given format.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

/// Runs the runner with `args`, checks that the fixture passed and returns its stderr lines that
/// are `balExecution` events.
fn bal_execution_lines(args: &[&str]) -> Vec<serde_json::Value> {
    let output = Command::new(env!("CARGO_BIN_EXE_ef-test-runner")).args(args).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));

    let results: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["pass"], true, "{}", results[0]["error"]);

    String::from_utf8(output.stderr)
        .unwrap()
        .lines()
        .filter(|line| line.contains("balExecution"))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn assert_reports_block_one(lines: &[serde_json::Value]) {
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0]["block"], 1);
    assert_eq!(lines[0]["path"], "sequential");
    assert_eq!(lines[0]["reason"], "no-access-list");
}

#[test]
fn blocktest_reports_only_with_flag() {
    let fixture = fixture("blocktest_empty_block.json");
    let fixture = fixture.to_str().unwrap();

    assert_reports_block_one(&bal_execution_lines(&["blocktest", fixture, "--bal-report"]));
    assert!(bal_execution_lines(&["blocktest", fixture]).is_empty());
}

#[test]
fn enginetest_reports_only_with_flag() {
    let fixture = fixture("enginetest_empty_block.json");
    let fixture = fixture.to_str().unwrap();
    let datadir_root = tempfile::tempdir().unwrap();
    let datadir_root = datadir_root.path().to_str().unwrap();
    let run = ["enginetest", fixture, "--workers", "1", "--datadir-root", datadir_root];

    assert_reports_block_one(&bal_execution_lines(&[&run[..], &["--bal-report"]].concat()));
    assert!(bal_execution_lines(&run).is_empty());
}
