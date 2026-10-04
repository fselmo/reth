//! Checks that every path given to the runner runs, in one process with one results array.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// A Paris fixture with one block on genesis, in the given format.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

/// Runs `command` on one fixture file and on a directory holding the other, and checks that
/// both fixtures' results come back in one array.
fn check(command: &str, extra_args: &[&str], file: &str, file_in_dir: &str) {
    let dir = tempfile::tempdir().unwrap();
    fs::copy(fixture(file_in_dir), dir.path().join(file_in_dir)).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_ef-test-runner"))
        .arg(command)
        .arg(fixture(file))
        .arg(dir.path())
        .args(extra_args)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));

    let results: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    let mut names: Vec<_> = results.iter().map(|result| result["name"].as_str().unwrap()).collect();
    names.sort_unstable();
    assert_eq!(names, ["bad_state_root", "empty_block"]);
    assert!(results.iter().all(|result| result["pass"] == true), "{results:?}");
}

#[test]
fn blocktest_runs_every_path() {
    check("blocktest", &[], "blocktest_empty_block.json", "blocktest_bad_state_root.json");
}

#[test]
fn enginetest_runs_every_path() {
    check(
        "enginetest",
        &["--workers", "1"],
        "enginetest_empty_block.json",
        "enginetest_bad_state_root.json",
    );
}

/// A path that does not exist stops the run before any fixture runs.
#[test]
fn a_missing_path_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_ef-test-runner"))
        .arg("blocktest")
        .arg(fixture("blocktest_empty_block.json"))
        .arg(dir.path().join("missing.json"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty(), "{}", String::from_utf8_lossy(&output.stdout));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot read") && stderr.contains("missing.json"), "{stderr}");
}

/// With no command and no suite path, the runner prints its usage instead of running.
#[test]
fn no_arguments_print_usage() {
    let output = Command::new(env!("CARGO_BIN_EXE_ef-test-runner")).output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Usage: ef-test-runner"), "{stderr}");
}
