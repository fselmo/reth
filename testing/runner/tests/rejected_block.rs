//! Checks the verdict on a block the client rejects: it depends on whether the fixture expects a
//! rejection, never on the reason it names, and the exit status agrees with it.

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

/// Runs the runner with `args` on `fixture`, checks that it exits 0 exactly when the fixture
/// passed, and returns its one result.
fn run(args: &[&str], fixture: &Path) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_ef-test-runner"))
        .args(&args[..1])
        .arg(fixture)
        .args(&args[1..])
        .output()
        .unwrap();
    let mut results: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(results.len(), 1);
    let result = results.remove(0);
    let expected_code = if result["pass"] == true { 0 } else { 1 };
    assert_eq!(
        output.status.code(),
        Some(expected_code),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    result
}

/// Writes `fixture` with `edit` applied to its one block or payload into `dir`.
fn edited(
    dir: &Path,
    name: &str,
    edit: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
) -> PathBuf {
    let mut json: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(fixture(name)).unwrap()).unwrap();
    let test = json.as_object_mut().unwrap().values_mut().next().unwrap();
    let key = if test.get("blocks").is_some() { "blocks" } else { "engineNewPayloads" };
    let block = &mut test[key][0];
    edit(block.as_object_mut().unwrap());
    let path = dir.join(name);
    fs::write(&path, json.to_string()).unwrap();
    path
}

/// Runs the fixture as is, with its expected exception swapped for another, both of which pass,
/// and without an expected exception, which fails.
fn check(args: &[&str], name: &str, exception_key: &str) {
    let result = run(args, &fixture(name));
    assert_eq!(result["pass"], true, "{}", result["error"]);

    let dir = tempfile::tempdir().unwrap();
    let other_reason = edited(dir.path(), name, |block| {
        block.insert(
            exception_key.to_string(),
            "TransactionException.INSUFFICIENT_ACCOUNT_FUNDS".into(),
        );
    });
    let result = run(args, &other_reason);
    assert_eq!(result["pass"], true, "{}", result["error"]);

    let expected_valid = edited(dir.path(), name, |block| {
        block.remove(exception_key);
    });
    let result = run(args, &expected_valid);
    assert_eq!(result["pass"], false);
}

#[test]
fn blocktest_rejected_block() {
    check(&["blocktest"], "blocktest_bad_state_root.json", "expectException");
}

#[test]
fn enginetest_rejected_payload() {
    let datadir_root = tempfile::tempdir().unwrap();
    let datadir_root = datadir_root.path().to_str().unwrap();
    check(
        &["enginetest", "--workers", "1", "--datadir-root", datadir_root],
        "enginetest_bad_state_root.json",
        "validationError",
    );
}
