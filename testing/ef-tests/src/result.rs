//! Test results and errors

use alloy_primitives::B256;
use reth_db::DatabaseError;
use reth_provider::ProviderError;
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};
use thiserror::Error;
use walkdir::{DirEntry, WalkDir};

/// Test errors
///
/// # Note
///
/// `Error::Skipped` should not be treated as a test failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The test was skipped
    #[error("test was skipped")]
    Skipped,
    /// Block processing failed
    /// Note: This includes but is not limited to execution.
    /// For example, the header number could be incorrect.
    #[error("block {block_number} failed to process: {err}")]
    BlockProcessingFailed {
        /// The block number for the block that failed
        block_number: u64,
        /// The specific error
        #[source]
        err: Box<dyn std::error::Error + Send + Sync>,
    },
    /// An IO error occurred
    #[error("an error occurred interacting with the file system at {path}: {error}")]
    Io {
        /// The path to the file or directory
        path: PathBuf,
        /// The specific error
        #[source]
        error: std::io::Error,
    },
    /// A deserialization error occurred
    #[error("an error occurred deserializing the test at {path}: {error}")]
    CouldNotDeserialize {
        /// The path to the file we wanted to deserialize
        path: PathBuf,
        /// The specific error
        #[source]
        error: serde_json::Error,
    },
    /// A database error occurred.
    #[error(transparent)]
    Database(#[from] DatabaseError),
    /// A test assertion failed.
    #[error("test failed: {0}")]
    Assertion(String),
    /// An error internally in reth occurred.
    #[error("test failed: {0}")]
    Provider(#[from] ProviderError),
    /// An error occurred while decoding RLP.
    #[error("an error occurred deserializing RLP: {0}")]
    RlpDecodeError(#[from] alloy_rlp::Error),
    /// A consensus error occurred.
    #[error("an error occurred during consensus checks: {0}")]
    ConsensusError(#[from] reth_consensus::ConsensusError),
}

impl Error {
    /// Create a new [`Error::BlockProcessingFailed`] error.
    pub fn block_failed(
        block_number: u64,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::BlockProcessingFailed { block_number, err: Box::new(err) }
    }
}

/// The result of running a test.
#[derive(Debug)]
pub struct CaseResult {
    /// A description of the test.
    pub desc: String,
    /// The full path to the test.
    pub path: PathBuf,
    /// The result of the test.
    pub result: Result<(), Error>,
}

impl CaseResult {
    /// Create a new test result.
    pub fn new(path: &Path, desc: String, result: Result<(), Error>) -> Self {
        Self { desc, path: path.into(), result }
    }
}

/// The result of one fixture, as printed by the runner's JSON output.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FixtureResult {
    /// The fixture name, or the file path for a file that failed to load.
    pub name: String,
    /// Whether the fixture passed.
    pub pass: bool,
    /// The fixture's network, empty for a file that failed to load.
    pub fork: String,
    /// The hash of the client's head block once the fixture ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_block_hash: Option<B256>,
    /// The status of the last `engine_newPayload` call, for engine tests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_payload_status: Option<String>,
    /// Why the fixture failed, empty if it passed.
    pub error: String,
}

impl FixtureResult {
    /// Creates the result of a fixture that ran.
    pub fn new(name: String, fork: String, result: Result<(), Error>) -> Self {
        let error = result.err().map(|err| err.to_string()).unwrap_or_default();
        Self {
            name,
            pass: error.is_empty(),
            fork,
            last_block_hash: None,
            last_payload_status: None,
            error,
        }
    }

    /// Creates the failed result of a file that could not be loaded.
    pub fn load_failed(path: &Path, error: Error) -> Self {
        Self::new(path.display().to_string(), String::new(), Err(error))
    }

    /// Sets the hash of the client's head block.
    pub const fn with_last_block_hash(mut self, hash: B256) -> Self {
        self.last_block_hash = Some(hash);
        self
    }
}

/// How the runner prints [`FixtureResult`]s on stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// One JSON array of all results, printed once the run ends.
    JsonArray,
    /// One JSON object per line, printed as each fixture completes.
    Jsonl,
}

/// Prints [`FixtureResult`]s to stdout in an [`OutputFormat`].
#[derive(Debug)]
pub struct ResultPrinter {
    format: OutputFormat,
    results: Mutex<Vec<FixtureResult>>,
}

impl ResultPrinter {
    /// Creates a printer for the given format.
    pub const fn new(format: OutputFormat) -> Self {
        Self { format, results: Mutex::new(Vec::new()) }
    }

    /// Records one result, printing it right away in [`OutputFormat::Jsonl`].
    pub fn push(&self, result: FixtureResult) {
        match self.format {
            OutputFormat::JsonArray => self.results.lock().unwrap().push(result),
            OutputFormat::Jsonl => {
                let line = serde_json::to_string(&result).expect("result serializes");
                println!("{line}");
            }
        }
    }

    /// Prints the JSON array in [`OutputFormat::JsonArray`].
    pub fn finish(self) {
        if self.format == OutputFormat::JsonArray {
            let results = self.results.into_inner().unwrap();
            println!("{}", serde_json::to_string(&results).expect("results serialize"));
        }
    }
}

/// Returns every `.json` file under `path`, or `path` itself if it is a file, following symlinks.
pub fn find_json_files(path: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = WalkDir::new(path)
        .follow_links(true)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".json"))
        .map(DirEntry::into_path)
        .collect();
    files.sort();
    files
}

/// Assert that all the given tests passed and print the results to stdout.
pub(crate) fn assert_tests_pass(suite_name: &str, path: &Path, results: &[CaseResult]) {
    let (passed, failed, skipped) = categorize_results(results);

    print_results(suite_name, path, &passed, &failed, &skipped);

    assert!(failed.is_empty(), "Some tests failed (see above)");
}

/// Categorize test results into `(passed, failed, skipped)`.
pub(crate) fn categorize_results(
    results: &[CaseResult],
) -> (Vec<&CaseResult>, Vec<&CaseResult>, Vec<&CaseResult>) {
    let mut passed = Vec::new();
    let mut failed = Vec::new();
    let mut skipped = Vec::new();

    for case in results {
        match case.result.as_ref().err() {
            Some(Error::Skipped) => skipped.push(case),
            Some(_) => failed.push(case),
            None => passed.push(case),
        }
    }

    (passed, failed, skipped)
}

/// Display the given test results to stdout.
pub(crate) fn print_results(
    suite_name: &str,
    path: &Path,
    passed: &[&CaseResult],
    failed: &[&CaseResult],
    skipped: &[&CaseResult],
) {
    println!("Suite: {suite_name} (at {})", path.display());
    println!(
        "Ran {} tests ({} passed, {} failed, {} skipped)",
        passed.len() + failed.len() + skipped.len(),
        passed.len(),
        failed.len(),
        skipped.len()
    );

    for case in skipped {
        println!("[S] Case {} skipped", case.path.display());
    }

    for case in failed {
        let error = case.result.as_ref().unwrap_err();
        println!("[!] Case {} failed (description: {}): {}", case.path.display(), case.desc, error);
    }
}
