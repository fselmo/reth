//! Command-line interface for running tests.
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use clap::{Args, Parser, Subcommand};
use ef_tests::{
    cases::blockchain_test::{BlockTestOptions, BlockchainTests},
    result::FixtureResult,
    suite::find_all_files_with_extension,
    Suite,
};
use reth_node_core::version::version_metadata;

mod engine_test;
mod report;

/// Command-line arguments for the test runner.
#[derive(Debug, Parser)]
#[command(
    name = "ef-test-runner",
    version = version_metadata().short_version.as_ref(),
    propagate_version = true,
    args_conflicts_with_subcommands = true,
    arg_required_else_help = true
)]
pub struct TestRunnerCommand {
    #[command(subcommand)]
    command: Option<Command>,
    /// Path to a test suite that contains a `blockchain_tests` directory, when no command is
    /// given.
    suite_path: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run blockchain tests by importing their blocks.
    #[command(name = "blocktest", display_name = "ef-test-runner")]
    BlockTest(RunArgs),
    /// Run blockchain tests in the engine format through reth's Engine API handler, each on a
    /// datadir in the system temporary directory (`TMPDIR`; a tmpfs avoids disk syncs).
    #[command(name = "enginetest", display_name = "ef-test-runner")]
    EngineTest {
        #[command(flatten)]
        args: RunArgs,
        /// How many fixtures run at once.
        #[arg(long, default_value_t = default_workers())]
        workers: usize,
    },
}

#[derive(Debug, Args)]
struct RunArgs {
    /// A fixture file, or a directory searched for fixture files.
    path: PathBuf,
    /// Run blocks on the sequential executor instead of the BAL-driven parallel one, as the
    /// node flag of the same name does. Block import has only the sequential executor, so for
    /// `blocktest` this only changes the reported reason.
    #[arg(long = "engine.disable-bal-parallel-execution")]
    disable_bal_parallel_execution: bool,
    /// Print a `balExecution` JSON line on stderr for each executed block, saying which executor
    /// ran it and, for the sequential one, why.
    #[arg(long)]
    bal_report: bool,
}

fn default_workers() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

fn main() {
    let cmd = TestRunnerCommand::parse();
    let Some(command) = cmd.command else {
        let suite_path = cmd.suite_path.expect("a suite path or a command is required");
        BlockchainTests::new(suite_path.join("blockchain_tests")).run();
        return
    };

    let args = match &command {
        Command::BlockTest(args) | Command::EngineTest { args, .. } => args,
    };
    if args.bal_report {
        report::init();
    }
    let results = Mutex::new(Vec::new());
    let on_result = |result: FixtureResult| results.lock().unwrap().push(result);
    match command {
        Command::BlockTest(args) => {
            let suite = BlockchainTests::new(fixtures_path(&args.path, "blockchain_tests"));
            let options = BlockTestOptions {
                disable_bal_parallel_execution: args.disable_bal_parallel_execution,
            };
            suite.run_fixtures(options, &on_result);
        }
        Command::EngineTest { args, workers } => {
            let path = fixtures_path(&args.path, "blockchain_tests_engine");
            let mut files = find_all_files_with_extension(&path, ".json");
            files.sort();
            engine_test::run_fixtures(
                files,
                args.disable_bal_parallel_execution,
                workers,
                &on_result,
            );
        }
    }
    let results = results.into_inner().unwrap();
    println!("{}", serde_json::to_string(&results).expect("results serialize"));
}

/// Returns the `format` directory of a fixtures release if `path` is one, otherwise `path`.
fn fixtures_path(path: &Path, format: &str) -> PathBuf {
    let candidate = path.join(format);
    if candidate.is_dir() {
        candidate
    } else {
        path.to_path_buf()
    }
}
