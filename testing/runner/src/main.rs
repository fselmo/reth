//! Command-line interface for running tests.
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use clap::{Args, Parser, Subcommand};
use ef_tests::{cases::blockchain_test::BlockchainTests, result::FixtureResult, Suite};

/// Command-line arguments for the test runner.
#[derive(Debug, Parser)]
#[command(args_conflicts_with_subcommands = true, arg_required_else_help = true)]
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
    #[command(name = "blocktest")]
    BlockTest(RunArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    /// A fixture file, or a directory searched for fixture files.
    path: PathBuf,
}

fn main() {
    let cmd = TestRunnerCommand::parse();
    let Some(command) = cmd.command else {
        let suite_path = cmd.suite_path.expect("a suite path or a command is required");
        BlockchainTests::new(suite_path.join("blockchain_tests")).run();
        return
    };

    let results = Mutex::new(Vec::new());
    let on_result = |result: FixtureResult| results.lock().unwrap().push(result);
    match command {
        Command::BlockTest(args) => {
            let suite = BlockchainTests::new(fixtures_path(&args.path, "blockchain_tests"));
            suite.run_fixtures(&on_result);
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
