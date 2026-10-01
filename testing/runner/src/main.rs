//! Command-line interface for running tests.
use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};
use ef_tests::{
    cases::blockchain_test::{BlockTestOptions, BlockchainTests},
    result::{OutputFormat, ResultPrinter},
    Suite,
};

mod report;

/// Command-line arguments for the test runner.
#[derive(Debug, Parser)]
#[command(args_conflicts_with_subcommands = true)]
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
    /// Print the results as one JSON array on stdout, once all fixtures ran (the default).
    #[arg(long, conflicts_with = "jsonl")]
    json_array: bool,
    /// Print each result as a JSON object on its own line of stdout, as it completes.
    #[arg(long)]
    jsonl: bool,
    /// Run blocks on the sequential executor instead of the BAL-driven parallel one, as the
    /// node flag of the same name does. Block import always runs the sequential executor.
    #[arg(long = "engine.disable-bal-parallel-execution")]
    disable_bal_parallel_execution: bool,
}

impl RunArgs {
    const fn output_format(&self) -> OutputFormat {
        if self.jsonl {
            OutputFormat::Jsonl
        } else {
            OutputFormat::JsonArray
        }
    }
}

fn main() {
    let cmd = TestRunnerCommand::parse();
    let Some(command) = cmd.command else {
        let suite_path = cmd.suite_path.expect("a suite path or a command is required");
        BlockchainTests::new(suite_path.join("blockchain_tests")).run();
        return
    };

    report::init();
    match command {
        Command::BlockTest(args) => {
            let suite = BlockchainTests::new(fixtures_path(&args.path, "blockchain_tests"));
            let options = BlockTestOptions {
                disable_bal_parallel_execution: args.disable_bal_parallel_execution,
            };
            let printer = ResultPrinter::new(args.output_format());
            suite.run_fixtures(options, &|result| printer.push(result));
            printer.finish();
        }
    }
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
