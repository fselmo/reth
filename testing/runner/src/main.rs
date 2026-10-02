//! Command-line interface for running tests.
use std::{
    fs, io,
    path::{Path, PathBuf},
};

use clap::{Args, Parser, Subcommand};
use ef_tests::{
    cases::{
        blockchain_test::{BlockTestOptions, BlockchainTests},
        engine_test::{EngineTestOptions, EngineTests},
    },
    result::{find_json_files, OutputFormat, ResultPrinter},
    Suite,
};
use reth_node_core::version::version_metadata;

mod report;

/// Command-line arguments for the test runner.
#[derive(Debug, Parser)]
#[command(
    name = "ef-test-runner",
    version = version_metadata().short_version.as_ref(),
    propagate_version = true,
    args_conflicts_with_subcommands = true
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
    /// Run blockchain tests by importing their blocks. Exits 1 if any fixture fails.
    #[command(name = "blocktest", display_name = "ef-test-runner")]
    BlockTest(RunArgs),
    /// Run blockchain tests in the engine format through reth's Engine API handler. Exits 1 if
    /// any fixture fails.
    #[command(name = "enginetest", display_name = "ef-test-runner")]
    EngineTest {
        #[command(flatten)]
        args: RunArgs,
        /// How many fixtures run at once.
        #[arg(long, default_value_t = default_workers())]
        workers: usize,
        /// Where each fixture's temporary datadir is created. Defaults to the system temporary
        /// directory (`TMPDIR`); a tmpfs such as `/dev/shm` avoids disk syncs.
        #[arg(long, value_name = "DIR", default_value_os_t = std::env::temp_dir())]
        datadir_root: PathBuf,
    },
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Fixture files, or directories searched for fixture files, all run in one process.
    #[arg(required = true)]
    paths: Vec<PathBuf>,
    /// Print the results as one JSON array on stdout, once all fixtures ran (the default).
    #[arg(long, conflicts_with = "jsonl")]
    json_array: bool,
    /// Print each result as a JSON object on its own line of stdout, as it completes.
    #[arg(long)]
    jsonl: bool,
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

impl RunArgs {
    const fn output_format(&self) -> OutputFormat {
        if self.jsonl {
            OutputFormat::Jsonl
        } else {
            OutputFormat::JsonArray
        }
    }
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
    for path in &args.paths {
        if let Err(err) = check_readable(path) {
            eprintln!("error: cannot read {}: {err}", path.display());
            std::process::exit(2);
        }
    }
    if args.bal_report {
        report::init();
    }
    let all_passed = match command {
        Command::BlockTest(args) => {
            let options = BlockTestOptions {
                disable_bal_parallel_execution: args.disable_bal_parallel_execution,
                check_exception: true,
            };
            let printer = ResultPrinter::new(args.output_format());
            let files = fixture_files(&args.paths, "blockchain_tests");
            BlockchainTests::run_fixtures(files, options, &|result| printer.push(result));
            printer.finish()
        }
        Command::EngineTest { args, workers, datadir_root } => {
            let options = EngineTestOptions {
                disable_bal_parallel_execution: args.disable_bal_parallel_execution,
                workers,
                datadir_root,
            };
            let printer = ResultPrinter::new(args.output_format());
            let files = fixture_files(&args.paths, "blockchain_tests_engine");
            EngineTests::run_fixtures(files, options, &|result| printer.push(result));
            printer.finish()
        }
    };
    if !all_passed {
        std::process::exit(1);
    }
}

/// Returns an error if `path` does not exist or cannot be read.
fn check_readable(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        fs::read_dir(path).map(drop)
    } else {
        fs::File::open(path).map(drop)
    }
}

/// Returns every fixture file under `paths`, in order.
fn fixture_files(paths: &[PathBuf], format: &str) -> Vec<PathBuf> {
    paths.iter().flat_map(|path| find_json_files(&fixtures_path(path, format))).collect()
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
