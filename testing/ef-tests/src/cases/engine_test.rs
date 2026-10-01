//! Test runner for blockchain tests in the engine format.
//!
//! Each fixture runs against its own in-process node, launched as `reth node` launches one, on a
//! temporary database. Payloads and forkchoice updates go through the node's authenticated
//! Engine API server, with the parameters from the fixture, at the versions the fixture names.

use crate::{
    models::{EngineNewPayload, EngineTest},
    result::{find_json_files, FixtureResult},
    Error,
};
use alloy_primitives::B256;
use jsonrpsee::core::{
    client::{ClientT, Error as RpcClientError},
    params::ArrayParams,
};
use reth_chainspec::EthChainSpec;
use reth_node_builder::{NodeBuilder, NodeConfig, NodeHandle};
use reth_node_ethereum::EthereumNode;
use reth_provider::BlockNumReader;
use reth_tasks::Runtime;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

/// How long a node's tasks get to stop once its fixture has run.
const NODE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// The cross-block cache size of each node, in MB (`--engine.cross-block-cache-size`).
const CROSS_BLOCK_CACHE_SIZE_MB: usize = 16;

/// Options for running engine tests.
#[derive(Debug, Clone, Copy)]
pub struct EngineTestOptions {
    /// The node's `--engine.disable-bal-parallel-execution`.
    pub disable_bal_parallel_execution: bool,
    /// How many fixtures run at once, each against its own node.
    pub workers: usize,
}

/// A handler for blockchain tests in the engine format.
#[derive(Debug)]
pub struct EngineTests {
    suite_path: PathBuf,
}

impl EngineTests {
    /// Creates a handler for the fixtures under `suite_path`, which may also be a single file.
    pub const fn new(suite_path: PathBuf) -> Self {
        Self { suite_path }
    }

    /// Runs every fixture in the JSON files under the suite path and passes one result per
    /// fixture to `on_result` as it completes. A file that fails to load is reported as a failed
    /// result.
    pub fn run_fixtures(
        &self,
        options: EngineTestOptions,
        on_result: &(dyn Fn(FixtureResult) + Sync),
    ) {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(options.workers.max(1))
            .thread_name(|idx| format!("enginetest-{idx}"))
            .build()
            .expect("failed to build the engine test thread pool");
        pool.install(|| {
            use rayon::prelude::*;
            find_json_files(&self.suite_path).into_par_iter().for_each(|path| {
                let tests = match load(&path) {
                    Ok(tests) => tests,
                    Err(err) => return on_result(FixtureResult::load_failed(&path, err)),
                };
                for (name, test) in tests {
                    on_result(run_fixture(name, &test, options));
                }
            });
        });
    }
}

fn load(path: &Path) -> Result<BTreeMap<String, EngineTest>, Error> {
    let contents =
        fs::read_to_string(path).map_err(|error| Error::Io { path: path.into(), error })?;
    serde_json::from_str(&contents)
        .map_err(|error| Error::CouldNotDeserialize { path: path.into(), error })
}

/// What the node ended with, reported beside the verdict.
#[derive(Debug, Default)]
struct Outcome {
    last_block_hash: Option<B256>,
    last_payload_status: Option<String>,
}

/// Runs one fixture against a fresh node, on a runtime of its own that is shut down afterwards.
fn run_fixture(name: String, test: &EngineTest, options: EngineTestOptions) -> FixtureResult {
    let fork = format!("{:?}", test.network);
    let mut outcome = Outcome::default();
    let runtime = Runtime::test();
    let result = runtime.handle().clone().block_on(run_case(test, options, &runtime, &mut outcome));
    runtime.graceful_shutdown_with_timeout(NODE_SHUTDOWN_TIMEOUT);
    runtime.shutdown_timeout(NODE_SHUTDOWN_TIMEOUT);

    let mut result = FixtureResult::new(name, fork, result);
    result.last_block_hash = outcome.last_block_hash;
    result.last_payload_status = outcome.last_payload_status;
    result
}

async fn run_case(
    test: &EngineTest,
    options: EngineTestOptions,
    runtime: &Runtime,
    outcome: &mut Outcome,
) -> Result<(), Error> {
    let chain_spec = Arc::new(test.chain_spec());
    let genesis_hash = chain_spec.genesis_hash();
    if genesis_hash != test.genesis_block_header.hash {
        return Err(Error::Assertion(format!(
            "genesis hash mismatch: the chain spec has {genesis_hash}, the fixture {}",
            test.genesis_block_header.hash
        )))
    }

    let mut config = NodeConfig::test().with_chain(chain_spec).with_disabled_discovery();
    config.engine.bal_parallel_execution_disabled = options.disable_bal_parallel_execution;
    // The default 4 GB cross-block cache would dominate the memory of concurrent fixtures.
    config.engine.cross_block_cache_size = CROSS_BLOCK_CACHE_SIZE_MB;
    let NodeHandle { node, node_exit_future: _ } = NodeBuilder::new(config)
        .testing_node(runtime.clone())
        .node(EthereumNode::default())
        .launch()
        .await
        .map_err(|err| Error::Assertion(format!("failed to launch the node: {err:#}")))?;
    let engine = node.auth_server_handle().http_client();

    let result = drive(&engine, test, outcome).await;
    outcome.last_block_hash = node.provider.chain_info().ok().map(|info| info.best_hash);
    result?;

    match outcome.last_block_hash {
        Some(hash) if hash == test.lastblockhash => Ok(()),
        hash => Err(Error::Assertion(format!(
            "last block hash mismatch: got {hash:?}, expected {}",
            test.lastblockhash
        ))),
    }
}

/// Sends the fixture's payloads in order, each valid one followed by a forkchoice update that
/// makes it the head, as a consensus client would.
async fn drive(
    engine: &impl ClientT,
    test: &EngineTest,
    outcome: &mut Outcome,
) -> Result<(), Error> {
    let Some(first) = test.engine_new_payloads.first() else { return Ok(()) };
    forkchoice_updated(engine, &first.forkchoice_updated_version, test.genesis_block_header.hash)
        .await
        .map_err(|err| Error::Assertion(format!("forkchoice update to genesis: {err}")))?;

    for (idx, payload) in test.engine_new_payloads.iter().enumerate() {
        new_payload(engine, payload, outcome)
            .await
            .map_err(|err| Error::Assertion(format!("payload {idx}: {err}")))?;
        if payload.validation_error.is_none() && payload.error_code.is_none() {
            let head = payload.params[0]["blockHash"].as_str().unwrap_or_default();
            let head = head.parse().map_err(|err| {
                Error::Assertion(format!("payload {idx}: invalid blockHash {head:?}: {err}"))
            })?;
            forkchoice_updated(engine, &payload.forkchoice_updated_version, head)
                .await
                .map_err(|err| Error::Assertion(format!("payload {idx}: {err}")))?;
        }
    }
    Ok(())
}

/// Calls `engine_newPayloadV<n>` and checks the response against the fixture's expectation.
async fn new_payload(
    engine: &impl ClientT,
    payload: &EngineNewPayload,
    outcome: &mut Outcome,
) -> Result<(), String> {
    let method = format!("engine_newPayloadV{}", payload.new_payload_version);
    let mut params = ArrayParams::new();
    for param in &payload.params {
        params.insert(param).map_err(|err| format!("{method} parameters: {err}"))?;
    }
    let expected_code = payload
        .error_code
        .as_deref()
        .map(str::parse::<i32>)
        .transpose()
        .map_err(|err| format!("invalid errorCode: {err}"))?;

    match engine.request::<Value, _>(&method, params).await {
        Ok(response) => {
            let status = response["status"].as_str().unwrap_or_default().to_string();
            outcome.last_payload_status = Some(status.clone());
            let expected = if payload.validation_error.is_some() { "INVALID" } else { "VALID" };
            if status != expected {
                return Err(format!("{method} returned {response}, expected status {expected}"))
            }
            if let Some(code) = expected_code {
                return Err(format!("{method} returned {response}, expected error code {code}"))
            }
            Ok(())
        }
        Err(RpcClientError::Call(err)) => {
            outcome.last_payload_status = Some(format!("error {}", err.code()));
            if expected_code == Some(err.code()) {
                Ok(())
            } else {
                Err(format!("{method} failed with error {}: {}", err.code(), err.message()))
            }
        }
        Err(err) => Err(format!("{method} failed: {err}")),
    }
}

/// Calls `engine_forkchoiceUpdatedV<n>` without payload attributes and requires a valid head.
async fn forkchoice_updated(
    engine: &impl ClientT,
    version: &str,
    head: B256,
) -> Result<(), String> {
    let method = format!("engine_forkchoiceUpdatedV{version}");
    let state = json!({
        "headBlockHash": head,
        "safeBlockHash": B256::ZERO,
        "finalizedBlockHash": B256::ZERO,
    });
    let mut params = ArrayParams::new();
    params
        .insert(state)
        .and_then(|()| params.insert(Value::Null))
        .map_err(|err| err.to_string())?;
    let response = engine
        .request::<Value, _>(&method, params)
        .await
        .map_err(|err| format!("{method} failed: {err}"))?;
    match response["payloadStatus"]["status"].as_str() {
        Some("VALID") => Ok(()),
        _ => Err(format!("{method} returned {response}, expected status VALID")),
    }
}
