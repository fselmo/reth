//! Test runner for blockchain tests in the engine format.
//!
//! Each fixture gets a fresh temporary database with its genesis, and the engine stack a node
//! builds on it: the engine tree with [`BasicEngineValidator`] and its persistence service, and
//! the [`EngineApi`] handler. The fixture's parameters go, unchanged, through the handler's
//! JSON-RPC method table for `engine_newPayloadV<n>` and `engine_forkchoiceUpdatedV<n>` at the
//! versions the fixture names, so the handler's own parameter decoding and version checks run.
//! No node, RPC server or network is started.

use crate::{
    models::{EngineNewPayload, EngineTest},
    result::{find_json_files, FixtureResult},
    Error,
};
use alloy_primitives::B256;
use alloy_rpc_types_engine::{ClientCode, ClientVersionV1};
use futures::future::poll_fn;
use jsonrpsee::RpcModule;
use reth_chainspec::{ChainSpec, EthChainSpec};
use reth_db::{
    init_db,
    mdbx::{DatabaseArguments, SyncMode},
    DatabaseEnv,
};
use reth_db_common::init::init_genesis;
use reth_engine_primitives::{ConsensusEngineHandle, NoopInvalidBlockHook};
use reth_engine_tree::{
    chain::{ChainHandler, FromOrchestrator, HandlerEvent},
    download::BasicBlockDownloader,
    engine::{EngineApiKind, EngineApiRequestHandler, EngineHandler},
    persistence::PersistenceHandle,
    tree::{BasicEngineValidator, EngineApiTreeHandler, TreeConfig},
};
use reth_eth_wire_types::EthNetworkPrimitives;
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_ethereum_primitives::EthPrimitives;
use reth_evm_ethereum::EthEvmConfig;
use reth_network_api::noop::NoopNetwork;
use reth_network_p2p::full_block::NoopFullBlockClient;
use reth_node_core::args::EngineArgs;
use reth_node_ethereum::{EthEngineTypes, EthereumEngineValidator, EthereumNode};
use reth_node_types::NodeTypesWithDBAdapter;
use reth_payload_builder::{PayloadBuilderHandle, PayloadStore};
use reth_provider::{
    providers::{BlockchainProvider, RocksDBBuilder, StaticFileProvider},
    BlockNumReader, ProviderFactory,
};
use reth_prune::PrunerBuilder;
use reth_rpc_api::EngineApiServer;
use reth_rpc_engine_api::{EngineApi, EngineCapabilities};
use reth_storage_overlay::OverlayManager;
use reth_tasks::Runtime;
use reth_transaction_pool::noop::NoopTransactionPool;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// The cross-block cache size of each fixture's engine, in MB (`--engine.cross-block-cache-size`).
const CROSS_BLOCK_CACHE_SIZE_MB: usize = 1;

/// The engine tree's persistence threshold (`--engine.persistence-threshold`): more blocks than a
/// fixture has, so the tree persists them only when it terminates.
const PERSISTENCE_THRESHOLD: u64 = 1_000_000;

/// How long a fixture's engine tree gets to persist its blocks and stop.
const ENGINE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Options for running engine tests.
#[derive(Debug, Clone)]
pub struct EngineTestOptions {
    /// The node's `--engine.disable-bal-parallel-execution`.
    pub disable_bal_parallel_execution: bool,
    /// How many fixtures run at once.
    pub workers: usize,
    /// The directory in which each fixture's temporary datadir is created, e.g. a tmpfs.
    pub datadir_root: PathBuf,
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
        let engine_args = EngineArgs {
            bal_parallel_execution_disabled: options.disable_bal_parallel_execution,
            // The default 4 GB cache would dominate the memory of concurrent workers.
            cross_block_cache_size: CROSS_BLOCK_CACHE_SIZE_MB,
            // Keep a fixture's blocks in memory instead of persisting them while it runs.
            persistence_threshold: PERSISTENCE_THRESHOLD,
            ..Default::default()
        };
        engine_args.validate().expect("the runner's engine arguments are valid");
        let tree_config = engine_args.tree_config();
        let files = Mutex::new(find_json_files(&self.suite_path).into_iter());
        std::thread::scope(|scope| {
            for _ in 0..options.workers.max(1) {
                scope.spawn(|| {
                    let mut cleanup = Cleanup::default();
                    while let Some(path) = files.lock().unwrap().next() {
                        let tests = match load(&path) {
                            Ok(tests) => tests,
                            Err(err) => {
                                on_result(FixtureResult::load_failed(&path, err));
                                continue
                            }
                        };
                        for (name, test) in tests {
                            let (result, remains) =
                                run_fixture(name, &test, &tree_config, &options.datadir_root);
                            on_result(result);
                            cleanup.remains.push(remains);
                            cleanup.sweep();
                        }
                    }
                    cleanup.finish();
                });
            }
        });
    }
}

fn load(path: &Path) -> Result<BTreeMap<String, EngineTest>, Error> {
    let contents =
        fs::read_to_string(path).map_err(|error| Error::Io { path: path.into(), error })?;
    serde_json::from_str(&contents)
        .map_err(|error| Error::CouldNotDeserialize { path: path.into(), error })
}

/// What the engine ended with, reported beside the verdict.
#[derive(Debug, Default)]
struct Outcome {
    last_block_hash: Option<B256>,
    last_payload_status: Option<String>,
}

/// What a fixture leaves behind: its runtime, on which the engine's background tasks can run
/// for seconds after the tree stopped, and its datadir.
struct Remains {
    runtime: Runtime,
    db: Option<Arc<DatabaseEnv>>,
    datadir: Option<PathBuf>,
}

impl Remains {
    /// Whether the background tasks released the fixture's database.
    fn is_released(&self) -> bool {
        self.db.as_ref().is_none_or(|db| Arc::strong_count(db) == 1)
    }

    /// Drops the runtime, joining its threads, and tries to remove the datadir. Returns the
    /// datadir if a task still writing to it made the removal fail.
    fn clear(self) -> Option<PathBuf> {
        let Self { runtime, db, datadir } = self;
        drop(db);
        drop(runtime);
        datadir.filter(|datadir| fs::remove_dir_all(datadir).is_err())
    }
}

/// What a worker still has to clean up.
#[derive(Default)]
struct Cleanup {
    remains: Vec<Remains>,
    datadirs: Vec<PathBuf>,
}

impl Cleanup {
    /// Clears the remains whose database was released and retries the datadirs that could not be
    /// removed yet.
    fn sweep(&mut self) {
        let (released, pending): (Vec<_>, Vec<_>) =
            std::mem::take(&mut self.remains).into_iter().partition(Remains::is_released);
        self.remains = pending;
        self.datadirs.retain(|datadir| fs::remove_dir_all(datadir).is_err());
        self.datadirs.extend(released.into_iter().filter_map(Remains::clear));
    }

    /// Sweeps until everything is cleaned up or [`ENGINE_SHUTDOWN_TIMEOUT`] passes, then clears
    /// what is left and reports the datadirs that could not be removed.
    fn finish(mut self) {
        let deadline = std::time::Instant::now() + ENGINE_SHUTDOWN_TIMEOUT;
        self.sweep();
        while !(self.remains.is_empty() && self.datadirs.is_empty()) &&
            std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
            self.sweep();
        }
        self.datadirs.extend(self.remains.into_iter().filter_map(Remains::clear));
        for datadir in self.datadirs {
            if let Err(err) = fs::remove_dir_all(&datadir) {
                eprintln!("failed to remove {}: {err}", datadir.display());
            }
        }
    }
}

/// Runs one fixture against a fresh engine on a runtime of its own, and returns what it leaves
/// behind.
///
/// The runtime is per fixture because the engine's background tasks can outlive a fixture's
/// tree by seconds, and on a runtime shared with the next fixtures they slowed those by seconds.
/// The runtime's async tasks stop with the fixture; it is dropped, and the datadir removed, once
/// the remaining tasks released the database, from the worker thread rather than from one of
/// the runtime's own threads, which could not join itself.
fn run_fixture(
    name: String,
    test: &EngineTest,
    tree_config: &TreeConfig,
    datadir_root: &Path,
) -> (FixtureResult, Remains) {
    let fork = format!("{:?}", test.network);
    let mut outcome = Outcome::default();
    let runtime = Runtime::test();
    let mut remains = Remains { runtime: runtime.clone(), db: None, datadir: None };
    let result = runtime.handle().block_on(run_case(
        test,
        tree_config,
        datadir_root,
        &runtime,
        &mut outcome,
        &mut remains,
    ));
    runtime.shutdown_timeout(Duration::ZERO);
    let mut result = FixtureResult::new(name, fork, result);
    result.last_block_hash = outcome.last_block_hash;
    result.last_payload_status = outcome.last_payload_status;
    (result, remains)
}

type FixtureNode = NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>;

async fn run_case(
    test: &EngineTest,
    tree_config: &TreeConfig,
    datadir_root: &Path,
    runtime: &Runtime,
    outcome: &mut Outcome,
    remains: &mut Remains,
) -> Result<(), Error> {
    let chain_spec = Arc::new(test.chain_spec());
    let genesis_hash = chain_spec.genesis_hash();
    if genesis_hash != test.genesis_block_header.hash {
        return Err(Error::Assertion(format!(
            "genesis hash mismatch: the chain spec has {genesis_hash}, the fixture {}",
            test.genesis_block_header.hash
        )))
    }

    let datadir = tempfile::Builder::new()
        .prefix("reth-enginetest-")
        .tempdir_in(datadir_root)
        .map_err(|err| Error::Assertion(format!("failed to create a datadir: {err}")))?
        .keep();
    remains.datadir = Some(datadir.clone());
    let engine = Engine::start(chain_spec, &datadir, tree_config, runtime)?;
    let result = drive(&engine.rpc, test, outcome).await;
    outcome.last_block_hash = engine.provider.chain_info().ok().map(|info| info.best_hash);
    remains.db = Some(engine.stop().await);
    result?;

    match outcome.last_block_hash {
        Some(hash) if hash == test.lastblockhash => Ok(()),
        hash => Err(Error::Assertion(format!(
            "last block hash mismatch: got {hash:?}, expected {}",
            test.lastblockhash
        ))),
    }
}

/// The engine stack of one fixture, wired as the node's engine launcher wires it,
/// without the network, the RPC servers, the transaction pool or the payload builder, which the
/// fixtures do not reach.
struct Engine {
    /// The `engine_` JSON-RPC methods of the [`EngineApi`] handler.
    rpc: RpcModule<()>,
    provider: BlockchainProvider<FixtureNode>,
    db: Arc<DatabaseEnv>,
    handler: tokio::task::JoinHandle<()>,
    to_handler: tokio::sync::mpsc::UnboundedSender<FromOrchestrator>,
}

impl Engine {
    fn start(
        chain_spec: Arc<ChainSpec>,
        datadir: &Path,
        tree_config: &TreeConfig,
        runtime: &Runtime,
    ) -> Result<Self, Error> {
        let setup_err = |err: &dyn std::fmt::Display| {
            Error::Assertion(format!("failed to set up the engine: {err}"))
        };
        let db = Arc::new(
            init_db(
                datadir.join("db"),
                // The database is thrown away after the fixture, so skip MDBX's durable syncs
                // (`--db.sync-mode safe-no-sync`).
                DatabaseArguments::test().with_sync_mode(Some(SyncMode::SafeNoSync)),
            )
            .map_err(|e| setup_err(&e))?,
        );
        let overlay_manager = OverlayManager::new(runtime.state_trie_overlay_worker_pool());
        let factory = ProviderFactory::<FixtureNode>::new(
            db.clone(),
            chain_spec.clone(),
            StaticFileProvider::read_write(datadir.join("static_files"))
                .map_err(|e| setup_err(&e))?,
            RocksDBBuilder::new(datadir.join("rocksdb"))
                .with_default_tables()
                .build()
                .map_err(|e| setup_err(&e))?,
            runtime.clone(),
        )
        .map_err(|e| setup_err(&e))?
        .with_overlay_manager(overlay_manager.clone());
        init_genesis(&factory).map_err(|e| setup_err(&e))?;
        let provider = BlockchainProvider::new(factory.clone()).map_err(|e| setup_err(&e))?;

        let consensus = Arc::new(EthBeaconConsensus::new(chain_spec.clone()));
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let validator = BasicEngineValidator::new(
            provider.clone(),
            consensus.clone(),
            evm_config.clone(),
            EthereumEngineValidator::new(chain_spec.clone()),
            tree_config.clone(),
            Box::new(NoopInvalidBlockHook::default()),
            overlay_manager.clone(),
            runtime.clone(),
        );
        let (sync_metrics_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let persistence = PersistenceHandle::<EthPrimitives>::spawn_service(
            factory.clone(),
            PrunerBuilder::default().build_with_provider_factory(factory),
            sync_metrics_tx,
        );
        let payload_builder = PayloadBuilderHandle::<EthEngineTypes>::noop();
        let (to_tree, from_tree) = EngineApiTreeHandler::spawn_new(
            provider.clone(),
            consensus.clone(),
            validator,
            persistence,
            payload_builder.clone(),
            provider.canonical_in_memory_state(),
            overlay_manager,
            tree_config.clone(),
            EngineApiKind::Ethereum,
            evm_config,
            runtime.clone(),
        );

        // The node polls this handler from its chain orchestrator, which also runs backfill sync;
        // fixtures never trigger backfill.
        let (to_engine, from_api) = tokio::sync::mpsc::unbounded_channel();
        let mut handler = EngineHandler::new(
            EngineApiRequestHandler::new(to_tree, from_tree),
            BasicBlockDownloader::new(
                NoopFullBlockClient::<EthNetworkPrimitives>::default(),
                consensus,
            ),
            UnboundedReceiverStream::new(from_api),
        );
        let (to_handler, mut from_runner) = tokio::sync::mpsc::unbounded_channel();
        // Events for the chain orchestrator (canonical chain updates, backfill requests) have no
        // consumer here and are dropped.
        let handler = runtime.handle().spawn(poll_fn(move |cx| {
            while let Poll::Ready(Some(event)) = from_runner.poll_recv(cx) {
                handler.on_event(event);
            }
            while let Poll::Ready(event) = handler.poll(cx) {
                if matches!(event, HandlerEvent::FatalError) {
                    return Poll::Ready(())
                }
            }
            Poll::Pending
        }));

        let api = EngineApi::new(
            provider.clone(),
            chain_spec.clone(),
            ConsensusEngineHandle::new(to_engine),
            PayloadStore::new(payload_builder),
            NoopTransactionPool::default(),
            runtime.clone(),
            ClientVersionV1 {
                code: ClientCode::RH,
                name: "reth".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                commit: String::new(),
            },
            EngineCapabilities::default(),
            EthereumEngineValidator::new(chain_spec),
            false,
            NoopNetwork::default(),
        );

        Ok(Self { rpc: api.into_rpc().remove_context(), provider, db, handler, to_handler })
    }

    /// Asks the engine tree to persist its blocks and stop, as a node does on shutdown, and
    /// returns the database, which background tasks may still hold.
    async fn stop(self) -> Arc<DatabaseEnv> {
        let Self { rpc, provider, db, handler, to_handler } = self;
        drop(rpc);
        let (tx, rx) = tokio::sync::oneshot::channel();
        if to_handler.send(FromOrchestrator::Terminate { tx }).is_ok() {
            let _ = tokio::time::timeout(ENGINE_SHUTDOWN_TIMEOUT, rx).await;
        }
        handler.abort();
        let _ = handler.await;
        drop(provider);
        db
    }
}

/// Sends the fixture's payloads in order, each valid one followed by a forkchoice update that
/// makes it the head, as a consensus client would.
async fn drive(rpc: &RpcModule<()>, test: &EngineTest, outcome: &mut Outcome) -> Result<(), Error> {
    let Some(first) = test.engine_new_payloads.first() else { return Ok(()) };
    forkchoice_updated(rpc, &first.forkchoice_updated_version, test.genesis_block_header.hash)
        .await
        .map_err(|err| Error::Assertion(format!("forkchoice update to genesis: {err}")))?;

    for (idx, payload) in test.engine_new_payloads.iter().enumerate() {
        new_payload(rpc, payload, outcome)
            .await
            .map_err(|err| Error::Assertion(format!("payload {idx}: {err}")))?;
        if payload.validation_error.is_none() && payload.error_code.is_none() {
            let head = payload.params[0]["blockHash"].as_str().unwrap_or_default();
            let head = head.parse().map_err(|err| {
                Error::Assertion(format!("payload {idx}: invalid blockHash {head:?}: {err}"))
            })?;
            forkchoice_updated(rpc, &payload.forkchoice_updated_version, head)
                .await
                .map_err(|err| Error::Assertion(format!("payload {idx}: {err}")))?;
        }
    }
    Ok(())
}

/// Calls a method of the handler with raw JSON parameters, as a JSON-RPC request would, and
/// returns its result or its error code and message.
async fn call(rpc: &RpcModule<()>, method: &str, params: Value) -> Result<Value, (i64, String)> {
    let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let (response, _) = rpc
        .raw_json_request(&request.to_string(), 1)
        .await
        .map_err(|err| (0, format!("invalid request: {err}")))?;
    let mut response: Value =
        serde_json::from_str(response.get()).map_err(|err| (0, err.to_string()))?;
    if let Some(error) = response.get("error") {
        return Err((
            error["code"].as_i64().unwrap_or_default(),
            error["message"].as_str().unwrap_or_default().to_string(),
        ))
    }
    Ok(response["result"].take())
}

/// Calls `engine_newPayloadV<n>` and checks the response against the fixture's expectation.
async fn new_payload(
    rpc: &RpcModule<()>,
    payload: &EngineNewPayload,
    outcome: &mut Outcome,
) -> Result<(), String> {
    let method = format!("engine_newPayloadV{}", payload.new_payload_version);
    let expected_code = payload
        .error_code
        .as_deref()
        .map(str::parse::<i64>)
        .transpose()
        .map_err(|err| format!("invalid errorCode: {err}"))?;

    match call(rpc, &method, Value::from(payload.params.clone())).await {
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
        Err((code, message)) => {
            outcome.last_payload_status = Some(format!("error {code}"));
            if expected_code == Some(code) {
                Ok(())
            } else {
                Err(format!("{method} failed with error {code}: {message}"))
            }
        }
    }
}

/// Calls `engine_forkchoiceUpdatedV<n>` without payload attributes and requires a valid head.
async fn forkchoice_updated(rpc: &RpcModule<()>, version: &str, head: B256) -> Result<(), String> {
    let method = format!("engine_forkchoiceUpdatedV{version}");
    let state = json!({
        "headBlockHash": head,
        "safeBlockHash": B256::ZERO,
        "finalizedBlockHash": B256::ZERO,
    });
    let response = call(rpc, &method, json!([state, null]))
        .await
        .map_err(|(code, message)| format!("{method} failed with error {code}: {message}"))?;
    match response["payloadStatus"]["status"].as_str() {
        Some("VALID") => Ok(()),
        _ => Err(format!("{method} returned {response}, expected status VALID")),
    }
}
