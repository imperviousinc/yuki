use std::{ops::DerefMut, sync::Arc, time::Duration};
use std::collections::{BTreeMap};
use futures_util::stream::StreamExt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::{anyhow, Context};
use bitcoin::{block::Header, p2p::{
    message_filter::{CFHeaders, CFilter},
    message_network::VersionMessage,
}, Block, BlockHash, Network, ScriptBuf, Transaction, Txid};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::{mpsc::Receiver, Mutex, RwLock};
use tokio::{
    sync::mpsc::{self},
};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use crate::{chain::{
    chain::Chain,
    checkpoints::{HeaderCheckpoint, HeaderCheckpoints},
    error::HeaderSyncError,
    HeightMonitor,
}, core::{error::FetchHeaderError, peer_map::PeerMap}, db::traits::{HeaderStore, PeerStore}, filters::{cfheader_chain::AppendAttempt}, BlockchainInfo, RejectPayload, TxBroadcastPolicy};
use crate::chain::block_queue::DownloadRequest;
use crate::chain::chain::AdvanceKind;
use crate::core::error::{DownloadRequestError};
use crate::core::peer_map::DownloadResponseKind;
use crate::db::sqlite::blocks::BlocksStore;
use crate::db::sqlite::filters::FiltersStore;
use crate::rpc::server::MempoolEntryResult;
use super::{
    broadcaster::Broadcaster,
    channel_messages::{
        CombinedAddr, GetHeaderConfig, MainThreadMessage, PeerMessage,
        PeerThreadMessage,
    },
    client::Client,
    config::NodeConfig,
    dialog::Dialog,
    error::NodeError,
    messages::{ClientMessage, Event, Log, SyncUpdate, Warning},
    FilterSyncPolicy, LastBlockMonitor, PeerId, PeerTimeoutConfig,
};

pub(crate) const WTXID_VERSION: u32 = 70016;
const LOOP_TIMEOUT: u64 = 1;

const CHECK_PEERS_MEMPOOL_INTERVAL: Duration = Duration::from_secs(28);
const LAST_SEEN_IN_MEMPOOL_TIMEOUT: Duration = Duration::from_secs(10 * 60);


type PeerRequirement = usize;

/// The state of the node with respect to connected peers.
#[derive(Debug, Clone, Copy)]
pub enum NodeState {
    /// We are behind on block headers according to our peers.
    Behind,
    /// We may start downloading compact block filter headers.
    HeadersSynced,
    /// We may start scanning compact block filters.
    FilterHeadersSynced,
}


/// A compact block filter node. Nodes download Bitcoin block headers, block filters, and blocks to send relevant events to a client.
#[derive(Debug, Clone)]
pub struct Node<H: HeaderStore, P: PeerStore, B: BlocksStore + 'static, F: FiltersStore + 'static> {
    state: Arc<RwLock<NodeState>>,
    chain: Arc<Mutex<Chain<H, B, F>>>,
    peer_map: Arc<Mutex<PeerMap<P>>>,
    tx_broadcaster: Arc<Mutex<Broadcaster>>,
    // A partial mempool of transactions we have broadcasted
    mempool: Arc<Mutex<MempoolStore>>,
    required_peers: PeerRequirement,
    dialog: Arc<Dialog>,
    client_recv: Arc<Mutex<Receiver<ClientMessage>>>,
    peer_recv: Arc<Mutex<Receiver<PeerThreadMessage>>>,
    filter_sync_policy: Arc<RwLock<FilterSyncPolicy>>,
    queued_blocks: Arc<Mutex<QueueBlocksStatus>>,
    checkpoint: HeaderCheckpoint,
    external_filter_endpoint: Option<String>,
    filter_download: Arc<Mutex<Option<ExternalFilterLoader>>>,
    filter_download_progress: Arc<Mutex<Option<f32>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolStore {
    txs: Vec<MempoolTransaction>,
    file_path: PathBuf,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
// status for on demand blocks
pub struct QueueBlocksStatus {
    pending: u32,
    completed: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolTransaction {
    tx: Transaction,
    txid: Txid,
    #[serde(serialize_with = "serialize_instant", deserialize_with = "deserialize_instant")]
    last_seen: Option<Instant>,
    #[serde(serialize_with = "serialize_instant", deserialize_with = "deserialize_instant")]
    broadcast: Option<Instant>,
    #[serde(serialize_with = "serialize_instant", deserialize_with = "deserialize_instant")]
    last_check: Option<Instant>,
    time: u64,
}

impl<H: HeaderStore + 'static, P: PeerStore + 'static, B: BlocksStore + 'static + Send, F: FiltersStore + 'static> Node<H, P, B, F> {
    pub(crate) fn new(
        network: Network,
        config: NodeConfig,
        peer_store: P,
        header_store: H,
        block_store: B,
        filter_store: F,
        mempool_store: MempoolStore,
    ) -> (Self, Client) {
        let NodeConfig {
            required_peers,
            white_list,
            dns_resolver,
            addresses,
            data_path: _,
            header_checkpoint,
            connection_type,
            target_peer_size,
            response_timeout,
            max_connection_time,
            filter_sync_policy,
            external_filter_endpoint,
            log_level,
            prune_point,
            cf_headers_path
        } = config;

        let timeout_config = PeerTimeoutConfig::new(response_timeout, max_connection_time);
        // Set up a communication channel between the node and client
        let (log_tx, log_rx) = mpsc::channel::<Log>(32);
        let (warn_tx, warn_rx) = mpsc::unbounded_channel::<Warning>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<Event>();
        let (ctx, crx) = mpsc::channel::<ClientMessage>(5);
        let client = Client::new(log_rx, warn_rx, event_rx, ctx);
        // A structured way to talk to the client
        let dialog = Arc::new(Dialog::new(log_level, log_tx, warn_tx, event_tx));
        // We always assume we are behind
        let state = Arc::new(RwLock::new(NodeState::Behind));
        // Configure the peer manager
        let (mtx, mrx) = mpsc::channel::<PeerThreadMessage>(32);
        let height_monitor = Arc::new(Mutex::new(HeightMonitor::new()));
        let peer_map = Arc::new(Mutex::new(PeerMap::new(
            mtx, network, peer_store, white_list,
            Arc::clone(&dialog), connection_type, target_peer_size,
            timeout_config, Arc::clone(&height_monitor), dns_resolver,
        )));
        // Set up the transaction broadcaster
        let tx_broadcaster = Arc::new(Mutex::new(Broadcaster::new()));
        // Prepare the header checkpoints for the chain source
        let mut checkpoints = HeaderCheckpoints::new(&network);
        let checkpoint = header_checkpoint.unwrap_or_else(|| checkpoints.last());
        checkpoints.prune_up_to(checkpoint);

        let mut filter_loader = None;
        if let Some(external) = external_filter_endpoint.clone() {
            if let Some(filters_path) = filter_store.file_path() {
                if let Some(prune_point) = prune_point.clone() {
                    filter_loader = Some(ExternalFilterLoader {
                        progress: None,
                        status: FilterLoadStatus::Idle,
                        endpoint_url: external,
                        filter_headers_path: cf_headers_path.clone(),
                        filters_path,
                        header_checkpoint: checkpoint.clone(),
                        prune_point,
                        attempts: 0,
                        last_attempt: Instant::now(),
                    })
                }
            }
        }

        // Build the chain
        let chain = Chain::new(
            network,
            addresses,
            checkpoint,
            checkpoints,
            Arc::clone(&dialog),
            height_monitor,
            header_store,
            block_store,
            filter_store,
            cf_headers_path,
            required_peers.into(),
            prune_point,
        );
        let chain = Arc::new(Mutex::new(chain));
        (
            Self {
                state,
                chain,
                peer_map,
                tx_broadcaster,
                mempool: Arc::new(Mutex::new(mempool_store)),
                required_peers: required_peers.into(),
                dialog,
                client_recv: Arc::new(Mutex::new(crx)),
                peer_recv: Arc::new(Mutex::new(mrx)),
                filter_sync_policy: Arc::new(RwLock::new(filter_sync_policy)),
                queued_blocks: Arc::new(Mutex::new(QueueBlocksStatus { pending: 0, completed: 0 })),
                checkpoint,
                external_filter_endpoint,
                filter_download: Arc::new(Mutex::new(filter_loader)),
                filter_download_progress: Arc::new(Mutex::new(None)),
            },
            client,
        )
    }

    async fn maybe_download_filters(&self) {
        let mut loader = if let Ok(lock) =
            self.filter_download.try_lock() {
            lock
        } else {
            return;
        };
        if !matches!(&*self.filter_sync_policy.read().await , FilterSyncPolicy::External) {
            return;
        }
        let filters_synced = self.chain.lock().await.filters_synced().await;
        if filters_synced {
            return;
        }

        let loader_ref = match loader.as_mut() {
            Some(loader) => loader,
            None => return,
        };

        if matches!(loader_ref.status, FilterLoadStatus::Loaded) {
            // Complete syncing
            match self.chain.lock().await.reload_filters().await {
                Ok(_) => {}
                Err(e) => {
                    self.dialog.send_dialog(
                        format!("Received bad filters from external source, switching to P2P: {}", e))
                        .await;
                    let mut policy = self.filter_sync_policy.write().await;
                    *policy = FilterSyncPolicy::P2P;
                }
            }
            return;
        }

        drop(loader);

        let downloader = self.filter_download.clone();
        let sync_policy = self.filter_sync_policy.clone();
        let download_progress = self.filter_download_progress.clone();
        let dialog = self.dialog.clone();
        tokio::spawn(async move {
            let mut lock = match downloader.try_lock() {
                Ok(lock) => lock,
                Err(_) => return
            };
            let loader = match lock.as_mut() {
                None => return,
                Some(loader) => loader,
            };

            match loader.sync_next(&dialog, download_progress).await {
                Ok(_) => {}
                Err(e) => {
                    dialog.send_dialog(
                        format!("Could not do external filter sync, switching to P2P: {}", e))
                        .await;
                    let mut policy = sync_policy.write().await;
                    *policy = FilterSyncPolicy::P2P;
                }
            }
        });
    }

    /// Run the node continuously. Typically run on a separate thread than the underlying application.
    ///
    /// # Errors
    ///
    /// A node will cease running if a fatal error is encountered with either the [`PeerStore`] or [`HeaderStore`].
    pub async fn run(self: Arc<Self>) -> Result<(), NodeError<H::Error, P::Error>> {
        crate::log!(self.dialog, "Starting node");
        crate::log!(
        self.dialog,
        format!(
            "Configured connection requirement: {} peers",
            self.required_peers
        )
    );
        self.fetch_headers().await?;
        let mut last_block = LastBlockMonitor::new();
        let mut peer_recv = self.peer_recv.lock().await;

        let shutdown_token = CancellationToken::new();
        let shutdown_token_client = shutdown_token.clone();
        let shutdown_token_node = shutdown_token.clone();

        let that = self.clone();

        tokio::task::spawn(async move {
            _ = that.client_messages_task(shutdown_token_client).await;
        });

        loop {
            if shutdown_token_node.is_cancelled() {
                return Ok(());
            }

            self.advance_state(&mut last_block).await;
            self.dispatch().await?;
            self.request_next_block_batch().await;
            self.broadcast_transactions().await;
            self.maybe_download_filters().await;

            let peer = tokio::time::timeout(
                Duration::from_secs(LOOP_TIMEOUT), peer_recv.recv(),
            ).await;
            match peer {
                Ok(Some(peer_thread)) => {
                    match peer_thread.message {
                        PeerMessage::Version(version) => {
                            {
                                let mut peer_map = self.peer_map.lock().await;
                                peer_map.set_offset(peer_thread.nonce, version.timestamp);
                                peer_map.set_services(peer_thread.nonce, version.services);
                                peer_map.set_height(peer_thread.nonce, version.start_height as u32).await;
                            }
                            let response = self.handle_version(peer_thread.nonce, version).await?;
                            self.send_message(peer_thread.nonce, response).await;
                            crate::log!(self.dialog, format!("[{}]: version", peer_thread.nonce));
                        }
                        PeerMessage::Addr(addresses) => self.handle_new_addrs(addresses).await,
                        PeerMessage::Headers(headers) => {
                            last_block.reset();
                            crate::log!(self.dialog, format!("[{}]: headers", peer_thread.nonce));
                            match self.handle_headers(peer_thread.nonce, headers).await {
                                Some(response) => {
                                    self.send_message(peer_thread.nonce, response).await;
                                }
                                None => continue,
                            }
                        }
                        PeerMessage::FilterHeaders(cf_headers) => {
                            crate::log!(self.dialog, format!("[{}]: filter headers", peer_thread.nonce));
                            match self.handle_cf_headers(peer_thread.nonce, cf_headers).await {
                                Some(response) => {
                                    self.broadcast(response).await;
                                }
                                None => continue,
                            }
                        }
                        PeerMessage::Filter(filter) => {
                            crate::log!(self.dialog, format!("{} sent filter {}", peer_thread.nonce, filter.block_hash));

                            match self.handle_filter(peer_thread.nonce, filter).await {
                                Some(response) => {
                                    self.send_message(peer_thread.nonce, response).await;
                                }
                                None => continue,
                            }
                        }
                        PeerMessage::Tx(tx) => {
                            let txid = tx.compute_txid();
                            crate::log!(self.dialog, format!("{} sent tx {}", peer_thread.nonce, txid));
                            // Refresh the last seen to keep them in our mempool
                            if let Err(e) = self.mempool.lock().await.refresh(txid).await {
                                self.dialog
                                    .send_dialog(format!("Could not refresh mempool tx {}: {}", txid, e))
                                    .await;
                            }
                            continue;
                        }
                        PeerMessage::Block(block) => {
                            match self.handle_block(peer_thread.nonce, block).await {
                                Some(response) => {
                                    self.send_message(peer_thread.nonce, response).await;
                                }
                                None => {
                                    continue;
                                }
                            }
                        }
                        PeerMessage::NewBlocks(blocks) => {
                            crate::log!(self.dialog, format!("[{}]: inv", peer_thread.nonce));
                            match self.handle_inventory_blocks(peer_thread.nonce, blocks).await {
                                Some(response) => {
                                    self.broadcast(response).await;
                                }
                                None => continue,
                            }
                        }
                        PeerMessage::Reject(payload) => {
                            self.dialog
                                .send_warning(Warning::TransactionRejected { payload });
                        }
                        PeerMessage::FeeFilter(feerate) => {
                            let mut peer_map = self.peer_map.lock().await;
                            peer_map.set_broadcast_min(peer_thread.nonce, feerate);
                        }
                        _ => continue,
                    }
                }
                _ => continue,
            }
        }
    }

    async fn client_messages_task(&self, cancellation_token: CancellationToken) -> Result<(), NodeError<H::Error, P::Error>> {
        'main: loop {
            if cancellation_token.is_cancelled() {
                break;
            }

            let mut client_recv = self.client_recv.lock().await;
            while let Some(message) = client_recv.recv().await {
                if cancellation_token.is_cancelled() {
                    break;
                }

                match message {
                    ClientMessage::Shutdown => {
                        cancellation_token.cancel();
                        return Ok(());
                    }
                    ClientMessage::Broadcast(broadcast) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards");
                        // Accept tx in our mini mempool, and then we observe
                        // if other peers retain it or drop it.
                        let txid = broadcast.tx.compute_txid();
                        if let Err(e) = self.mempool.lock().await.insert(MempoolTransaction {
                            tx: broadcast.tx.clone(),
                            txid,
                            last_seen: None,
                            broadcast: None,
                            last_check: None,
                            time: now.as_secs(),
                        }).await {
                            self.dialog.send_dialog(
                                format!("Could not insert tx {} to mempool: {}", txid, e))
                                .await;
                        } else {
                            self.tx_broadcaster.lock().await.add(broadcast)
                        }
                    },
                    ClientMessage::AddScript(script) => self.add_script(script).await,
                    ClientMessage::Rescan => {
                        if let Some(response) = self.rescan().await {
                            self.broadcast(response).await;
                        }
                    }
                    ClientMessage::ContinueDownload => {
                        if let Some(response) = self.start_filter_download().await {
                            self.broadcast(response).await
                        }
                    }
                    ClientMessage::GetBlock(reqs) => {
                        let chain_clone = self.chain.clone();
                        tokio::spawn(async move {
                            for req in reqs {
                                let block = chain_clone.lock().await
                                    .get_block(&req.hash).await;
                                _ = req.oneshot.send(match block {
                                    Ok(Some(b)) => Ok(b),
                                    Ok(None) => Err(DownloadRequestError::UnknownHash),
                                    _ => Err(DownloadRequestError::RecvError)
                                });
                            }
                        });
                    }
                    ClientMessage::SetDuration(duration) => {
                        let mut peer_map = self.peer_map.lock().await;
                        peer_map.set_duration(duration);
                    }
                    ClientMessage::AddPeer(peer) => {
                        let mut peer_map = self.peer_map.lock().await;
                        peer_map.add_trusted_peer(peer);
                    }
                    ClientMessage::GetHeader(request) => {
                        let mut chain = self.chain.lock().await;
                        let header_opt = chain.fetch_header(request.height)
                            .await
                            .map_err(|e| FetchHeaderError::DatabaseOptFailed { error: e.to_string() })
                            .and_then(|opt| opt.ok_or(FetchHeaderError::UnknownHeight(request.height)));
                        let send_result = request.oneshot.send(header_opt);
                        if send_result.is_err() {
                            self.dialog.send_warning(Warning::ChannelDropped);
                        };
                    }
                    ClientMessage::GetHeaderByHash(request) => {
                        let mut chain = self.chain.lock().await;
                        if let Some(height) = chain.height_of_hash(request.hash).await {
                            let header_opt = chain.fetch_header(height)
                                .await
                                .map_err(|e| FetchHeaderError::DatabaseOptFailed { error: e.to_string() })
                                .and_then(|opt| opt.ok_or(FetchHeaderError::UnknownHeight(height)));
                            let send_result = request.oneshot.send(header_opt);
                            if send_result.is_err() {
                                self.dialog.send_warning(Warning::ChannelDropped);
                            };
                        } else {
                            _ = request.oneshot.send(Err(FetchHeaderError::DatabaseOptFailed {
                                error: format!("no header with hash {} found", request.hash)
                            }));
                        }
                    }
                    ClientMessage::GetHeaderBatch(request) => {
                        let chain = self.chain.lock().await;
                        let range_opt = chain.fetch_header_range(request.range)
                            .await
                            .map_err(|e| FetchHeaderError::DatabaseOptFailed { error: e.to_string() });
                        let send_result = request.oneshot.send(range_opt);
                        if send_result.is_err() {
                            self.dialog.send_warning(Warning::ChannelDropped);
                        };
                    }
                    ClientMessage::GetBlockchainInfo(request) => {
                        _ = request.oneshot.send(self.fetch_blockchain_info().await);
                    }
                    ClientMessage::GetBroadcastMinFeeRate(request) => {
                        let peer_map = self.peer_map.lock().await;
                        let fee_rate = peer_map.broadcast_min();
                        let send_result = request.send(fee_rate);
                        if send_result.is_err() {
                            self.dialog.send_warning(Warning::ChannelDropped);
                        }
                    }
                    ClientMessage::NoOp => (),
                    ClientMessage::GetMempoolEntry(request) => {
                        _ = request.oneshot
                            .send(self.mempool.lock().await.get_mempool_entry(request.txid));
                    }
                    ClientMessage::GetBlockFilterByHeight(bfr) => {
                        let result = self.chain.lock().await
                            .get_block_filter_by_height(bfr.height).await;
                        _ = bfr.oneshot.send(result
                            .map_err(|_| FetchHeaderError::DatabaseOptFailed { error: "no filter ".to_string() }));
                    }
                    ClientMessage::QueueBlocks(queue) => {
                        let mut result = self.chain.lock().await;
                        let mut headers = Vec::new();
                        let count = queue.blocks.len();
                        for height in queue.blocks {
                            match result.fetch_header(height).await {
                                Ok(Some(header)) => {
                                    headers.push((height, header));
                                }
                                Ok(None) => {
                                    _ = queue.oneshot.send(Err(anyhow!("No such header at height = {}", height)));
                                    continue 'main;
                                }
                                Err(err) => {
                                    _ = queue.oneshot.send(Err(anyhow!("Could not fetch header: {}", err.to_string())));
                                    continue 'main;
                                }
                            }
                        }

                        self.peer_map.lock().await
                            .queue_blocks(headers.into_iter().map(|(height, header)| {
                                DownloadRequest {
                                    hash: header.block_hash(),
                                    kind: AdvanceKind::Blocks,
                                    height,
                                    sender: None,
                                    downloading_since: None,
                                }
                            }).collect());

                        {
                            let mut status = self.queued_blocks.lock().await;
                            status.pending += count as u32;
                        }

                        self.dialog.send_dialog(format!("Queued {} blocks", count)).await;
                        _ = queue.oneshot.send(Ok(()));
                    }
                    ClientMessage::PruneBlockchain(request) => {
                        let result = self.chain.lock().await
                            .prune_up_to_height(request.height).await;
                        match &result {
                            Ok(deleted) => {
                                self.dialog.send_dialog(format!("Pruned {} blocks up to height {}", deleted, request.height)).await;
                            }
                            Err(e) => {
                                self.dialog.send_dialog(format!("Could not prune blocks: {}", e)).await;
                            }
                        }
                        _ = request.oneshot.send(result);
                    }
                }
            }
        }
        Ok(())
    }

    // Send a message to a specified peer
    async fn send_message(&self, nonce: PeerId, message: MainThreadMessage) {
        let mut peer_map = self.peer_map.lock().await;
        peer_map.send_message(nonce, message).await;
    }

    // Broadcast a messsage to all connected peers
    async fn broadcast(&self, message: MainThreadMessage) {
        let mut peer_map = self.peer_map.lock().await;
        peer_map.broadcast(message).await;
    }

    // Send a message to a random peer
    #[allow(dead_code)]
    async fn send_random(&self, message: MainThreadMessage) -> Option<PeerId> {
        let mut peer_map = self.peer_map.lock().await;
        peer_map.send_random(message).await
    }

    // Connect to a new peer if we are not connected to enough
    async fn dispatch(&self) -> Result<(), NodeError<H::Error, P::Error>> {
        let mut peer_map = self.peer_map.lock().await;
        peer_map.clean().await;

        let live = peer_map.live();
        let required = self.next_required_peers().await;
        // Find more peers when lower than the desired threshold.
        if live < required {
            self.dialog.send_warning(Warning::NeedConnections {
                connected: live,
                required,
            });
            let address = peer_map.next_peer().await?;
            if peer_map.dispatch(address).await.is_err() {
                self.dialog.send_warning(Warning::CouldNotConnect);
            }
        }
        Ok(())
    }

    async fn request_next_block_batch(&self) {
        let state = self.state.read().await;
        if matches!(*state, NodeState::Behind) {
            return;
        }

        let mut chain = self.chain.lock().await;
        let can_sync_filters = chain.can_sync_filters;

        // Once filter sync is initiated, it must be completed before syncing
        // blocks again.
        let blocks_synced = chain.blocks_synced().await;
        let filter_p2p = {
            if let Ok(policy) = self.filter_sync_policy.try_read() {
                matches!(*policy, FilterSyncPolicy::P2P)
            } else {
                false
            }
        };
        let filter_sync = (blocks_synced || can_sync_filters) &&
            !chain.filters_synced().await &&
            filter_p2p;
        chain.can_sync_filters = filter_sync;

        if self.peer_map.lock().await.reset_peers(filter_sync).await {
            _ = self.dispatch().await;
        }

        let to_queue =
            chain.advance_downloads_tip(filter_sync).await;

        drop(chain);

        match to_queue {
            Ok(queue) => {
                if !queue.is_empty() {
                    crate::log!(self.dialog, format!("Queueing {} items", queue.len()));
                    self.peer_map.lock().await.queue_blocks(queue);
                }
            }
            Err(err) => {
                crate::log!(self.dialog, format!("Could not advance tips: {}", err));
            }
        }

        self.peer_map.lock().await.request_next_download_batch().await;
    }

    // Broadcast transactions according to the configured policy
    async fn broadcast_transactions(&self) {
        let mut broadcaster = self.tx_broadcaster.lock().await;
        let mut peer_map = self.peer_map.lock().await;
        if peer_map.live().ge(&self.required_peers) {
            // Observe the transactions we have broadcasted by asking other peers
            // to see if they're still being held in their mempool.
            // This is to lossely detect if they have been replaced/or mined.
            // (We don't have a good view of the mempool otherwise)
            {
                let mempool_txs: Vec<_> = self.mempool
                    .lock().await.get_txids_to_check();
                if !mempool_txs.is_empty() {
                    // Ask peers for the transactions that were broadcasted to
                    // see if they're still in mempool.
                    _ = peer_map.broadcast(MainThreadMessage::GetTx(mempool_txs)).await;
                }
                // Clear all transactions that appear to be removed
                if let Err(e) = self.mempool.lock().await.remove_stale().await {
                    self.dialog
                        .send_dialog(format!("Could not remove stale txs from mempool: {}", e))
                        .await;
                }
            }

            for transaction in broadcaster.queue() {
                let txid = transaction.tx.compute_txid();
                let did_broadcast = match transaction.broadcast_policy {
                    TxBroadcastPolicy::AllPeers => {
                        crate::log!(
                            self.dialog,
                            format!("Sending transaction to {} connected peers", peer_map.live())
                        );
                        peer_map
                            .broadcast(MainThreadMessage::BroadcastTx(transaction.tx))
                            .await
                    }
                    TxBroadcastPolicy::RandomPeer => {
                        crate::log!(self.dialog, "Sending transaction to a random peer");
                        peer_map
                            .send_random(MainThreadMessage::BroadcastTx(transaction.tx))
                            .await.is_some()
                    }
                };

                if did_broadcast {
                    match self.mempool.lock().await.mark_broadcasted(txid).await {
                        Ok(true) => {},
                        Ok(false) => {
                            self.dialog.send_dialog(
                                format!("Expected tx {} to be in our mempool", txid)
                            ).await;
                        }
                        Err(e) => {
                            self.dialog.send_dialog(
                                format!("Could not remove tx {} from mempool: {}", txid, e)
                            ).await
                        }
                    }
                    self.dialog.send_info(Log::TxSent(txid)).await;
                } else {
                    // Tx wasn't broadcast remove it from our mempool
                    if let Err(e) = self.mempool.lock().await.remove(txid).await {
                        self.dialog.send_dialog(
                            format!("Could not remove tx {} from mempool: {}", txid, e)
                        ).await
                    }
                    self.dialog.send_warning(Warning::TransactionRejected {
                        payload: RejectPayload::from_txid(txid),
                    });
                }
            }
        }
    }

    // Try to continue with the syncing process
    async fn advance_state(&self, last_block: &mut LastBlockMonitor) {
        let mut state = self.state.write().await;
        match *state {
            NodeState::Behind => {
                let mut header_chain = self.chain.lock().await;
                if header_chain.is_synced().await {
                    header_chain.flush_to_disk().await;
                    self.dialog
                        .send_info(Log::StateChange(NodeState::HeadersSynced))
                        .await;
                    *state = NodeState::HeadersSynced;
                }
            }
            NodeState::HeadersSynced => {
                let header_chain = self.chain.lock().await;
                if header_chain.is_synced().await {
                    let update = SyncUpdate::new(
                        HeaderCheckpoint::new(header_chain.height(), header_chain.tip()),
                        header_chain.last_ten(),
                    );
                    self.dialog.send_event(Event::HeadersSynced(update));
                }

                if header_chain.is_cf_headers_synced() {
                    self.dialog
                        .send_info(Log::StateChange(NodeState::FilterHeadersSynced))
                        .await;
                    if let Err(e) = header_chain.save_cf_headers().await {
                        self.dialog
                            .send_dialog(format!("Could not save cf headers chain: {}", e))
                            .await;
                    }

                    *state = NodeState::FilterHeadersSynced;
                }
            }
            NodeState::FilterHeadersSynced => {}
        }

        if !matches!(*state, NodeState::Behind) {
            self.chain.lock().await.update_median_time().await;
            if last_block.stale() {
                self.dialog.send_warning(Warning::PotentialStaleTip);
                crate::log!(
                        self.dialog,
                        "Disconnecting from remote nodes to find new connections"
                    );
                self.broadcast(MainThreadMessage::Disconnect).await;
                last_block.reset();
            }
        }
    }

    // When syncing headers we are only interested in one peer to start
    async fn next_required_peers(&self) -> PeerRequirement {
        let state = self.state.read().await;
        match *state {
            NodeState::Behind => 1,
            _ => self.required_peers,
        }
    }

    // After we receiving some chain-syncing message, we decide what chain of data needs to be
    // requested next.
    async fn next_stateful_message(&self, chain: &mut Chain<H, B, F>) -> Option<MainThreadMessage> {
        if !chain.is_synced().await {
            let headers = GetHeaderConfig {
                locators: chain.locators().await,
                stop_hash: None,
            };
            return Some(MainThreadMessage::GetHeaders(headers));
        }

        if chain.can_sync_filters && !chain.is_cf_headers_synced() {
            return Some(MainThreadMessage::GetFilterHeaders(
                chain.next_cf_header_message().await,
            ));
        }
        None
    }

    // We accepted a handshake with a peer but we may disconnect if they do not support CBF
    async fn handle_version(
        &self,
        nonce: PeerId,
        version_message: VersionMessage,
    ) -> Result<MainThreadMessage, NodeError<H::Error, P::Error>> {
        if version_message.version < WTXID_VERSION {
            return Ok(MainThreadMessage::Disconnect);
        }
        let state = self.state.read().await;
        match *state {
            NodeState::Behind => (),
            _ => {
                let services = self.peer_map.lock().await.required_services;
                if !version_message.services.has(services) {
                    self.dialog.send_warning(Warning::NoRequiredServices);
                    return Ok(MainThreadMessage::Disconnect);
                }
            }
        }
        let mut peer_map = self.peer_map.lock().await;
        peer_map.tried(nonce).await;
        let needs_peers = peer_map.need_peers().await?;
        // First we signal for ADDRV2 support
        peer_map
            .send_message(nonce, MainThreadMessage::GetAddrV2)
            .await;
        // Then for BIP 339 witness transaction broadcast
        peer_map
            .send_message(nonce, MainThreadMessage::WtxidRelay)
            .await;
        peer_map
            .send_message(nonce, MainThreadMessage::Verack)
            .await;
        // Now we may request peers if required
        if needs_peers {
            crate::log!(self.dialog, "Requesting new addresses");
            peer_map
                .send_message(nonce, MainThreadMessage::GetAddr)
                .await;
        }
        // Inform the user we are connected to all required peers
        if peer_map.live().eq(&self.required_peers) {
            self.dialog.send_info(Log::ConnectionsMet).await
        }
        // Even if we start the node as caught up in terms of height, we need to check for reorgs. So we can send this unconditionally.
        let mut chain = self.chain.lock().await;
        let next_headers = GetHeaderConfig {
            locators: chain.locators().await,
            stop_hash: None,
        };
        Ok(MainThreadMessage::GetHeaders(next_headers))
    }


    // Handle new addresses gossiped over the p2p network
    async fn handle_new_addrs(&self, new_peers: Vec<CombinedAddr>) {
        crate::log!(
            self.dialog,
            format!("Adding {} new peers to the peer database", new_peers.len())
        );
        let mut peer_map = self.peer_map.lock().await;
        peer_map.add_gossiped_peers(new_peers).await;
    }

    // We always send headers to our peers, so our next message depends on our state
    async fn handle_headers(
        &self,
        peer_id: PeerId,
        headers: Vec<Header>,
    ) -> Option<MainThreadMessage> {
        let mut chain = self.chain.lock().await;
        match chain.sync_chain(headers).await {
            Ok(disconnected_blocks) => {
                self.peer_map.lock().await.cancel_blocks(&disconnected_blocks);
            }
            Err(e) => match e {
                HeaderSyncError::EmptyMessage => {
                    if !chain.is_synced().await {
                        return Some(MainThreadMessage::Disconnect);
                    }
                    return self.next_stateful_message(chain.deref_mut()).await;
                }
                HeaderSyncError::LessWorkFork => {
                    self.dialog.send_warning(Warning::UnexpectedSyncError {
                        warning: "A peer sent us a fork with less work.".into(),
                    });
                    return Some(MainThreadMessage::Disconnect);
                }
                _ => {
                    self.dialog.send_warning(Warning::UnexpectedSyncError {
                        warning: format!("Unexpected header syncing error: {}", e),
                    });
                    let mut lock = self.peer_map.lock().await;
                    lock.ban(peer_id).await;
                    return Some(MainThreadMessage::Disconnect);
                }
            }
        }


        self.next_stateful_message(chain.deref_mut()).await
    }

    // Compact filter headers may result in a number of outcomes, including the need to audit filters.
    async fn handle_cf_headers(
        &self,
        peer_id: PeerId,
        cf_headers: CFHeaders,
    ) -> Option<MainThreadMessage> {
        let mut chain = self.chain.lock().await;
        match chain.sync_cf_headers(peer_id.0, cf_headers).await {
            Ok(potential_message) => match potential_message {
                AppendAttempt::AddedToQueue => None,
                AppendAttempt::Extended => self.next_stateful_message(chain.deref_mut()).await,
                AppendAttempt::Conflict(_) => {
                    // TODO: Request the filter and block from the peer
                    self.dialog.send_warning(Warning::UnexpectedSyncError {
                        warning: "Found a conflict while peers are sending filter headers".into(),
                    });
                    Some(MainThreadMessage::Disconnect)
                }
            },
            Err(e) => {
                self.dialog.send_warning(Warning::UnexpectedSyncError {
                    warning: format!("Compact filter header syncing encountered an error: {}", e),
                });
                let mut lock = self.peer_map.lock().await;
                lock.ban(peer_id).await;
                Some(MainThreadMessage::Disconnect)
            }
        }
    }


    async fn fetch_blockchain_info(&self) -> Result<BlockchainInfo, FetchHeaderError> {
        let chain = self.chain.lock().await;
        let chain_work = chain.chainwork();
        let headers = chain.height();

        let best_hash = chain.tip();
        let cf_filter_headers = chain.cf_headers_height();

        // try to fetch both tips, bailing out early on error
        let blocks_tip = chain
            .blocks_tip()
            .await.map(|t| t.height).unwrap_or(0);

        let filters_tip = chain
            .filters_tip()
            .await;

        let filters_count = filters_tip.map(|t| t.height).unwrap_or(0);
        let filters_progress = match self.filter_download_progress.lock().await.clone() {
            Some(p) => p,
            None => filters_count as f32 / cf_filter_headers as f32
        };

        Ok(BlockchainInfo {
            chain: match chain.network {
                Network::Bitcoin => "main",
                Network::Testnet |  Network::Testnet4  => "test",
                Network::Signet => "signet",
                Network::Regtest => "regtest",
                _ => "unknown"
            }.to_string(),
            blocks: if blocks_tip > headers { 0 } else { blocks_tip },
            headers,
            filters: filters_count,
            filter_headers: cf_filter_headers,
            chain_work,
            checkpoint: self.checkpoint,
            prune_height: chain.prune_height,
            best_blockhash: Some(best_hash),
            median_time: chain.median_time() as _,
            block_queue: self.queued_blocks.lock().await.clone(),
            pruned: true,
            filters_progress,
            headers_synced: chain.is_synced().await,
        })
    }

    // Handle a new compact block filter
    async fn handle_filter(&self, peer_id: PeerId, filter: CFilter) -> Option<MainThreadMessage> {
        // TODO: verify this is a filter we want
        let mut lock = self.peer_map.lock().await;
        lock.receive_download(peer_id, DownloadResponseKind::Filter(filter)).await;
        None
    }

    // Scan a block for transactions.
    async fn handle_block(&self, peer_id: PeerId, block: Block) -> Option<MainThreadMessage> {
        let block = {
            let mut lock = self.peer_map.lock().await;
            lock.receive_download(peer_id, DownloadResponseKind::Block(block)).await
        };

        if let Some((height, block)) = block {
            let mut map = BTreeMap::new();
            map.insert(height, block);
            // If the request had no receiver, store the block
            if !self.chain.lock().await.persister.lock().await.insert(map).await {
                self.dialog.send_warning(
                    Warning::FailedPersistence {
                        warning: format!("Could not insert received block {}", height)
                    },
                )
            } else {
                self.dialog.send_dialog(format!("Received queued block {}", height)).await;
                {
                    let mut status = self.queued_blocks.lock().await;
                    status.pending -= 1;
                    status.completed += 1;
                }
            }
        }

        None
    }

    // If new inventory came in, we need to download the headers and update the node state
    async fn handle_inventory_blocks(
        &self,
        nonce: PeerId,
        blocks: Vec<BlockHash>,
    ) -> Option<MainThreadMessage> {
        let mut state = self.state.write().await;
        let mut chain = self.chain.lock().await;
        let mut peer_map = self.peer_map.lock().await;
        for block in blocks.iter() {
            peer_map.increment_height(nonce).await;
            if !chain.contains_hash(*block) {
                crate::log!(self.dialog, format!("New block: {}", block));
            }
        }
        match *state {
            NodeState::Behind => None,
            _ => {
                if blocks.into_iter().any(|block| !chain.contains_hash(block)) {
                    self.dialog
                        .send_info(Log::StateChange(NodeState::Behind))
                        .await;
                    *state = NodeState::Behind;
                    let next_headers = GetHeaderConfig {
                        locators: chain.locators().await,
                        stop_hash: None,
                    };
                    chain.clear_compact_filter_queue();
                    Some(MainThreadMessage::GetHeaders(next_headers))
                } else {
                    None
                }
            }
        }
    }

    // Add more scripts to the chain to look for. Does not imply a rescan.
    async fn add_script(&self, script: ScriptBuf) {
        let mut chain = self.chain.lock().await;
        chain.put_script(script);
    }

    // Clear the filter hash cache and redownload the filters.
    async fn rescan(&self) -> Option<MainThreadMessage> {
        let mut state = self.state.write().await;
        let mut chain = self.chain.lock().await;
        match *state {
            NodeState::Behind => None,
            NodeState::HeadersSynced => None,
            _ => {
                chain.clear_filters();
                self.dialog
                    .send_info(Log::StateChange(NodeState::FilterHeadersSynced))
                    .await;
                *state = NodeState::FilterHeadersSynced;
                // TODO: clean filters
                None
            }
        }
    }

    // Continue the filter syncing process by explicit command
    async fn start_filter_download(&self) -> Option<MainThreadMessage> {
        let mut download_policy = self.filter_sync_policy.write().await;
        if self.external_filter_endpoint.is_some() {
            self.dialog.send_dialog("Starting external filter download as requested").await;
            *download_policy = FilterSyncPolicy::External;
        } else {
            self.dialog.send_dialog("Starting p2p filter download as requested").await;
            *download_policy = FilterSyncPolicy::P2P;
        }
        drop(download_policy);
        None
    }

    // When the application starts, fetch any headers we know about from the database.
    async fn fetch_headers(&self) -> Result<(), NodeError<H::Error, P::Error>> {
        crate::log!(self.dialog, "Attempting to load headers from the database");
        let mut chain = self.chain.lock().await;
        chain
            .load_headers()
            .await
            .map_err(NodeError::HeaderDatabase)
    }
}

impl core::fmt::Display for NodeState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            NodeState::Behind => {
                write!(f, "Requesting block headers.")
            }
            NodeState::HeadersSynced => {
                write!(f, "Headers synced.")
            }
            NodeState::FilterHeadersSynced => {
                write!(f, "Filter headers synced.")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExternalFilterLoader {
    #[allow(dead_code)]
    progress: Option<DownloadProgress>,
    status: FilterLoadStatus,
    endpoint_url: String,
    filter_headers_path: PathBuf,
    filters_path: PathBuf,
    header_checkpoint: HeaderCheckpoint,
    prune_point: HeaderCheckpoint,
    attempts: usize,
    last_attempt: Instant,
}

#[derive(Debug, Clone)]
pub enum FilterLoadStatus {
    Idle,
    FilterHeaders,
    Filters,
    Loaded,
}

#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub downloaded: u64,
    pub total: u64,

}

impl ExternalFilterLoader {

    async fn sync_next(&mut self, dialog: &Arc<Dialog>, p: Arc<Mutex<Option<f32>>>) -> anyhow::Result<()> {
        if self.last_attempt.elapsed() <= Duration::from_secs(1) {
            return Ok(());
        }
        match self.try_sync_next(dialog, p).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if self.attempts > 6 {
                    return Err(e);
                }
                self.last_attempt = Instant::now();
                self.attempts += 1;
                dialog.send_dialog(format!(
                    "{} (attempt {})",
                    e.to_string(), self.attempts)).await;
                Ok(())
            }
        }
    }
    async fn try_sync_next(
        &mut self,
        dialog: &Arc<Dialog>,
        p: Arc<Mutex<Option<f32>>>
    ) -> anyhow::Result<()> {
        if matches!(self.status, FilterLoadStatus::Loaded) {
            return Ok(());
        }

        let endpoint_url = self.endpoint_url.trim_end_matches("/");
        match self.status {
            FilterLoadStatus::Idle => self.status = FilterLoadStatus::FilterHeaders,
            FilterLoadStatus::FilterHeaders => {
                let headers_endpoint = format!("{}/cf_headers_{}-{}.bin", endpoint_url,
                                               self.header_checkpoint.height,
                                               self.prune_point.height
                );
                if self.filter_headers_path.exists() {
                    let backup = self.filter_headers_path.with_extension("bak");
                    let _ = tokio::fs::rename(&self.filter_headers_path, &backup).await;
                }
                dialog.send_dialog(format!("Loading filter headers from {}", headers_endpoint)).await;
                self.download_file(&headers_endpoint, &self.filter_headers_path, None).await?;
                self.status = FilterLoadStatus::Filters;
            }
            FilterLoadStatus::Filters => {
                if self.filters_path.exists() {
                    let backup = self.filters_path.with_extension("bak");
                    let _ = tokio::fs::rename(&self.filters_path, &backup).await;
                }
                let filters_endpoint = format!("{}/filters_{}-{}.db", endpoint_url,
                                               self.header_checkpoint.height,
                                               self.prune_point.height
                );
                dialog.send_dialog(format!("Loading filters from {}", filters_endpoint)).await;

                let (tx, mut rx) =
                    mpsc::channel::<DownloadProgress>(20);
                tokio::spawn(async move {
                   while let Some(update) = rx.recv().await {
                       p.lock().await.replace(update.downloaded as f32 / update.total as f32);
                   }
                });
                self.download_file(&filters_endpoint, &self.filters_path, Some(tx)).await?;
                self.status = FilterLoadStatus::Loaded;
            }
            FilterLoadStatus::Loaded => {
                dialog.send_dialog("Filters loaded").await;
            }
        }

        Ok(())
    }

    pub async fn download_file(
        &self,
        url: &str,
        file_path: &PathBuf,
        mut progress: Option<mpsc::Sender<DownloadProgress>>,
    ) -> anyhow::Result<()> {
        let client = reqwest::Client::new();
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| anyhow!("Could not load filters from external source: {}", e))?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("HTTP request failed with status: {}", response.status()));
        }

        let total = response
            .content_length()
            .context("Could not get content length")?;

        let mut file = tokio::fs::File::create(file_path)
            .await
            .map_err(|e| anyhow!("Could not create path to store filters: {}", e))?;

        let mut downloaded = 0;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Failed to read chunk")?;
            file.write_all(&chunk)
                .await
                .context("Failed to write chunk to file")?;
            downloaded += chunk.len() as u64;

            if let Some(progress) = progress.as_mut() {
                _ = progress.send(DownloadProgress { downloaded, total }).await;
            }
        }
        file.flush().await.context("Failed to flush file")?;
        Ok(())
    }
}

impl MempoolStore {
    pub async fn load(file_path: PathBuf) -> anyhow::Result<Self> {
        let mut txs = match tokio::fs::read_to_string(&file_path).await {
            Ok(data) => serde_json::from_str(&data)
                .map_err(|e| anyhow!("Could not deserialize mempool from {}: {}", file_path.display(), e))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => vec![],
            Err(e) => return Err(anyhow!("Could not read mempool file {}: {}", file_path.display(), e)),
        };

        // refresh all
        txs.iter_mut().for_each(|tx: &mut MempoolTransaction| {
           if tx.last_seen.is_some() {
               tx.last_seen = Some(Instant::now());
           }
        });
        Ok(Self { txs, file_path })
    }

    pub async fn insert(&mut self, tx: MempoolTransaction) -> anyhow::Result<()> {
        self.txs.retain(|mtx| mtx.txid != tx.txid);
        self.txs.push(tx);
        self.save().await
    }

    pub fn get_mempool_entry(&mut self, txid: Txid) -> Option<MempoolEntryResult> {
        let entry = self.txs.iter()
            .find(|tx| tx.txid == txid)?;
        Some(MempoolEntryResult {
            time: entry.time,
            ..MempoolEntryResult::default()
        })
    }

    pub async fn remove(&mut self, txid: Txid) -> anyhow::Result<()> {
        self.txs.retain(|mtx| mtx.txid != txid);
        self.save().await
    }

    pub async fn mark_broadcasted(&mut self, txid: Txid) -> anyhow::Result<bool> {
        let entry = match self.txs
            .iter_mut().find(|m| m.txid == txid) {
            None => return Ok(false),
            Some(entry) => entry,
        };

        entry.broadcast = Some(Instant::now());
        entry.last_check = Some(Instant::now());
        self.save().await?;
        Ok(true)
    }

    pub async fn refresh(&mut self, txid: Txid) -> anyhow::Result<()> {
        if let Some(mem_tx) = self.txs.iter_mut()
            .find(|m| m.txid == txid) {
            mem_tx.last_seen = Some(Instant::now());
        }
        self.save().await
    }

    pub async fn remove_stale(&mut self) -> anyhow::Result<()> {
        self.txs.retain(|m| {
            match &m.broadcast {
                None => true,
                Some(broadcast) =>
                    m.last_seen.as_ref()
                        .unwrap_or(broadcast).elapsed() < LAST_SEEN_IN_MEMPOOL_TIMEOUT
            }
        });
        self.save().await
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        let data = serde_json::to_string_pretty(&self.txs)
            .expect("could not serialize mempool txs");
        tokio::fs::write(&self.file_path, data).await
            .map_err(|e| anyhow!("Could not store mempool to file: {}: {}",
                self.file_path.display(), e))?;
        Ok(())
    }

    pub fn get_txids_to_check(&mut self) -> Vec<Txid> {
        self.txs.iter_mut()
            .filter(|m|
                m.last_check
                    .is_some_and(|t| t.elapsed() > CHECK_PEERS_MEMPOOL_INTERVAL)
            )
            .map(|m| {
                m.last_check = Some(Instant::now());
                m.txid
            }).collect()
    }
}

fn serialize_instant<S>(instant: &Option<Instant>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let seconds = match instant {
        Some(i) => {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| serde::ser::Error::custom(format!("Time error: {}", e)))?
                .as_secs();
            Some(now_secs - i.elapsed().as_secs())
        }
        None => None,
    };
    seconds.serialize(serializer)
}

fn deserialize_instant<'de, D>(deserializer: D) -> Result<Option<Instant>, D::Error>
where
    D: Deserializer<'de>,
{
    let seconds = Option::<u64>::deserialize(deserializer)?;
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| serde::de::Error::custom(format!("Time error: {}", e)))?
        .as_secs();
    Ok(seconds.map(|s| {
        let elapsed_secs = now_secs.saturating_sub(s);
        Instant::now() - Duration::from_secs(elapsed_secs)
    }))
}
