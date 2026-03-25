extern crate alloc;
use std::{
    collections::{BTreeMap, HashSet},
    ops::Range,
    sync::Arc,
};
use std::collections::VecDeque;
use std::path::PathBuf;
use anyhow::anyhow;
use tokio::sync::RwLock;
use bitcoin::{block::Header, p2p::message_filter::{CFHeaders, GetCFHeaders}, Block, BlockHash, CompactTarget, Network, ScriptBuf, Work};
use tokio::sync::Mutex;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinSet;
use tracing::info;
use super::{
    checkpoints::{HeaderCheckpoint, HeaderCheckpoints},
    error::{HeaderSyncError},
    header_chain::HeaderChain,
    HeightMonitor,
};
use crate::core::messages::{DownloadKind};
use crate::{chain::header_batch::HeadersBatch, core::{
    dialog::Dialog,
    error::HeaderPersistenceError,
    messages::{Event, Warning},
}, db::traits::HeaderStore, filters::{
    cfheader_batch::CFHeaderBatch,
    cfheader_chain::{AppendAttempt, CFHeaderChain, QueuedCFHeader},
    error::{CFHeaderSyncError},
    CF_HEADER_BATCH_SIZE,
}, IndexedFilter};
use crate::chain::block_queue::{DownloadRequest};
use crate::core::error::DownloadRequestError;
use crate::db::sqlite::blocks::{BlocksStore};
use crate::db::sqlite::filters::FiltersStore;

#[derive(Clone, Copy, Debug)]
pub enum AdvanceKind {
    Blocks,
    Filters,
}

#[derive(Clone, Debug)]
pub struct DownloadPersister<B: BlocksStore + 'static, F: FiltersStore + 'static> {
    block_db: B,
    filters_db: F,
    prune_point: Option<HeaderCheckpoint>,
    checkpoint: HeaderCheckpoint,
}

impl<B: BlocksStore, F: FiltersStore> DownloadPersister<B, F> {
    pub async fn get_block_hash(&mut self, kind: AdvanceKind, height: u32) -> Option<BlockHash> {
        match kind {
            AdvanceKind::Blocks => {
                self.block_db.get_block_hash(height).await.unwrap_or(None)
            }
            AdvanceKind::Filters => {
                self.filters_db.get_block_hash(height).await.unwrap_or(None)
            }
        }
    }
    pub async fn get_tip(&mut self, kind: AdvanceKind) -> Option<HeaderCheckpoint> {
        match kind {
            AdvanceKind::Blocks => {
                let tip = match self.block_db.get_tip().await.ok() {
                    Some(tip) => tip.unwrap_or(self.checkpoint),
                    None => return None
                };
                match &self.prune_point {
                    None => self.block_db.get_tip().await.ok()?,
                    Some(prune_point) => if prune_point.height > tip.height {
                        Some(*prune_point)
                    } else {
                        Some(tip)
                    }
                }
            }
            AdvanceKind::Filters => {
                self.filters_db.get_tip().await.ok()?
            }
        }
    }

    pub async fn set_tip(&mut self, kind: AdvanceKind, tip: HeaderCheckpoint) -> bool {
        match kind {
            AdvanceKind::Blocks => {
                self.block_db.set_tip(tip).await.is_ok()
            }
            AdvanceKind::Filters => {
                self.filters_db.set_tip(tip).await.is_ok()
            }
        }
    }

    pub async fn insert(&mut self, batch: BTreeMap<u32, DownloadKind>) -> bool {
        let mut batch_1 = BTreeMap::new();
        let mut batch_2 = BTreeMap::new();

        for (key, value) in batch.into_iter() {
            match value {
                DownloadKind::Block(idx) => {
                    assert_eq!(key, idx.height, "heights must be equal");
                    batch_1.insert(key, idx.block);
                }
                DownloadKind::Filter(idx) => {
                    assert_eq!(key, idx.height, "heights must be equal");
                    batch_2.insert(key, idx.filter);
                }
            }
        }

        if !batch_1.is_empty() {
            if let Err(_) = self.block_db.insert_blocks(batch_1).await {
                return false;
            }
        }
        if !batch_2.is_empty() {
            if let Err(_) = self.filters_db.insert_filters(batch_2).await {
                return false;
            }
        }

        true
    }

    pub async fn get_tip_or_default(&mut self, kind: AdvanceKind) -> HeaderCheckpoint {
        self.get_tip(kind).await.unwrap_or(self.get_default_tip(kind))
    }

    fn get_default_tip(&self, kind: AdvanceKind) -> HeaderCheckpoint {
        match kind {
            AdvanceKind::Blocks => self.prune_point.unwrap_or(self.checkpoint),
            AdvanceKind::Filters => self.checkpoint
        }
    }

    async fn flush(
        &mut self,
        kind: AdvanceKind,
        blocks_buffer: &mut BTreeMap<u32, DownloadKind>,
        persisted_heights: &mut BTreeMap<u32, BlockHash>,
    ) -> bool {
        let buffered_heights: Vec<_> = blocks_buffer.iter()
            .map(|(k, v)| (*k, v.block_hash())).collect();

        if !self.insert(std::mem::take(blocks_buffer)).await {
            return false;
        }
        for (height, hash) in buffered_heights {
            persisted_heights.insert(height, hash);
        }

        let mut update_tip =  self.get_tip_or_default(kind).await;
        while let Some(hash) = persisted_heights.get(&(update_tip.height + 1)) {
            update_tip.height += 1;
            update_tip.hash = hash.clone();
        }
        self.set_tip(kind, update_tip).await
    }
}

const MAX_REORG_DEPTH: u32 = 5_000;
const REORG_LOOKBACK: u32 = 7;
const MAX_HEADER_SIZE: usize = 20_000;
const FILTER_BASIC: u8 = 0x00;

#[derive(Debug)]
pub(crate) struct Chain<H: HeaderStore, B: BlocksStore + 'static, F: FiltersStore + 'static> {
    header_chain: HeaderChain,
    cf_header_chain: CFHeaderChain,
    checkpoints: HeaderCheckpoints,
    pub network: Network,
    db: Arc<Mutex<H>>,
    pub(crate) persister: Arc<Mutex<DownloadPersister<B, F>>>,
    heights: Arc<Mutex<HeightMonitor>>,
    scripts: HashSet<ScriptBuf>,
    dialog: Arc<Dialog>,
    downloading: Arc<RwLock<bool>>,
    pub(crate) prune_height: Option<u32>,
    cfheader_chain_path: PathBuf,
    pub can_sync_filters: bool,
    cfheader_quorum_required: usize,

    // Median time calculation
    median_time: u32,
    median_timestamps: VecDeque<u32>,
    median_last_height: u32,
}

#[allow(dead_code)]
impl<H: HeaderStore, B: BlocksStore, F: FiltersStore> Chain<H, B, F> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        network: Network,
        scripts: HashSet<ScriptBuf>,
        anchor: HeaderCheckpoint,
        checkpoints: HeaderCheckpoints,
        dialog: Arc<Dialog>,
        height_monitor: Arc<Mutex<HeightMonitor>>,
        db: H,
        block_db: B,
        filters_db: F,
        cfheader_chain_path: PathBuf,
        quorum_required: usize,
        prune_point: Option<HeaderCheckpoint>,
    ) -> Self {
        let header_chain = HeaderChain::new(anchor);
        let cf_header_chain = match CFHeaderChain::load(cfheader_chain_path.clone()) {
            Ok(cf_header_chain) => cf_header_chain,
            Err(_) => {
                CFHeaderChain::new(anchor, quorum_required)
            }
        };

        let checkpoint = cf_header_chain.checkpoint().clone();
        Chain {
            header_chain,
            checkpoints,
            network,
            db: Arc::new(Mutex::new(db)),
            cf_header_chain,
            cfheader_chain_path,
            heights: height_monitor,
            scripts,
            dialog,
            persister: Arc::new(Mutex::new(DownloadPersister {
                block_db,
                filters_db,
                prune_point,
                checkpoint,
            })),
            downloading: Arc::new(RwLock::new(false)),
            prune_height: prune_point.map(|p| p.height),
            can_sync_filters: false,
            cfheader_quorum_required: quorum_required,
            median_time: 0,
            median_timestamps: VecDeque::with_capacity(11),
            median_last_height: 0,
        }
    }


    pub async fn reload_filters(&mut self) -> anyhow::Result<()> {
        let mut err = None;
        self.cf_header_chain = match CFHeaderChain::load(self.cfheader_chain_path.clone()) {
            Ok(cf_header_chain) => {
                cf_header_chain
            },
            Err(e) => {
                err = Some(e);
                CFHeaderChain::new(
                    self.persister.lock().await.checkpoint.clone(),
                    self.cfheader_quorum_required
                )
            }
        };
        if let Some(err) = err {
            return Err(anyhow!("Could not reload filter headers: {}", err));
        }
        self.persister.lock().await.filters_db.reload().await.map_err(|e|
            anyhow!("Could not reload filters: {}", e))?;
        Ok(())
    }

    pub(crate) async fn join_block_download(
        advance_kind: AdvanceKind,
        persister: Arc<Mutex<DownloadPersister<B, F>>>,
        mut set: JoinSet<Result<Result<DownloadKind, DownloadRequestError>, RecvError>>,
    ) -> bool {
        let mut blocks_buffer = BTreeMap::new();
        let mut persisted_heights = BTreeMap::new();

        while let Some(result) = set.join_next().await {
            let response = match result {
                Ok(Ok(Ok(block))) => block,
                _ => return false,
            };
            let (height, _hash) = match &response {
                DownloadKind::Block(idx) => (idx.height, idx.block.block_hash()),
                DownloadKind::Filter(idx) => (idx.height, *idx.filter.block_hash())
            };

            blocks_buffer.insert(height, response);
            if blocks_buffer.len() > 20 {
                if !persister.lock().await.flush(advance_kind, &mut blocks_buffer, &mut persisted_heights).await {
                    return false;
                }
            }
        }

        if !blocks_buffer.is_empty() {
            if !persister.lock().await.flush(advance_kind, &mut blocks_buffer, &mut persisted_heights).await {
                return false;
            }
        }
        true
    }

    async fn get_next_batch(&mut self, kind: AdvanceKind) -> Result<BTreeMap<u32, Header>, DownloadRequestError> {
        let end_height = match kind {
            AdvanceKind::Blocks => self.height(),
            AdvanceKind::Filters => std::cmp::min(self.prune_height.unwrap_or(self.height()), self.height())
        };

        let mut start_height = self.persister.lock().await.get_tip_or_default(kind).await.height;

        if start_height > end_height {
            start_height = end_height;
        }

        let remaining = std::cmp::min(end_height - start_height, 160);
        if remaining == 0 {
            return Ok(BTreeMap::new());
        }

        let range = start_height + 1..start_height + remaining + 1;
        if range.is_empty() {
            return Ok(BTreeMap::new());
        }

        let headers = self
            .fetch_header_range(range.clone()).await
            .map_err(|e| DownloadRequestError::DatabaseOptFailed {
                error: e.to_string()
            })?;
        self.dialog.send_dialog(format!("Fetched header range: {:?} (headers: {})", range,
                                        headers.len())).await;

        Ok(headers)
    }

    pub(crate) fn median_time(&self) -> u32 {
        self.median_time
    }

    pub(crate) async fn update_median_time(&mut self) {
        let current_height = self.height();
        let target_count = std::cmp::min(11, current_height + 1) as usize;

        let start_height = current_height.saturating_sub(target_count as u32 - 1);
        let fetch_start = if self.median_timestamps.is_empty() {
            start_height
        } else {
            self.median_last_height + 1
        };

        if fetch_start <= current_height {
            if let Ok(headers) = self.fetch_header_range(fetch_start..current_height + 1).await {
                for (_, header) in headers {
                    if self.median_timestamps.len() >= target_count {
                        self.median_timestamps.pop_front();
                    }
                    self.median_timestamps.push_back(header.time);
                }
            } else {
                self.dialog.send_warning(Warning::FailedPersistence {
                    warning: format!("Failed to fetch headers for median time at height {}", current_height),
                });
            }
        }
        while self.median_timestamps.len() > target_count {
            self.median_timestamps.pop_front();
        }
        self.median_time = calculate_median(&self.median_timestamps);
        self.median_last_height = current_height;
    }

    pub(crate) async fn advance_downloads_tip(&mut self, filter_sync: bool) -> Result<Vec<DownloadRequest>, DownloadRequestError> {
        if filter_sync && !self.is_cf_headers_synced() {
            return Ok(vec![]);
        }
        if *self.downloading.read().await {
            return Ok(vec![]);
        }
        {
            let mut downloading = self.downloading.write().await;
            *downloading = true;
        }

        let kind = if filter_sync {
            AdvanceKind::Filters
        } else {
            AdvanceKind::Blocks
        };

        let headers = self.get_next_batch(kind).await?;
        if headers.is_empty() {
            let mut downloading = self.downloading.write().await;
            *downloading = false;
            return Ok(vec![]);
        }

        let mut requests = Vec::with_capacity(headers.len());
        let mut receivers = Vec::with_capacity(headers.len());
        let mut heights = BTreeMap::new();

        for (height, header) in headers.iter() {
            let (tx, rx) =
                tokio::sync::oneshot::channel::<Result<DownloadKind, DownloadRequestError>>();
            let block_hash = header.block_hash();
            requests.push((*height, block_hash, tx));
            receivers.push(rx);
            heights.insert(block_hash, *height);
        }

        let mut join_set = JoinSet::new();
        for rx in receivers {
            join_set.spawn(async move {
                rx.await
            });
        }

        let downloading = self.downloading.clone();
        let persistor = self.persister.clone();
        _ = tokio::task::spawn(async move {
            Self::join_block_download(kind, persistor, join_set).await;
            let mut downloading = downloading.write().await;
            *downloading = false;
        });

        Ok(requests.into_iter().map(|(height, hash, sender)| DownloadRequest {
            hash,
            sender: Some(sender),
            height,
            downloading_since: None,
            kind,
        }).collect())
    }

    pub(crate) async fn get_block(&self, block_hash: &BlockHash) -> Result<Option<Block>, <B as BlocksStore>::Error> {
        self.persister.lock().await.block_db.get_block_by_hash(block_hash).await
    }

    pub(crate) async fn get_block_filter_by_height(&self, height: u32) -> Result<Option<IndexedFilter>, <F as FiltersStore>::Error> {
        self.persister.lock().await.filters_db.get_filter_by_height(height).await.map(|f| f.map(|f| {
            IndexedFilter {
                height,
                filter: f,
            }
        }))
    }

    // Top of the chain
    pub(crate) fn tip(&self) -> BlockHash {
        self.header_chain.tip()
    }

    pub(crate) async fn prune_up_to_height(&self, height: u32) -> anyhow::Result<u32> {
        let mut persister = self.persister.lock().await;
        let deleted = persister.block_db.prune_up_to_height(height).await
            .map_err(|_| anyhow::anyhow!("could not prune blocks"))?;
        Ok(deleted)
    }

    pub(crate) async fn blocks_tip(&self) -> Option<HeaderCheckpoint> {
        self.persister.lock().await.get_tip(AdvanceKind::Blocks).await
    }

    pub(crate) async fn filters_tip(&self) -> Option<HeaderCheckpoint> {
        self.persister.lock().await.get_tip(AdvanceKind::Filters).await
    }

    pub(crate) async fn filters_synced(&self) -> bool {
        if let Some(tip) = self.persister.lock().await.get_tip(AdvanceKind::Filters).await {
            tip.height >= self.prune_height.unwrap_or(self.height())
        } else {
            false
        }
    }

    pub(crate) async fn blocks_synced(&self) -> bool {
        if let Some(tip) = self.persister.lock().await.get_tip(AdvanceKind::Blocks).await {
            self.height() == tip.height
        } else { false }
    }

    // The canoncial height of the chain, one less than the length
    pub(crate) fn height(&self) -> u32 {
        self.header_chain.height()
    }

    // This header chain contains a block hash in memory
    pub(crate) fn contains_hash(&self, blockhash: BlockHash) -> bool {
        self.header_chain.contains_hash(blockhash)
    }

    // This header chain contains a block hash, potentially checking the disk
    pub(crate) async fn height_of_hash(&self, blockhash: BlockHash) -> Option<u32> {
        match self.header_chain.height_of_hash(blockhash) {
            Some(height) => Some(height),
            None => {
                let mut lock = self.db.lock().await;
                lock.height_of(&blockhash).await.unwrap_or(None)
            }
        }
    }

    // This header chain contains a block hash in memory
    pub(crate) fn cached_header_at_height(&self, height: u32) -> Option<&Header> {
        self.header_chain.header_at_height(height)
    }

    // Fetch a header from the cache or disk.
    pub(crate) async fn fetch_header(
        &mut self,
        height: u32,
    ) -> Result<Option<Header>, HeaderPersistenceError<H::Error>> {
        match self.header_chain.header_at_height(height) {
            Some(header) => Ok(Some(*header)),
            None => {
                let mut db = self.db.lock().await;
                let header_opt = db.header_at(height).await;
                if header_opt.is_err() {
                    self.dialog
                        .send_warning(Warning::FailedPersistence {
                            warning: format!(
                                "Unexpected error fetching a header from the header store at height {height}"
                            ),
                        });
                }
                header_opt.map_err(HeaderPersistenceError::Database)
            }
        }
    }

    // The hash at the given height, potentially checking on disk
    pub(crate) async fn blockhash_at_height(&self, height: u32) -> Option<BlockHash> {
        match self
            .cached_header_at_height(height)
            .map(|header| header.block_hash())
        {
            Some(hash) => Some(hash),
            None => {
                let mut lock = self.db.lock().await;
                lock.hash_at(height).await.unwrap_or(None)
            }
        }
    }

    // This header chain contains a block hash
    pub(crate) fn contains_header(&self, header: &Header) -> bool {
        self.header_chain.contains_header(header)
    }

    // Canoncial chainwork
    pub(crate) fn chainwork(&self) -> Work {
        self.header_chain.chainwork()
    }

    // Calculate the chainwork after a fork height to evalutate the fork
    pub(crate) fn chainwork_after_height(&self, height: u32) -> Work {
        self.header_chain.chainwork_after_height(height)
    }

    // Human readable chainwork
    pub(crate) fn log2_work(&self) -> f64 {
        self.header_chain.log2_work()
    }

    // Have we hit the known checkpoints
    pub(crate) fn checkpoints_complete(&self) -> bool {
        self.checkpoints.is_exhausted()
    }

    // The last ten heights and headers in the chain
    pub(crate) fn last_ten(&self) -> BTreeMap<u32, Header> {
        self.header_chain.last_ten()
    }

    // Do we have best known height and is our height equal to it
    // If our height is greater, we received partial inventory, and
    // the header message contained the rest of the new blocks.
    pub(crate) async fn is_synced(&self) -> bool {
        let height_lock = self.heights.lock().await;
        match height_lock.max() {
            Some(peer_max) => self.height() >= peer_max,
            None => false,
        }
    }

    // The "locators" are the headers we inform our peers we know about
    pub(crate) async fn locators(&mut self) -> Vec<BlockHash> {
        // If a peer is sending us a fork at this point they are faulty.
        if !self.checkpoints_complete() {
            vec![self.tip()]
        } else {
            // We should try to catch any reorgs if we are on a fresh start.
            // The database may have a header that is useful to the remote node
            // that is not currently in memory.
            if self.header_chain.inner_len() < REORG_LOOKBACK as usize {
                let older_locator = self.height().saturating_sub(REORG_LOOKBACK);
                let mut db_lock = self.db.lock().await;
                let hash = db_lock.hash_at(older_locator).await;
                if let Ok(Some(locator)) = hash {
                    vec![self.tip(), locator]
                } else {
                    // We couldn't find a header deep enough to send over. Just proceed as usual
                    self.header_chain.locators()
                }
            } else {
                // We have enough headers in memory to catch a reorg.
                self.header_chain.locators()
            }
        }
    }

    // Write the chain to disk
    pub(crate) async fn flush_to_disk(&mut self) {
        if let Err(e) = self
            .db
            .lock()
            .await
            .write(self.header_chain.headers())
            .await
        {
            self.dialog.send_warning(Warning::FailedPersistence {
                warning: format!("Could not save headers to disk: {e}"),
            });
        }
    }

    pub(crate) async fn reset_tip(&mut self, kind: AdvanceKind) -> anyhow::Result<()> {
        let mut tip = match self.persister.lock().await.get_tip(kind).await {
            None => return Ok(()),
            Some(tip) => tip
        };

        loop {
            if tip.height == 0 {
                break;
            }
            let expected_hash = match self.blockhash_at_height(tip.height).await {
                Some(hash) => hash,
                None => continue
            };
            if expected_hash == tip.hash {
                break;
            }

            tip.height -= 1;
            match self.persister.lock().await
                .get_block_hash(kind, tip.height).await {
                None => {
                    tip = self.persister.lock().await.get_default_tip(kind);
                    break;
                },
                Some(hash) => tip.hash = hash,
            }
        }

        if !self.persister.lock().await.set_tip(kind, tip).await {
            return Err(anyhow::anyhow!("Could not reset tip for {}", match kind {
                AdvanceKind::Blocks => "blocks",
                AdvanceKind::Filters => "filters"
            }));
        }
        Ok(())
    }

    pub(crate) async fn reset_tip_old(&mut self, kind: AdvanceKind) -> Option<()> {
        let mut new_tip = self.persister.lock().await.get_tip(kind).await?;
        info!("Resetting tip");
        loop {
            if new_tip.height == 0 {
                break;
            }
            let hash = self.blockhash_at_height(new_tip.height).await;
            match hash {
                None => {
                    info!("No hash found at height {}", new_tip.height);
                    new_tip.height -= 1;
                    continue;
                }
                Some(hash) => {
                    if new_tip.hash != hash {
                        self.dialog.send_dialog(
                            format!("Resetting tip found new_tip hash {}, prev_tip hash {} dont match at new tip height {}",
                                    new_tip.hash, hash,  new_tip.height)).await;
                        new_tip.height -= 1;
                        continue;
                    }

                    info!("Found new tip at {} {} breaking", new_tip.height, new_tip.hash);
                    break;
                }
            }
        }
        info!("broke out of the loop");
        self.persister.lock().await.set_tip(kind, new_tip).await;
        Some(())
    }

    // Write the chain to disk, overriding previous heights
    pub(crate) async fn flush_over_height(&mut self, height: u32) {
        if let Err(e) = self
            .db
            .lock()
            .await
            .write_over(self.header_chain.headers(), height)
            .await
        {
            self.dialog.send_warning(Warning::FailedPersistence {
                warning: format!("Could not save headers to disk: {e}"),
            });
        }
    }

    // Load in the headers
    pub(crate) async fn load_headers(&mut self) -> Result<(), HeaderPersistenceError<H::Error>> {
        let loaded_headers = self
            .db
            .lock()
            .await
            .load(self.height() + 1..)
            .await
            .map_err(HeaderPersistenceError::Database)?;
        if let Some(first) = loaded_headers.values().next() {
            if first.prev_blockhash.ne(&self.tip()) {
                self.dialog.send_warning(Warning::InvalidStartHeight);
                // The header chain did not align, so just start from the anchor
                return Err(HeaderPersistenceError::CannotLocateHistory);
            } else if loaded_headers
                .iter()
                .zip(loaded_headers.iter().skip(1))
                .any(|(first, second)| first.1.block_hash().ne(&second.1.prev_blockhash))
            {
                self.dialog.send_warning(Warning::CorruptedHeaders);
                return Err(HeaderPersistenceError::HeadersDoNotLink);
            }
            loaded_headers.iter().for_each(|header| {
                if let Some(checkpoint) = self.checkpoints.next() {
                    if header.1.block_hash().eq(&checkpoint.hash) {
                        self.checkpoints.advance()
                    }
                }
            })
        }
        self.header_chain.set_headers(loaded_headers);
        Ok(())
    }

    // If the number of headers in memory gets too large, move some of them to the disk
    pub(crate) async fn manage_memory(&mut self) {
        if self.header_chain.inner_len() > MAX_HEADER_SIZE {
            self.flush_to_disk().await;
            self.header_chain.move_up();
        }
    }

    // Sync the chain with headers from a peer, adjusting to reorgs if needed
    pub(crate) async fn sync_chain(&mut self, message: Vec<Header>) -> Result<Vec<BlockHash>, HeaderSyncError> {
        let mut disconnected_hashes = vec![];

        let header_batch = HeadersBatch::new(message).map_err(|_| HeaderSyncError::EmptyMessage)?;
        // If our chain already has the last header in the message there is no new information
        if self.contains_hash(header_batch.last().block_hash()) {
            return Ok(disconnected_hashes);
        }
        // We check first if the peer is sending us nonsense
        self.sanity_check(&header_batch)?;
        // How we handle forks depends on if we are caught up through all checkpoints or not
        match self.checkpoints.next().cloned() {
            Some(checkpoint) => {
                self.catch_up_sync(header_batch, checkpoint).await?;
            }
            None => {
                // Nothing left to do but add the headers to the chain
                if self.tip().eq(&header_batch.first().prev_blockhash) {
                    self.audit_difficulty(self.height(), &header_batch).await?;
                    self.header_chain.extend(header_batch.inner());
                    return Ok(disconnected_hashes);
                }
                // We see if we have this previous hash in the database, and reload our
                // chain from that hash if so.
                let fork_start_hash = header_batch.first().prev_blockhash;
                if !self.contains_hash(fork_start_hash) {
                    self.load_fork(&header_batch).await?;
                }
                // Check if the fork has more work.
                disconnected_hashes = self.evaluate_fork(&header_batch).await?;
            }
        };
        self.manage_memory().await;
        Ok(disconnected_hashes)
    }

    // These are invariants in all batches of headers we receive
    fn sanity_check(&mut self, header_batch: &HeadersBatch) -> Result<(), HeaderSyncError> {
        let initially_syncing = !self.checkpoints.is_exhausted();
        // Some basic sanity checks that should result in peer bans on errors

        // If we aren't synced up to the checkpoints we don't accept any forks
        if initially_syncing && self.tip().ne(&header_batch.first().prev_blockhash) {
            return Err(HeaderSyncError::PreCheckpointFork);
        }

        // All the headers connect with each other and is the difficulty adjustment not absurd
        if !header_batch.connected() {
            return Err(HeaderSyncError::HeadersNotConnected);
        }

        // All headers pass their own proof of work and the network minimum
        if !header_batch.individually_valid_pow() {
            return Err(HeaderSyncError::InvalidHeaderWork);
        }

        if !header_batch.bits_adhere_transition(self.network) {
            return Err(HeaderSyncError::InvalidBits);
        }

        Ok(())
    }

    /// Sync with extra requirements on checkpoints and forks
    async fn catch_up_sync(
        &mut self,
        header_batch: HeadersBatch,
        checkpoint: HeaderCheckpoint,
    ) -> Result<(), HeaderSyncError> {
        self.audit_difficulty(self.height(), &header_batch).await?;
        // Eagerly append the batch to the chain
        self.header_chain.extend(header_batch.inner());
        // We need to check a hard-coded checkpoint
        if self.height().ge(&checkpoint.height) {
            if self
                .blockhash_at_height(checkpoint.height)
                .await
                .ok_or(HeaderSyncError::InvalidCheckpoint)?
                .eq(&checkpoint.hash)
            {
                crate::log!(
                    self.dialog,
                    format!("Found checkpoint, height: {}", checkpoint.height)
                );
                crate::log!(self.dialog, "Writing progress to disk...");
                self.checkpoints.advance();
                self.flush_to_disk().await;
            } else {
                self.dialog
                    .send_warning(
                        Warning::UnexpectedSyncError { warning: "Unmatched checkpoint sent by a peer. Restarting header sync with new peers.".into() }
                    );
                return Err(HeaderSyncError::InvalidCheckpoint);
            }
        }
        Ok(())
    }

    // Audit the difficulty adjustment of the blocks we received

    // This function draws from the neutrino implemention, where even if a fork is valid
    // we only accept it if there is more work provided. otherwise, we disconnect the peer sending
    // us this fork
    async fn evaluate_fork(&mut self, header_batch: &HeadersBatch) -> Result<Vec<BlockHash>, HeaderSyncError> {
        self.dialog.send_warning(Warning::EvaluatingFork);
        // We only care about the headers these two chains do not have in common
        let uncommon: Vec<Header> = header_batch
            .inner()
            .iter()
            .filter(|header| !self.contains_header(header))
            .copied()
            .collect();
        let challenge_chainwork = uncommon
            .iter()
            .map(|header| header.work())
            .reduce(|acc, next| acc + next)
            .ok_or(HeaderSyncError::FloatingHeaders)?;
        let stem_position = self
            .height_of_hash(
                uncommon
                    .first()
                    .ok_or(HeaderSyncError::FloatingHeaders)?
                    .prev_blockhash,
            )
            .await;
        if let Some(stem) = stem_position {
            let current_chainwork = self.header_chain.chainwork_after_height(stem);
            if current_chainwork.lt(&challenge_chainwork) {
                crate::log!(self.dialog, "Valid reorganization found");
                let reorged = self.header_chain.extend(&uncommon);
                let removed_hashes = &reorged
                    .iter()
                    .map(|disconnect| disconnect.header.block_hash())
                    .collect::<Vec<BlockHash>>();
                self.clear_compact_filter_queue();
                self.cf_header_chain.remove(removed_hashes);
                self.cf_header_chain.save(self.cfheader_chain_path.clone())
                    .await.map_err(|_| HeaderSyncError::DbError)?;
                self.dialog.send_event(Event::BlocksDisconnected(reorged));
                self.flush_over_height(stem).await;
                self.reset_tip(AdvanceKind::Filters).await.map_err(|_| HeaderSyncError::DbError)?;
                self.reset_tip(AdvanceKind::Blocks).await.map_err(|_| HeaderSyncError::DbError)?;
                self.median_timestamps.clear();
                self.median_last_height = 0;
                Ok(removed_hashes.clone())
            } else {
                self.dialog.send_warning(Warning::UnexpectedSyncError {
                    warning: "Peer sent us a fork with less work than the current chain".into(),
                });
                Err(HeaderSyncError::LessWorkFork)
            }
        } else {
            Err(HeaderSyncError::FloatingHeaders)
        }
    }

    async fn audit_difficulty(
        &mut self,
        height_start: u32,
        batch: &HeadersBatch,
    ) -> Result<(), HeaderSyncError> {
        let params = self.network.params();
        if params.no_pow_retargeting {
            return Ok(());
        }
        if params.allow_min_difficulty_blocks {
            return Ok(());
        }
        // Next adjustment height = (floor(current height / interval) + 1) * interval
        let adjustment_interval = params.difficulty_adjustment_interval() as u32;
        let next_multiple = (height_start / adjustment_interval) + 1;
        let next_adjustment_height = next_multiple * adjustment_interval;
        // The height in the batch where the next adjustment is contained
        let offset = next_adjustment_height - height_start;
        // We already audited the difficulty last batch
        if offset == 0 {
            return Ok(());
        }
        // We cannot audit the difficulty yet, as the next adjustment will be contained in the next batch
        if offset > batch.len() as u32 {
            if let Some(tip) = self.cached_header_at_height(height_start) {
                if batch.inner().iter().any(|header| header.bits.ne(&tip.bits)) {
                    self.dialog
                        .send_warning(Warning::UnexpectedSyncError {
                            warning:
                            "The remote peer miscalculated the difficulty adjustment when syncing a batch of headers"
                                .into(),
                        });
                    return Err(HeaderSyncError::MiscalculatedDifficulty);
                }
            }
            return Ok(());
        }
        // The difficulty should be adjusted at this height
        let audit_index = (offset - 1) as usize;
        // This is the timestamp used to start the boundary
        let last_epoch_start_index = next_adjustment_height - adjustment_interval;
        // This is the timestamp used to end the boundary
        let last_epoch_boundary = if offset == 1 {
            // This is the case where the last epoch ends on the tip of our chain
            self.fetch_header(height_start).await.ok().flatten()
        } else {
            // Otherwise we can simply index into the batch and find the header at the boundary'
            let last_epoch_boundary_index = (offset - 2) as usize;
            batch.get(last_epoch_boundary_index).copied()
        };
        // The start of the epoch will always be a member of our chain because the batch size
        // is less than the adjustment interval
        let last_epoch_start = self
            .fetch_header(last_epoch_start_index)
            .await
            .ok()
            .flatten();

        let audit = batch.get_slice(audit_index..);

        match audit {
            Some(headers) => match last_epoch_start.zip(last_epoch_boundary) {
                Some((first, second)) => {
                    let target =
                        CompactTarget::from_header_difficulty_adjustment(first, second, params);
                    for header in headers {
                        let retarget_bits = header.bits;
                        if retarget_bits.ne(&target) {
                            self.dialog
                                .send_warning(Warning::UnexpectedSyncError {
                                    warning:
                                    "The remote peer miscalculated the difficulty adjustment when syncing a batch of headers"
                                        .into(),
                                });
                            return Err(HeaderSyncError::MiscalculatedDifficulty);
                        }
                    }
                    return Ok(());
                }
                None => {
                    crate::log!(
                        self.dialog,
                        "Unable to audit difficulty. This is likely due to no history present in the header store"
                    );
                }
            },
            None => {
                self.dialog.send_warning(Warning::UnexpectedSyncError {
                    warning: "Unable to audit the difficulty adjustment due to an index overflow"
                        .into(),
                });
            }
        }
        Ok(())
    }

    // We don't have a header in memory that we need to evaluate a fork.
    // We check if we have it on disk, and load some more headers into memory.
    // This call occurs if we sync to a block that is later reorganized out of the chain,
    // but we have restarted our node in between these events.
    async fn load_fork(&mut self, header_batch: &HeadersBatch) -> Result<(), HeaderSyncError> {
        let prev_hash = header_batch.first().prev_blockhash;
        let maybe_height = {
            let mut db_lock = self.db.lock().await;
            db_lock
                .height_of(&prev_hash)
                .await
                .map_err(|_| HeaderSyncError::DbError)?
        };
        match maybe_height {
            Some(height) => {
                // This is a very generous check to ensure a peer cannot get us to load an
                // absurd amount of headers into RAM. Because headers come in batches of 2,000,
                // we wouldn't accept a fork of a depth more than around 2,000 anyway.
                // The only reorgs that have ever been recorded are of depth 1.
                if self.height() - height > MAX_REORG_DEPTH {
                    return Err(HeaderSyncError::FloatingHeaders);
                } else {
                    let older_anchor = HeaderCheckpoint::new(height, prev_hash);
                    self.header_chain = HeaderChain::new(older_anchor);
                    self.cf_header_chain =
                        CFHeaderChain::new(older_anchor, self.cf_header_chain.quorum_required());
                    self.cf_header_chain.save(self.cfheader_chain_path.clone()).await
                        .map_err(|_| HeaderSyncError::DbError)?;
                    self.reset_tip(AdvanceKind::Filters).await
                        .map_err(|_| HeaderSyncError::DbError)?;
                    self.reset_tip(AdvanceKind::Blocks).await
                        .map_err(|_| HeaderSyncError::DbError)?;
                }
            }
            None => return Err(HeaderSyncError::FloatingHeaders),
        }
        self.load_headers()
            .await
            .map_err(|_| HeaderSyncError::DbError)?;
        Ok(())
    }

    // Sync the compact filter headers, possibly encountering conflicts
    pub(crate) async fn sync_cf_headers(
        &mut self,
        _peer_id: u32,
        cf_headers: CFHeaders,
    ) -> Result<AppendAttempt, CFHeaderSyncError> {
        let mut batch: CFHeaderBatch = cf_headers.into();
        let peer_max = self.heights.lock().await.max();
        self.dialog
            .chain_update(
                self.height(),
                self.cf_header_chain.height(),
                peer_max.unwrap_or(self.height()),
            )
            .await;
        match batch.last_header() {
            Some(batch_last) => {
                if let Some(prev_header) = self.cf_header_chain.prev_header() {
                    // A new block was mined and we ended up asking for this batch twice,
                    // or the quorum required is less than our connected peers.
                    if batch_last.eq(&prev_header) {
                        return Ok(AppendAttempt::AddedToQueue);
                    }
                }
            }
            None => return Err(CFHeaderSyncError::EmptyMessage),
        }
        // Check for any obvious faults
        self.audit_cf_headers(&batch).await?;
        // We already have a message like this. Verify they are the same
        match self.cf_header_chain.merged_queue.take() {
            Some(queue) => Ok(self.cf_header_chain.verify(&mut batch, queue)),
            None => {
                let queue = self.construct_cf_header_queue(&mut batch).await?;
                Ok(self.cf_header_chain.set_queue(queue))
            }
        }
    }

    // We need to associate the block hash with the incoming filter hashes
    async fn construct_cf_header_queue(
        &self,
        batch: &mut CFHeaderBatch,
    ) -> Result<Vec<QueuedCFHeader>, CFHeaderSyncError> {
        let mut queue = Vec::new();
        let ref_height = self.cf_header_chain.height();
        for (index, (filter_header, filter_hash)) in batch.take_inner().into_iter().enumerate() {
            let block_hash = self
                // This call may or may not retrieve the hash from disk
                .blockhash_at_height(ref_height + index as u32 + 1)
                .await
                .ok_or(CFHeaderSyncError::HeaderChainIndexOverflow)?;
            queue.push(QueuedCFHeader::new(block_hash, filter_header, filter_hash))
        }
        Ok(queue)
    }

    // Audit the validity of a batch of compact filter headers
    async fn audit_cf_headers(&mut self, batch: &CFHeaderBatch) -> Result<(), CFHeaderSyncError> {
        // Does the filter header line up with our current chain of filter headers
        if let Some(prev_header) = self.cf_header_chain.prev_header() {
            if batch.prev_header().ne(&prev_header) {
                return Err(CFHeaderSyncError::PrevHeaderMismatch);
            }
        }
        // Did we request up to this stop hash. We should have caught if this was a repeated message.
        let prev_stophash = self
            .cf_header_chain
            .last_stop_hash_request()
            .ok_or(CFHeaderSyncError::UnexpectedCFHeaderMessage)?;
        if prev_stophash.ne(batch.stop_hash()) {
            return Err(CFHeaderSyncError::StopHashMismatch);
        }
        // Did they send us the right amount of headers
        let stop_hash =
            // This call may or may not retrieve the hash from disk
            self.blockhash_at_height(self.cf_header_chain.height() + batch.len() as u32)
                .await
                .ok_or(CFHeaderSyncError::HeaderChainIndexOverflow)?;
        if stop_hash.ne(batch.stop_hash()) {
            return Err(CFHeaderSyncError::StopHashMismatch);
        }
        Ok(())
    }

    // We need to make this public for new peers that connect to us throughout syncing the filter headers
    pub(crate) async fn next_cf_header_message(&mut self) -> GetCFHeaders {
        let stop_hash_index = self.cf_header_chain.height() + CF_HEADER_BATCH_SIZE + 1;
        let stop_hash = self
            .blockhash_at_height(stop_hash_index)
            .await
            .unwrap_or(self.tip());
        self.cf_header_chain.set_last_stop_hash(stop_hash);

        self.dialog
            .send_dialog(format!("next_cf_header_message: {}, start height: {}", stop_hash_index,
                                 self.cf_header_chain.height() + 1)).await;

        GetCFHeaders {
            filter_type: FILTER_BASIC,
            start_height: self.cf_header_chain.height() + 1,
            stop_hash,
        }
    }

    pub(crate) fn cf_headers_height(&self) -> u32 {
        std::cmp::min(
            self.cf_header_chain.height(),
            self.prune_height.unwrap_or(self.cf_header_chain.height()),
        )
    }

    pub(crate) fn is_cf_headers_synced(&self) -> bool {
        self.prune_height.unwrap_or(self.height()).le(&self.cf_header_chain.height())
    }

    pub(crate) async fn save_cf_headers(&self) -> anyhow::Result<()> {
        self.cf_header_chain.save(self.cfheader_chain_path.clone()).await
    }

    // Add a script to our list
    pub(crate) fn put_script(&mut self, script: ScriptBuf) {
        self.scripts.insert(script);
    }

    pub(crate) async fn fetch_header_range(
        &self,
        range: Range<u32>,
    ) -> Result<BTreeMap<u32, Header>, HeaderPersistenceError<H::Error>> {
        let mut db = self.db.lock().await;
        let range_opt = db.load(range).await;
        if range_opt.is_err() {
            self.dialog.send_warning(Warning::FailedPersistence {
                warning: "Unexpected error fetching a range of headers from the header store"
                    .to_string(),
            });
        }
        range_opt.map_err(HeaderPersistenceError::Database)
    }

    // Reset the compact filter queue because we received a new block
    pub(crate) fn clear_compact_filter_queue(&mut self) {
        self.cf_header_chain.clear_queue();
    }

    // We found a reorg and some filters are no longer valid.
    async fn clear_filter_headers(&mut self) {
        self.cf_header_chain.clear_queue();
        self.cf_header_chain.clear_headers();
    }

    // Clear the filter header cache to rescan the filters for new scripts.
    pub(crate) fn clear_filters(&mut self) {
        // self.filter_chain.clear_cache();
    }
}

fn calculate_median(timestamps: &VecDeque<u32>) -> u32 {
    if timestamps.is_empty() {
        return 0;
    }

    let mut sorted: Vec<u32> = timestamps.iter().copied().collect();
    sorted.sort_unstable();

    let len = sorted.len();
    if len % 2 == 0 {
        (sorted[len / 2 - 1] + sorted[len / 2]) / 2
    } else {
        sorted[len / 2]
    }
}
