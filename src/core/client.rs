use bitcoin::BlockHash;
use bitcoin::{Block, Transaction, Txid};
use bitcoin::{block::Header, FeeRate};
use std::{collections::BTreeMap, ops::Range, time::Duration};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use crate::{BlockchainInfo, Event, IndexedFilter, Log, TrustedPeer, TxBroadcast, Warning};
use crate::core::messages::{BlockFilterByHeightRequest, BlockchainInfoRequest, HeaderByHashRequest, MempoolEntryRequest, PruneBlockchainRequest, QueueBlocksRequest};
use crate::rpc::server::MempoolEntryResult;
use super::{error::DownloadRequestError, messages::BlockRequest};
use super::{
    error::{ClientError, FetchFeeRateError, FetchHeaderError},
    messages::{BatchHeaderRequest, ClientMessage, HeaderRequest},
};

/// A [`Client`] allows for communication with a running node.
#[derive(Debug)]
pub struct Client {
    /// Send events to a node, such as broadcasting a transaction.
    pub requester: Requester,
    /// Receive log messages from a node.
    pub log_rx: mpsc::Receiver<Log>,
    /// Receive warning messages from a node.
    pub warn_rx: mpsc::UnboundedReceiver<Warning>,
    /// Receive [`Event`] from a node to act on.
    pub event_rx: mpsc::UnboundedReceiver<Event>,
}

impl Client {
    pub(crate) fn new(
        log_rx: mpsc::Receiver<Log>,
        warn_rx: mpsc::UnboundedReceiver<Warning>,
        event_rx: mpsc::UnboundedReceiver<Event>,
        ntx: Sender<ClientMessage>,
    ) -> Self {
        let requester = Requester::new(ntx);
        Self {
            requester: requester.clone(),
            log_rx,
            warn_rx,
            event_rx,
        }
    }
}

/// Send messages to a node that is running so the node may complete a task.
#[derive(Debug, Clone)]
pub struct Requester {
    ntx: Sender<ClientMessage>,
}

impl Requester {
    fn new(ntx: Sender<ClientMessage>) -> Self {
        Self { ntx }
    }

    /// Tell the node to shut down.
    ///
    /// # Errors
    ///
    /// If the node has already stopped running.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        self.ntx
            .send(ClientMessage::Shutdown)
            .await
            .map_err(|_| ClientError::SendError)
    }

    /// Tell the node to shut down from a synchronus context.
    ///
    /// # Errors
    ///
    /// If the node has already stopped running.
    ///
    /// # Panics
    ///
    /// When called within an asynchronus context (e.g `tokio::main`).
    pub fn shutdown_blocking(&self) -> Result<(), ClientError> {
        self.ntx
            .blocking_send(ClientMessage::Shutdown)
            .map_err(|_| ClientError::SendError)
    }

    /// Broadcast a new transaction to the network.
    ///
    /// # Note
    ///
    /// When broadcasting a one-parent one-child (TRUC) package,
    /// broadcast the child first, followed by the parent.
    ///
    /// Package relay is under-development at the time of writing.
    ///
    /// For more information, see BIP-431 and BIP-331.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn broadcast_tx(&self, tx: TxBroadcast) -> Result<(), ClientError> {
        self.ntx
            .send(ClientMessage::Broadcast(tx))
            .await
            .map_err(|_| ClientError::SendError)
    }

    pub async fn get_mempool_entry(&self, txid: Txid) -> Result<Option<MempoolEntryResult>, ClientError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Option<MempoolEntryResult>>();
        let message = MempoolEntryRequest {oneshot: tx, txid};

        self.ntx
            .send(ClientMessage::GetMempoolEntry(message))
            .await
            .map_err(|_| ClientError::SendError)?;
        Ok(rx.await.map_err(|_| ClientError::SendError)?)
    }

    pub async fn queue_blocks(&self, heights: Vec<u32>) -> Result<(), ClientError> {
        let (tx, rx) =
            tokio::sync::oneshot::channel::<anyhow::Result<()>>();
        self.ntx
            .send(ClientMessage::QueueBlocks(QueueBlocksRequest {
                oneshot: tx,
                blocks: heights,
            }))
            .await
            .map_err(|_| ClientError::SendError).map_err(|_| ClientError::SendError)?;

        Ok(rx.await.map_err(|e| ClientError::Custom(e.to_string()))?
            .map_err(|e| ClientError::Custom(e.to_string()))?)
    }

    /// Broadcast a new transaction to the network to a random peer.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn broadcast_random(&self, tx: Transaction) -> Result<(), ClientError> {
        let tx_broadcast = TxBroadcast::random_broadcast(tx);
        self.ntx
            .send(ClientMessage::Broadcast(tx_broadcast))
            .await
            .map_err(|_| ClientError::SendError)
    }



    /// Broadcast a new transaction to the network from a synchronus context.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    ///
    /// # Panics
    ///
    /// When called within an asynchronus context (e.g `tokio::main`).
    pub fn broadcast_tx_blocking(&self, tx: TxBroadcast) -> Result<(), ClientError> {
        self.ntx
            .blocking_send(ClientMessage::Broadcast(tx))
            .map_err(|_| ClientError::SendError)
    }

    /// A connection has a minimum transaction fee requirement to enter its mempool. For proper transaction propagation,
    /// transactions should have a fee rate at least as high as the maximum fee filter received.
    /// This method returns the maximum fee rate requirement of all connected peers.
    ///
    /// For more information, refer to BIP133
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn broadcast_min_feerate(&self) -> Result<FeeRate, FetchFeeRateError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<FeeRate>();
        self.ntx
            .send(ClientMessage::GetBroadcastMinFeeRate(tx))
            .await
            .map_err(|_| FetchFeeRateError::SendError)?;
        rx.await.map_err(|_| FetchFeeRateError::RecvError)
    }

    /// Get a header at the specified height, if it exists.
    ///
    /// # Note
    ///
    /// The height of the chain is the canonical index of the header in the chain.
    /// For example, the genesis block is at a height of zero.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn get_header(&self, height: u32) -> Result<Header, FetchHeaderError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<Header, FetchHeaderError>>();
        let message = HeaderRequest::new(tx, height);
        self.ntx
            .send(ClientMessage::GetHeader(message))
            .await
            .map_err(|_| FetchHeaderError::SendError)?;
        rx.await.map_err(|_| FetchHeaderError::RecvError)?
    }

    pub async fn get_header_by_hash(&self, hash: BlockHash) -> Result<Header, FetchHeaderError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<Header, FetchHeaderError>>();
        let message = HeaderByHashRequest::new(tx, hash);
        self.ntx
            .send(ClientMessage::GetHeaderByHash(message))
            .await
            .map_err(|_| FetchHeaderError::SendError)?;
        rx.await.map_err(|_| FetchHeaderError::RecvError)?
    }

    pub async fn get_blockchain_info(&self) -> Result<BlockchainInfo, FetchHeaderError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<BlockchainInfo, FetchHeaderError>>();
        let message = BlockchainInfoRequest {oneshot: tx};
        self.ntx
            .send(ClientMessage::GetBlockchainInfo(message))
            .await
            .map_err(|_| FetchHeaderError::SendError)?;
        rx.await.map_err(|_| FetchHeaderError::RecvError)?
    }

    pub async fn get_block_filter(&self, height: u32) -> Result<Option<IndexedFilter>, FetchHeaderError> {
        let (tx, rx) =
            tokio::sync::oneshot::channel::<Result<Option<IndexedFilter>, FetchHeaderError>>();
        let message = BlockFilterByHeightRequest {oneshot: tx, height};
        self.ntx
            .send(ClientMessage::GetBlockFilterByHeight(message))
            .await
            .map_err(|_| FetchHeaderError::SendError)?;
        rx.await.map_err(|_| FetchHeaderError::RecvError)?
    }

    /// Get a header at the specified height in a synchronus context, if it exists.
    ///
    /// # Note
    ///
    /// The height of the chain is the canonical index of the header in the chain.
    /// For example, the genesis block is at a height of zero.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    ///
    /// # Panics
    ///
    /// When called within an asynchronus context (e.g `tokio::main`).
    pub fn get_header_blocking(&self, height: u32) -> Result<Header, FetchHeaderError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<Header, FetchHeaderError>>();
        let message = HeaderRequest::new(tx, height);
        self.ntx
            .blocking_send(ClientMessage::GetHeader(message))
            .map_err(|_| FetchHeaderError::SendError)?;
        rx.blocking_recv()
            .map_err(|_| FetchHeaderError::RecvError)?
    }

    /// Get a range of headers by the specified range.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn get_header_range(
        &self,
        range: Range<u32>,
    ) -> Result<BTreeMap<u32, Header>, FetchHeaderError> {
        let (tx, rx) =
            tokio::sync::oneshot::channel::<Result<BTreeMap<u32, Header>, FetchHeaderError>>();
        let message = BatchHeaderRequest::new(tx, range);
        self.ntx
            .send(ClientMessage::GetHeaderBatch(message))
            .await
            .map_err(|_| FetchHeaderError::SendError)?;
        rx.await.map_err(|_| FetchHeaderError::RecvError)?
    }

    pub async fn get_block(&self, hash: BlockHash) -> Result<Block, DownloadRequestError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<Block, DownloadRequestError>>();

        let request = BlockRequest::new(tx, hash);

        self.ntx
            .send(ClientMessage::GetBlock(vec![request]))
            .await
            .map_err(|_| DownloadRequestError::SendError)?;
        rx.await.map_err(|_| DownloadRequestError::RecvError)?
    }

    /// Starting at the configured anchor checkpoint, look for block inclusions with newly added scripts.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn rescan(&self) -> Result<(), ClientError> {
        self.ntx
            .send(ClientMessage::Rescan)
            .await
            .map_err(|_| ClientError::SendError)
    }

    /// Set a new connection timeout for peers to respond to messages.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn set_response_timeout(&self, duration: Duration) -> Result<(), ClientError> {
        self.ntx
            .send(ClientMessage::SetDuration(duration))
            .await
            .map_err(|_| ClientError::SendError)
    }

    /// Add another known peer to connect to.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn add_peer(&self, peer: impl Into<TrustedPeer>) -> Result<(), ClientError> {
        self.ntx
            .send(ClientMessage::AddPeer(peer.into()))
            .await
            .map_err(|_| ClientError::SendError)
    }

    /// Explicitly start the block filter syncing process. Note that the node will automatically download and check
    /// filters unless the policy is to explicitly halt.
    ///
    /// # Errors
    ///
    /// If the node has stopped running.
    pub async fn continue_download(&self) -> Result<(), ClientError> {
        self.ntx
            .send(ClientMessage::ContinueDownload)
            .await
            .map_err(|_| ClientError::SendError)
    }

    /// Prune all blocks up to and including the given height.
    pub async fn prune_blockchain(&self, height: u32) -> Result<u32, ClientError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<u32>>();
        self.ntx
            .send(ClientMessage::PruneBlockchain(PruneBlockchainRequest {
                oneshot: tx,
                height,
            }))
            .await
            .map_err(|_| ClientError::SendError)?;
        rx.await
            .map_err(|_| ClientError::SendError)?
            .map_err(|e| ClientError::Custom(e.to_string()))
    }

    /// Check if the node is running.
    pub async fn is_running(&self) -> bool {
        self.ntx.send(ClientMessage::NoOp).await.is_ok()
    }
}



impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ClientError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        ClientError::SendError
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::{consensus::deserialize, Transaction};
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn test_client_works() {
        let transaction: Transaction = deserialize(&hex::decode("0200000001aad73931018bd25f84ae400b68848be09db706eac2ac18298babee71ab656f8b0000000048473044022058f6fc7c6a33e1b31548d481c826c015bd30135aad42cd67790dab66d2ad243b02204a1ced2604c6735b6393e5b41691dd78b00f0c5942fb9f751856faa938157dba01feffffff0280f0fa020000000017a9140fb9463421696b82c833af241c78c17ddbde493487d0f20a270100000017a91429ca74f8a08f81999428185c97b5d852e4063f618765000000").unwrap()).unwrap();
        let (log_tx, log_rx) = tokio::sync::mpsc::channel::<Log>(1);
        let (_, warn_rx) = tokio::sync::mpsc::unbounded_channel::<Warning>();
        let (_, event_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
        let (ctx, crx) = mpsc::channel::<ClientMessage>(10_000_000);
        let Client {
            requester,
            mut log_rx,
            warn_rx: _,
            event_rx: _,
        } = Client::new(log_rx, warn_rx, event_rx, ctx);
        let send_res = log_tx.send(Log::Debug("An important message".into())).await;
        assert!(send_res.is_ok());
        let message = log_rx.recv().await;
        assert!(message.is_some());
        tokio::task::spawn(async move {
            log_tx
                .send(Log::Debug("Another important message".into()))
                .await
        });
        assert!(send_res.is_ok());
        let message = log_rx.recv().await;
        assert!(message.is_some());
        drop(log_rx);
        let broadcast = requester
            .broadcast_tx(TxBroadcast::new(
                transaction.clone(),
                crate::TxBroadcastPolicy::AllPeers,
            ))
            .await;
        assert!(broadcast.is_ok());
        drop(crx);
        let broadcast = requester.shutdown().await;
        assert!(broadcast.is_err());
    }
}
