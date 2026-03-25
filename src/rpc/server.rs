use bitcoin::block::Header;
use bitcoin::{BlockHash, Transaction, Txid};
use jsonrpsee::{
    core::async_trait,
    proc_macros::rpc,
    types::ErrorObjectOwned,
};
use crate::{BlockchainInfo, Requester, TxBroadcast, TxBroadcastPolicy};

fn serialize_hex<S>(bytes: &Vec<u8>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(hex::encode(bytes).as_str())
}

fn deserialize_hex<'de, D>(d: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    hex::decode(s).map_err(D::Error::custom)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockFilterRpc {
    pub hash:   BlockHash,
    pub height: u32,
    #[serde(
        serialize_with   = "serialize_hex",
        deserialize_with = "deserialize_hex"
    )]
    pub content: Vec<u8>,
}

#[rpc(server, client)]
pub trait Rpc {
    #[method(name = "getblock")]
    async fn get_block(&self, block_hash: BlockHash, verbosity: usize) -> Result<String, ErrorObjectOwned>;

    #[method(name = "getblockhash")]
    async fn get_block_hash(&self, height: u32) -> Result<BlockHash, ErrorObjectOwned>;

    #[method(name = "getblockcount")]
    async fn get_block_count(&self) -> Result<u32, ErrorObjectOwned>;

    #[method(name = "getblockchaininfo")]
    async fn get_blockchain_info(&self) -> Result<BlockchainInfo, ErrorObjectOwned>;

    #[method(name = "getblockfilterbyheight")]
    async fn get_block_filter_by_height(&self, height: u32) -> Result<Option<BlockFilterRpc>, ErrorObjectOwned>;

    #[method(name = "queueblocks")]
    async fn queue_blocks(&self, heights: Vec<u32>) -> Result<(), ErrorObjectOwned>;

    #[method(name = "getblockheader")]
    async fn get_block_header(&self, block_hash: BlockHash) -> Result<Header, ErrorObjectOwned>;

    #[method(name = "sendrawtransaction")]
    async fn send_raw_transaction(&self, hex_string: String, max_fee_rate: Option<u32>, max_burn: Option<u32>) -> Result<Txid, ErrorObjectOwned>;

    #[method(name = "getmempoolentry")]
    async fn get_mempool_entry(&self, txid: Txid) -> Result<Option<MempoolEntryResult>, ErrorObjectOwned>;


    #[method(name = "queuefilters")]
    async fn queue_filters(&self) -> Result<(), ErrorObjectOwned>;

    #[method(name = "pruneblockchain")]
    async fn prune_blockchain(&self, height: u32) -> Result<u32, ErrorObjectOwned>;
}

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolEntryResult {
    #[serde(rename = "vsize")]
    pub virtual_size: u32,
    pub weight: u32,
    pub time: u64,
    pub height: u32,
    #[serde(rename = "descendantcount")]
    pub descendant_count: u32,
    #[serde(rename = "descendantsize")]
    pub descendant_size: u32,
    #[serde(rename = "ancestorcount")]
    pub ancestor_count: u32,
    #[serde(rename = "ancestorsize")]
    pub ancestor_size: u32,
    #[serde(rename = "ancestorfees")]
    pub wtxid: String,
    pub fees: Fees,
    #[serde(rename = "depends")]
    pub dependencies: Vec<String>,
    #[serde(rename = "spentby")]
    pub spent_by: Vec<String>,
    #[serde(rename = "bip125-replaceable")]
    pub bip125_replaceable: bool,
    pub unbroadcast: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fees {
    #[serde(rename = "base")]
    pub base_fee: i64,
    #[serde(rename = "modified")]
    pub modified_fee: i64,
    #[serde(rename = "ancestor")]
    pub ancestor_fee: i64,
    #[serde(rename = "descendant")]
    pub descendant_fee: i64,
}

impl Default for Fees {
    fn default() -> Self {
        Self {
            base_fee: 0,
            modified_fee: 0,
            ancestor_fee: 0,
            descendant_fee: 0,
        }
    }
}

impl Default for MempoolEntryResult {
    fn default() -> Self {
        Self {
            virtual_size: 0,
            weight: 0,
            time: 0,
            height: 0,
            descendant_count: 0,
            descendant_size: 0,
            ancestor_count: 0,
            ancestor_size: 0,
            wtxid: String::new(),
            fees: Fees::default(),
            dependencies: Vec::new(),
            spent_by: Vec::new(),
            bip125_replaceable: false,
            unbroadcast: false,
        }
    }
}


#[derive(Clone)]
pub struct RpcServerImpl {
    pub requester: Requester,
    // Specify an additional endpoint to broadcast
    // transactions to for mempool acceptance checks
    pub broadcast_endpoint: Option<String>,
    pub(crate) broadcast_client: reqwest::Client,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    async fn get_block(&self, block_hash: BlockHash, _verbosity: usize) -> Result<String, ErrorObjectOwned> {
       let block =  self.requester.get_block(block_hash).await
            .map_err(|e| ErrorObjectOwned::owned(-1, format!("getblock: {}:{}", block_hash, e.to_string()), None::<String>))?;
        let raw = tokio::task::spawn_blocking(move || {
            bitcoin::consensus::encode::serialize_hex(&block)
        }).await.expect("result");
       Ok(raw)
    }

    async fn get_block_hash(&self, height: u32) -> Result<BlockHash, ErrorObjectOwned> {
        let header = self.requester.get_header(height).await
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))?;
        Ok(header.block_hash())
    }

    async fn get_blockchain_info(&self) -> Result<BlockchainInfo, ErrorObjectOwned> {
        self.requester.get_blockchain_info().await
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))
    }

    async fn get_block_header(&self, block_hash: BlockHash) -> Result<Header, ErrorObjectOwned> {
        self.requester.get_header_by_hash(block_hash).await
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))
    }

    async fn get_block_filter_by_height(&self, height: u32) -> Result<Option<BlockFilterRpc>, ErrorObjectOwned> {
        self.requester.get_block_filter(height).await
            .map(|f| f.map(|idx| BlockFilterRpc {
                hash: *idx.filter.block_hash(),
                height,
                content: idx.filter.to_bytes(),
            }))
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))
    }

    async fn send_raw_transaction(&self, hex_string: String, _max_fee_rate: Option<u32>, _max_burn: Option<u32>) -> Result<Txid, ErrorObjectOwned> {
        // If a broadcast endpoint is set, test mempool acceptance to provide timely errors to the client.
        // If the endpoint is unavailable, skip the check for robustness.
        // Non-accepted transactions will eventually be dropped from our mempool.
        self.test_mempool_accept(&hex_string).await?;

        let tx: Transaction =  bitcoin::consensus::encode::deserialize_hex(&hex_string)
            .map_err(|_| ErrorObjectOwned::owned(-1, "Invalid transaction", None::<String>))?;

        let txid = tx.compute_txid();
        let broadcast = TxBroadcast {
            tx,
            broadcast_policy: TxBroadcastPolicy::AllPeers,
        };
        self.requester.broadcast_tx(broadcast).await
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))?;
        Ok(txid)
    }

    async fn get_mempool_entry(&self, txid: Txid) -> Result<Option<MempoolEntryResult>, ErrorObjectOwned> {
        self.requester.get_mempool_entry(txid)
            .await.map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))
    }

    async fn queue_blocks(&self, heights: Vec<u32>) -> Result<(), ErrorObjectOwned> {
        self.requester.queue_blocks(heights).await
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))
    }

    async fn queue_filters(&self) -> Result<(), ErrorObjectOwned> {
        self.requester.continue_download().await
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))
    }

    async fn prune_blockchain(&self, height: u32) -> Result<u32, ErrorObjectOwned> {
        self.requester.prune_blockchain(height).await
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))
    }

    async fn get_block_count(&self) -> Result<u32, ErrorObjectOwned> {
        let info = self.requester.get_blockchain_info().await
            .map_err(|e| ErrorObjectOwned::owned(-1, e.to_string(), None::<String>))?;

        if !info.headers_synced ||
            info.headers <= info.checkpoint.height ||
            info.prune_height.is_some_and(|h| info.headers <= h) {
            return Err(ErrorObjectOwned::owned(-1, "Headers are still syncing", None::<String>));
        }
        Ok(info.headers)
    }
}

impl RpcServerImpl {
    async fn test_mempool_accept(&self, tx_hex: &str) -> Result<(), ErrorObjectOwned> {
        let endpoint = match &self.broadcast_endpoint {
            Some(url) => url,
            None => return Ok(()),
        };

        // Send request to broadcast endpoint
        let response = self.broadcast_client
            .post(endpoint)
            .body(tx_hex.to_string())
            .send()
            .await;

        match response {
            Ok(resp) => {
                let text = match resp.text().await {
                    Ok(text) => text,
                    Err(e) => {
                        tracing::warn!("test mempool accept: could not read response body: {}", e);
                        return Ok(())
                    }
                };

                // Check if response starts with "sendrawtransaction RPC error"
                if text.starts_with("sendrawtransaction RPC error:") {
                    // Extract JSON part: remove prefix and parse
                    let json_str = text.trim_start_matches("sendrawtransaction RPC error: ");
                    match serde_json::from_str::<serde_json::Value>(json_str) {
                        Ok(error_json) => {
                            let code = error_json["code"].as_i64()
                                .unwrap_or(-25) as i32;
                            let message = error_json["message"].as_str()
                                .unwrap_or("Unknown error")
                                .to_string();
                            return Err(ErrorObjectOwned::owned(
                                code,
                                message,
                                None::<String>
                            ));
                        }
                        Err(e) => {
                            tracing::warn!("test mempool accept: could not parse error '{}': {}", json_str, e);
                            return Ok(());
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                tracing::warn!("test mempool accept: could not perform check: {}", e);
                Ok(())
            }
        }
    }
}
