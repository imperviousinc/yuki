use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use bitcoin::{Block, BlockHash, Network};
use bitcoin::consensus::{serialize, deserialize};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use tokio::sync::Mutex;

use crate::prelude::FutureResult;
use crate::db::error::SqlInitializationError;
use crate::HeaderCheckpoint;

#[derive(Debug)]
pub enum SqlBlocksStoreError {
    SQL(rusqlite::Error),
    StringConversion,
    DeserializeError(String),
}

impl From<rusqlite::Error> for SqlBlocksStoreError {
    fn from(err: rusqlite::Error) -> Self {
        SqlBlocksStoreError::SQL(err)
    }
}

// --- BlocksStore Trait ---

pub trait BlocksStore : Debug + Send + Sync  {
    type Error;

    /// Insert multiple blocks in a single commit.
    /// The map's key is the block height and the value is the block.
    fn insert_blocks(
        &mut self,
        blocks: BTreeMap<u32, Block>,
    ) -> FutureResult<'_, (), Self::Error>;

    /// Get the block for a given block_hash.
    fn get_block_by_hash<'a>(
        &'a mut self,
        block_hash: &'a BlockHash,
    ) -> FutureResult<'a, Option<Block>, Self::Error>;

    /// Get the block stored at a specific height.
    fn get_block_by_height<'a>(
        &'a mut self,
        height: u32,
    ) -> FutureResult<'a, Option<Block>, Self::Error>;

    fn get_block_hash(
        &mut self,
        height: u32,
    ) -> FutureResult<'_, Option<BlockHash>, Self::Error>;

    /// Load blocks within a given height range.
    /// Returns a map from height to (BlockHash, Block).
    fn load_blocks<'a>(
        &'a mut self,
        range: impl RangeBounds<u32> + Send + Sync + 'a + 'static,
    ) -> FutureResult<'a, BTreeMap<u32, (BlockHash, Block)>, Self::Error>;

    /// Persist the current blocks tip.
    fn set_tip(
        &mut self,
        tip: HeaderCheckpoint,
    ) -> FutureResult<'_, (), Self::Error>;

    /// Load the current blocks tip.
    fn get_tip(&mut self) -> FutureResult<'_, Option<HeaderCheckpoint>, Self::Error>;

    /// Delete all blocks with height less than or equal to the given height.
    fn prune_up_to_height(
        &mut self,
        height: u32,
    ) -> FutureResult<'_, u32, Self::Error>;
}

// --- SQLite Implementation ---

const FILE_NAME: &str = "blocks.db";
const BLOCKS_TABLE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS blocks (
    block_hash TEXT PRIMARY KEY,
    height INTEGER NOT NULL,
    block BLOB NOT NULL
) STRICT";

const META_TABLE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT";

/// SQLite-backed block store.
#[derive(Debug)]
pub struct SqliteBlockDb {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteBlockDb {
    /// Create a new [`SqliteBlockDb`] using an optional path.
    pub fn new(network: Network, path: Option<PathBuf>) -> Result<Self, SqlInitializationError> {
        let mut path = path.unwrap_or_else(|| PathBuf::from("."));
        path.push("data");
        path.push(network.to_string());
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        let conn = Connection::open(path.join(FILE_NAME))?;
        conn.execute(BLOCKS_TABLE_SCHEMA, [])?;
        conn.execute(META_TABLE_SCHEMA, [])?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    async fn set_tip(&mut self, tip: HeaderCheckpoint) -> Result<(), SqlBlocksStoreError> {
        let conn = self.conn.clone();
        let tip_str = serialize_tip(&tip);
        tokio::task::spawn_blocking(move || -> Result<(), SqlBlocksStoreError> {
            let lock = conn.blocking_lock();
            lock.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('blocks_tip', ?1)", [tip_str])?;
            Ok(())
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn get_tip(&mut self) -> Result<Option<HeaderCheckpoint>, SqlBlocksStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<HeaderCheckpoint>, SqlBlocksStoreError> {
            let lock = conn.blocking_lock();
            let tip_str: Option<String> = lock
                .query_row("SELECT value FROM meta WHERE key = 'blocks_tip'", [], |row| row.get(0))
                .optional()?;
            if let Some(s) = tip_str {
                let tip = deserialize_tip(&s)?;
                Ok(Some(tip))
            } else {
                Ok(None)
            }
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn insert_blocks(
        &mut self,
        blocks: BTreeMap<u32, Block>,
    ) -> Result<(), SqlBlocksStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<(), SqlBlocksStoreError> {
            let mut write_lock = conn.blocking_lock();
            let tx = write_lock.transaction()?;
            let stmt = "INSERT OR REPLACE INTO blocks (block_hash, height, block) VALUES (?1, ?2, ?3)";
            for (height, block) in blocks {
                let block_hash = block.block_hash();
                let serialized_block = serialize(&block);
                tx.execute(stmt, params![block_hash.to_string(), height, serialized_block])?;
            }
            tx.commit()?;
            Ok(())
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn get_block_hash(&mut self, height: u32) -> Result<Option<BlockHash>, SqlBlocksStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<BlockHash>, SqlBlocksStoreError> {
            let write_lock = conn.blocking_lock();
            let stmt = "SELECT block_hash FROM blocks WHERE height = ?1";
            let hash_str: Option<String> = write_lock
                .query_row(stmt, params![height], |row| row.get(0))
                .optional()?;

            match hash_str {
                Some(hash) => {
                    let block_hash = BlockHash::from_str(&hash)
                        .map_err(|_| SqlBlocksStoreError::StringConversion)?;
                    Ok(Some(block_hash))
                }
                None => Ok(None),
            }
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn get_block_by_hash(
        &mut self,
        block_hash: &BlockHash,
    ) -> Result<Option<Block>, SqlBlocksStoreError> {
        let write_lock = self.conn.lock().await;
        let stmt = "SELECT block FROM blocks WHERE block_hash = ?1";
        let block_blob: Option<Vec<u8>> = write_lock
            .query_row(stmt, params![block_hash.to_string()], |row| row.get(0))
            .optional()?;
        if let Some(blob) = block_blob {
            let block: Block = tokio::task::spawn_blocking(move || {
                deserialize(&blob)
            }).await.expect("block deserialization failed")
                .map_err(|e| SqlBlocksStoreError::DeserializeError(e.to_string()))?;
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }

    async fn get_block_by_height(
        &mut self,
        height: u32,
    ) -> Result<Option<Block>, SqlBlocksStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<Block>, SqlBlocksStoreError> {
            // Use the blocking version of the lock.
            let write_lock = conn.blocking_lock();
            let stmt_str = "SELECT block FROM blocks WHERE height = ?1";
            let mut stmt = write_lock.prepare(stmt_str)?;
            let mut rows = stmt.query(params![height])?;
            if let Some(row) = rows.next()? {
                let blob: Vec<u8> = row.get(0)?;
                let block = deserialize(&blob)
                    .map_err(|e| SqlBlocksStoreError::DeserializeError(e.to_string()))?;
                Ok(Some(block))
            } else {
                Ok(None)
            }
        })
            .await
            .expect("spawn_blocking failed")
    }


    async fn prune_up_to_height(&mut self, height: u32) -> Result<u32, SqlBlocksStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<u32, SqlBlocksStoreError> {
            let lock = conn.blocking_lock();
            let deleted = lock.execute(
                "DELETE FROM blocks WHERE height <= ?1",
                params![height],
            )?;
            Ok(deleted as u32)
        })
        .await
        .expect("spawn_blocking failed")
    }

    async fn load_blocks(
        &mut self,
        range: impl RangeBounds<u32> + Send + Sync + 'static,
    ) -> Result<BTreeMap<u32, (BlockHash, Block)>, SqlBlocksStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<BTreeMap<u32, (BlockHash, Block)>, SqlBlocksStoreError> {
            let mut param_list = Vec::new();
            let mut stmt_str = "SELECT height, block_hash, block FROM blocks ".to_string();

            match range.start_bound() {
                Bound::Unbounded => stmt_str.push_str("WHERE height >= 0 "),
                Bound::Included(h) => {
                    stmt_str.push_str("WHERE height >= ? ");
                    param_list.push(*h);
                }
                Bound::Excluded(h) => {
                    stmt_str.push_str("WHERE height > ? ");
                    param_list.push(*h);
                }
            }

            match range.end_bound() {
                Bound::Unbounded => {}
                Bound::Included(h) => {
                    stmt_str.push_str("AND height <= ? ");
                    param_list.push(*h);
                }
                Bound::Excluded(h) => {
                    stmt_str.push_str("AND height < ? ");
                    param_list.push(*h);
                }
            }

            stmt_str.push_str("ORDER BY height");

            let mut blocks = BTreeMap::new();
            let write_lock = conn.blocking_lock();
            let mut stmt = write_lock.prepare(&stmt_str)?;
            let mut rows = stmt.query(params_from_iter(param_list.iter()))?;
            while let Some(row) = rows.next()? {
                let height: u32 = row.get(0)?;
                let hash_str: String = row.get(1)?;
                let blob: Vec<u8> = row.get(2)?;
                let block_hash = BlockHash::from_str(&hash_str)
                    .map_err(|_| SqlBlocksStoreError::StringConversion)?;
                // Since we're already on a blocking thread, deserialize inline.
                let block = deserialize(&blob)
                    .map_err(|e| SqlBlocksStoreError::DeserializeError(e.to_string()))?;
                blocks.insert(height, (block_hash, block));
            }
            Ok(blocks)
        })
            .await
            .expect("spawn_blocking failed")
    }

}

impl BlocksStore for SqliteBlockDb {
    type Error = SqlBlocksStoreError;

    fn insert_blocks(
        &mut self,
        blocks: BTreeMap<u32, Block>,
    ) -> FutureResult<'_, (), Self::Error> {
        Box::pin(self.insert_blocks(blocks))
    }


    fn get_block_by_hash<'a>(
        &'a mut self,
        block_hash: &'a BlockHash,
    ) -> FutureResult<'a, Option<Block>, Self::Error> {
        Box::pin(self.get_block_by_hash(block_hash))
    }

    fn get_block_by_height(
        &mut self,
        height: u32,
    ) -> FutureResult<'_, Option<Block>, Self::Error> {
        Box::pin(self.get_block_by_height(height))
    }

    fn get_block_hash(
        &mut self,
        height: u32,
    ) -> FutureResult<'_, Option<BlockHash>, Self::Error> {
        Box::pin(self.get_block_hash(height))
    }

    fn load_blocks<'a>(
        &'a mut self,
        range: impl RangeBounds<u32> + Send + Sync + 'a + 'static,
    ) -> FutureResult<'a, BTreeMap<u32, (BlockHash, Block)>, Self::Error> {
        Box::pin(self.load_blocks(range))
    }

    fn set_tip(
        &mut self,
        tip: HeaderCheckpoint,
    ) -> FutureResult<'_, (), Self::Error> {
        Box::pin(self.set_tip(tip))
    }

    fn get_tip(&mut self) -> FutureResult<'_, Option<HeaderCheckpoint>, Self::Error> {
        Box::pin(self.get_tip())
    }

    fn prune_up_to_height(
        &mut self,
        height: u32,
    ) -> FutureResult<'_, u32, Self::Error> {
        Box::pin(self.prune_up_to_height(height))
    }
}


fn serialize_tip(tip: &HeaderCheckpoint) -> String {
    // Using the Display implementation of BlockHash.
    format!("{}:{}", tip.height, tip.hash)
}

// Helper to parse a tip string back into a HeaderCheckpoint.
fn deserialize_tip(s: &str) -> Result<HeaderCheckpoint, SqlBlocksStoreError> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(SqlBlocksStoreError::StringConversion);
    }
    let height = parts[0]
        .parse::<u32>()
        .map_err(|_| SqlBlocksStoreError::StringConversion)?;
    let hash = BlockHash::from_str(parts[1])
        .map_err(|_| SqlBlocksStoreError::StringConversion)?;
    Ok(HeaderCheckpoint { height, hash })
}
