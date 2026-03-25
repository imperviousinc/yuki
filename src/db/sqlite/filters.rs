use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use bitcoin::{BlockHash, Network};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use tokio::sync::Mutex;

use crate::prelude::FutureResult;
use crate::db::error::SqlInitializationError;
use crate::filters::Filter;
use crate::HeaderCheckpoint;


// ---------- Storage Error ----------
#[derive(Debug)]
pub enum SqlFiltersStoreError {
    SQL(rusqlite::Error),
    StringConversion,
    DeserializeError(String),
}

impl From<rusqlite::Error> for SqlFiltersStoreError {
    fn from(err: rusqlite::Error) -> Self {
        SqlFiltersStoreError::SQL(err)
    }
}

// ---------- FiltersStore Trait ----------
pub trait FiltersStore: Debug + Send + Sync {
    type Error;

    fn reload(
        &self,
    ) -> FutureResult<'_, (), SqlInitializationError>;

    fn file_path(&self) -> Option<PathBuf>;

    /// Insert multiple filters in one transaction.
    fn insert_filters(
        &mut self,
        filters: BTreeMap<u32, Filter>,
    ) -> FutureResult<'_, (), Self::Error>;

    /// Get a filter by its block hash.
    fn get_filter_by_hash<'a>(
        &'a mut self,
        block_hash: &'a BlockHash,
    ) -> FutureResult<'a, Option<Filter>, Self::Error>;

    /// Get a filter stored at a given height.
    fn get_filter_by_height<'a>(
        &'a mut self,
        height: u32,
    ) -> FutureResult<'a, Option<Filter>, Self::Error>;

    /// Load filters within a height range.
    fn load_filters<'a>(
        &'a mut self,
        range: impl RangeBounds<u32> + Send + Sync + 'a + 'static,
    ) -> FutureResult<'a, BTreeMap<u32, Filter>, Self::Error>;

    fn get_block_hash(
        &mut self,
        height: u32,
    ) -> FutureResult<'_, Option<BlockHash>, Self::Error>;

    /// Persist the current filters tip checkpoint.
    fn set_tip(
        &mut self,
        tip: HeaderCheckpoint,
    ) -> FutureResult<'_, (), Self::Error>;

    /// Load the current filters tip checkpoint.
    fn get_tip(&mut self) -> FutureResult<'_, Option<HeaderCheckpoint>, Self::Error>;
}

// ---------- SQLite Schema ----------
const FILE_NAME: &str = "filters.db";
const FILTERS_TABLE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS filters (
    block_hash TEXT PRIMARY KEY,
    height INTEGER NOT NULL,
    filter BLOB NOT NULL
) STRICT";

const META_TABLE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT";

/// SQLite-backed filter store.
#[derive(Debug)]
pub struct SqliteFilterDb {
    conn: Arc<Mutex<Connection>>,
    file_path: PathBuf
}

impl SqliteFilterDb {
    /// Initialize or open the filters database at the given path.
    pub fn new(network: Network, path: Option<PathBuf>) -> Result<Self, SqlInitializationError> {
        let mut path = path.unwrap_or_else(|| PathBuf::from("."));
        path.push("data");
        path.push(network.to_string());
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        let file_path = path.join(FILE_NAME);
        let conn = Self::open(&file_path)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            file_path
        })
    }

    fn open(file_path: &PathBuf) -> Result<Connection, SqlInitializationError> {
        let conn = Connection::open(&file_path)?;
        conn.execute(FILTERS_TABLE_SCHEMA, [])?;
        conn.execute(META_TABLE_SCHEMA, [])?;
        Ok(conn)
    }

    async fn set_tip(&mut self, tip: HeaderCheckpoint) -> Result<(), SqlFiltersStoreError> {
        let conn = self.conn.clone();
        let tip_str = format!("{}:{}", tip.height, tip.hash);
        tokio::task::spawn_blocking(move || -> Result<(), SqlFiltersStoreError> {
            let lock = conn.blocking_lock();
            lock.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('filters_tip', ?1)",
                [&tip_str],
            )?;
            Ok(())
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn get_tip(&mut self) -> Result<Option<HeaderCheckpoint>, SqlFiltersStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<HeaderCheckpoint>, SqlFiltersStoreError> {
            let lock = conn.blocking_lock();
            let tip_str: Option<String> = lock
                .query_row(
                    "SELECT value FROM meta WHERE key = 'filters_tip'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(s) = tip_str {
                let parts: Vec<&str> = s.split(':').collect();
                if parts.len() != 2 {
                    return Err(SqlFiltersStoreError::StringConversion);
                }
                let height = parts[0]
                    .parse::<u32>()
                    .map_err(|_| SqlFiltersStoreError::StringConversion)?;
                let hash = BlockHash::from_str(parts[1])
                    .map_err(|_| SqlFiltersStoreError::StringConversion)?;
                Ok(Some(HeaderCheckpoint { height, hash }))
            } else {
                return Ok(None)
            }
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn get_block_hash(&mut self, height: u32) -> Result<Option<BlockHash>, SqlFiltersStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<BlockHash>, SqlFiltersStoreError> {
            let write_lock = conn.blocking_lock();
            let stmt = "SELECT block_hash FROM filters WHERE height = ?1";
            let hash_str: Option<String> = write_lock
                .query_row(stmt, params![height], |row| row.get(0))
                .optional()?;

            match hash_str {
                Some(hash) => {
                    let block_hash = BlockHash::from_str(&hash)
                        .map_err(|_| SqlFiltersStoreError::StringConversion)?;
                    Ok(Some(block_hash))
                }
                None => Ok(None),
            }
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn reload(&self) -> Result<(), SqlInitializationError> {
        match Self::open(&self.file_path) {
            Ok(new_conn) => {
                let mut guard = self.conn.lock().await;
                *guard = new_conn;
                Ok(())
            }
            Err(open_err) => {
                let _ = tokio::fs::remove_file(&self.file_path).await;
                let fallback_conn = Self::open(&self.file_path)?;
                let mut guard = self.conn.lock().await;
                *guard = fallback_conn;
                Err(open_err)
            }
        }
    }

    async fn insert_filters(
        &mut self,
        filters: BTreeMap<u32, Filter>,
    ) -> Result<(), SqlFiltersStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<(), SqlFiltersStoreError> {
            let mut write_lock = conn.blocking_lock();
            let tx = write_lock.transaction()?;
            let stmt = "INSERT OR REPLACE INTO filters (block_hash, height, filter) VALUES (?1, ?2, ?3)";
            for (height, cf) in filters {
                tx.execute(
                    stmt,
                    params![cf.block_hash().to_string(), height, cf.to_bytes()],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn get_filter_by_hash(
        &mut self,
        block_hash: &BlockHash,
    ) -> Result<Option<Filter>, SqlFiltersStoreError> {
        let lock = self.conn.lock().await;
        let stmt = "SELECT height, filter FROM filters WHERE block_hash = ?1";
        let row: Option<(u32, Vec<u8>)> = lock
            .query_row(stmt, params![block_hash.to_string()], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        if let Some((_height, blob)) = row {
            Ok(Some(Filter::new(blob, *block_hash)))
        } else {
            Ok(None)
        }
    }

    async fn get_filter_by_height(
        &mut self,
        height: u32,
    ) -> Result<Option<Filter>, SqlFiltersStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<Filter>, SqlFiltersStoreError> {
            let lock = conn.blocking_lock();
            let mut stmt = lock.prepare("SELECT block_hash, filter FROM filters WHERE height = ?1")?;
            let mut rows = stmt.query(params![height])?;
            if let Some(row) = rows.next()? {
                let hash_str: String = row.get(0)?;
                let blob: Vec<u8> = row.get(1)?;
                let bh = BlockHash::from_str(&hash_str)
                    .map_err(|_| SqlFiltersStoreError::StringConversion)?;
                Ok(Some(Filter::new(blob, bh)))
            } else {
                Ok(None)
            }
        })
            .await
            .expect("spawn_blocking failed")
    }

    async fn load_filters(
        &mut self,
        range: impl RangeBounds<u32> + Send + Sync + 'static,
    ) -> Result<BTreeMap<u32, Filter>, SqlFiltersStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<BTreeMap<u32, Filter>, SqlFiltersStoreError> {
            let mut params_list = Vec::new();
            let mut stmt_str = String::from("SELECT height, block_hash, filter FROM filters ");
            match range.start_bound() {
                Bound::Unbounded => stmt_str.push_str("WHERE height >= 0 "),
                Bound::Included(h) => {
                    stmt_str.push_str("WHERE height >= ? ");
                    params_list.push(*h);
                }
                Bound::Excluded(h) => {
                    stmt_str.push_str("WHERE height > ? ");
                    params_list.push(*h);
                }
            }
            match range.end_bound() {
                Bound::Unbounded => {}
                Bound::Included(h) => {
                    stmt_str.push_str("AND height <= ? ");
                    params_list.push(*h);
                }
                Bound::Excluded(h) => {
                    stmt_str.push_str("AND height < ? ");
                    params_list.push(*h);
                }
            }
            stmt_str.push_str("ORDER BY height");

            let mut map = BTreeMap::new();
            let lock = conn.blocking_lock();
            let mut stmt = lock.prepare(&stmt_str)?;
            let mut rows = stmt.query(params_from_iter(params_list.iter()))?;
            while let Some(row) = rows.next()? {
                let height: u32 = row.get(0)?;
                let hash_str: String = row.get(1)?;
                let blob: Vec<u8> = row.get(2)?;
                let bh = BlockHash::from_str(&hash_str)
                    .map_err(|_| SqlFiltersStoreError::StringConversion)?;
                map.insert(
                    height,
                    Filter::new(blob, bh),
                );
            }
            Ok(map)
        })
            .await
            .expect("spawn_blocking failed")
    }
}

// ---------- Trait Implementation ----------
impl FiltersStore for SqliteFilterDb {
    type Error = SqlFiltersStoreError;

    fn file_path(&self) -> Option<PathBuf> {
        Some(self.file_path.clone())
    }

    fn reload(&self) -> FutureResult<'_, (), SqlInitializationError> {
        Box::pin(self.reload())
    }

    fn get_block_hash(
        &mut self,
        height: u32,
    ) -> FutureResult<'_, Option<BlockHash>, Self::Error> {
        Box::pin(self.get_block_hash(height))
    }

    fn insert_filters(
        &mut self,
        filters: BTreeMap<u32, Filter>,
    ) -> FutureResult<'_, (), Self::Error> {
        Box::pin(self.insert_filters(filters))
    }

    fn get_filter_by_hash<'a>(
        &'a mut self,
        block_hash: &'a BlockHash,
    ) -> FutureResult<'a, Option<Filter>, Self::Error> {
        Box::pin(self.get_filter_by_hash(block_hash))
    }

    fn get_filter_by_height(
        &mut self,
        height: u32,
    ) -> FutureResult<'_, Option<Filter>, Self::Error> {
        Box::pin(self.get_filter_by_height(height))
    }

    fn load_filters<'a>(
        &'a mut self,
        range: impl RangeBounds<u32> + Send + Sync + 'a + 'static,
    ) -> FutureResult<'a, BTreeMap<u32, Filter>, Self::Error> {
        Box::pin(self.load_filters(range))
    }

    fn set_tip(&mut self, tip: HeaderCheckpoint) -> FutureResult<'_, (), Self::Error> {
        Box::pin(self.set_tip(tip))
    }

    fn get_tip(&mut self) -> FutureResult<'_, Option<HeaderCheckpoint>, Self::Error> {
        Box::pin(self.get_tip())
    }
}
