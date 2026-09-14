//! Last-writer-wins replicated record store backed by redb.
//!
//! Two tables: `records` (key -> JSON record) and `vv` (author -> max HLC
//! seen). The version vector is maintained explicitly on every write and
//! merge so sync deltas stay monotonic even after a key is overwritten by
//! a different author.

use crate::hlc::{Clock, Hlc};
use crate::record::{NodeId, Record};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

const RECORDS: TableDefinition<&str, &[u8]> = TableDefinition::new("records");
const VV: TableDefinition<&str, u64> = TableDefinition::new("vv");

pub type VersionVector = BTreeMap<NodeId, Hlc>;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] redb::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<redb::DatabaseError> for StoreError {
    fn from(e: redb::DatabaseError) -> Self {
        StoreError::Db(e.into())
    }
}
impl From<redb::TransactionError> for StoreError {
    fn from(e: redb::TransactionError) -> Self {
        StoreError::Db(e.into())
    }
}
impl From<redb::TableError> for StoreError {
    fn from(e: redb::TableError) -> Self {
        StoreError::Db(e.into())
    }
}
impl From<redb::StorageError> for StoreError {
    fn from(e: redb::StorageError) -> Self {
        StoreError::Db(e.into())
    }
}
impl From<redb::CommitError> for StoreError {
    fn from(e: redb::CommitError) -> Self {
        StoreError::Db(e.into())
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub struct Store {
    db: Database,
    me: NodeId,
    clock: Arc<Clock>,
}

impl Store {
    pub fn open(path: &Path, me: impl Into<NodeId>) -> Result<Store> {
        let db = Database::create(path)?;
        Store::init(db, me.into(), Arc::new(Clock::new()))
    }

    pub fn in_memory(me: impl Into<NodeId>) -> Result<Store> {
        Store::in_memory_with_clock(me, Arc::new(Clock::new()))
    }

    pub fn in_memory_with_clock(me: impl Into<NodeId>, clock: Arc<Clock>) -> Result<Store> {
        let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;
        Store::init(db, me.into(), clock)
    }

    fn init(db: Database, me: NodeId, clock: Arc<Clock>) -> Result<Store> {
        let txn = db.begin_write()?;
        {
            txn.open_table(RECORDS)?;
            let vv = txn.open_table(VV)?;
            // Warm the clock past anything we have already issued so a
            // restart never reuses a timestamp.
            let mut max = Hlc::ZERO;
            for row in vv.iter()? {
                let (_, v) = row?;
                max = max.max(Hlc(v.value()));
            }
            clock.observe(max);
        }
        txn.commit()?;
        Ok(Store { db, me, clock })
    }

    pub fn me(&self) -> &NodeId {
        &self.me
    }

    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Write a value authored by this node.
    pub fn put(&self, key: &str, value: Value) -> Result<Record> {
        let rec = Record { key: key.to_string(), value, author: self.me.clone(), hlc: self.clock.now(), deleted: false };
        self.write(&rec)?;
        Ok(rec)
    }

    pub fn put_json<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<Record> {
        self.put(key, serde_json::to_value(value)?)
    }

    /// Tombstone a key. Returns None if the key is unknown or already deleted.
    pub fn delete(&self, key: &str) -> Result<Option<Record>> {
        match self.get(key)? {
            Some(_) => {
                let rec = Record {
                    key: key.to_string(),
                    value: Value::Null,
                    author: self.me.clone(),
                    hlc: self.clock.now(),
                    deleted: true,
                };
                self.write(&rec)?;
                Ok(Some(rec))
            }
            None => Ok(None),
        }
    }

    /// Live record for `key` (tombstones read as None).
    pub fn get(&self, key: &str) -> Result<Option<Record>> {
        Ok(self.get_raw(key)?.filter(|r| !r.deleted))
    }

    /// Record for `key` including tombstones.
    pub fn get_raw(&self, key: &str) -> Result<Option<Record>> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(RECORDS)?;
        match t.get(key)? {
            Some(v) => Ok(Some(serde_json::from_slice(v.value())?)),
            None => Ok(None),
        }
    }

    /// Live records whose key starts with `prefix`, in key order.
    pub fn list(&self, prefix: &str) -> Result<Vec<Record>> {
        Ok(self.list_raw(prefix)?.into_iter().filter(|r| !r.deleted).collect())
    }

    pub fn list_raw(&self, prefix: &str) -> Result<Vec<Record>> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(RECORDS)?;
        let mut out = Vec::new();
        for row in t.range(prefix..)? {
            let (k, v) = row?;
            if !k.value().starts_with(prefix) {
                break;
            }
            out.push(serde_json::from_slice(v.value())?);
        }
        Ok(out)
    }

    /// Merge a record from a peer. Returns true if it replaced the local
    /// version. Always advances the clock and version vector.
    pub fn merge(&self, incoming: &Record) -> Result<bool> {
        self.clock.observe(incoming.hlc);
        let txn = self.db.begin_write()?;
        let applied = {
            let mut t = txn.open_table(RECORDS)?;
            let current: Option<Record> = match t.get(incoming.key.as_str())? {
                Some(v) => Some(serde_json::from_slice(v.value())?),
                None => None,
            };
            let applied = match &current {
                Some(cur) => incoming.wins_over(cur),
                None => true,
            };
            if applied {
                t.insert(incoming.key.as_str(), serde_json::to_vec(incoming)?.as_slice())?;
            }
            let mut vv = txn.open_table(VV)?;
            let seen = vv.get(incoming.author.as_str())?.map(|v| v.value()).unwrap_or(0);
            if incoming.hlc.0 > seen {
                vv.insert(incoming.author.as_str(), incoming.hlc.0)?;
            }
            applied
        };
        txn.commit()?;
        Ok(applied)
    }

    pub fn merge_all<'a>(&self, records: impl IntoIterator<Item = &'a Record>) -> Result<usize> {
        let mut n = 0;
        for r in records {
            if self.merge(r)? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Max HLC seen per author.
    pub fn version_vector(&self) -> Result<VersionVector> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(VV)?;
        let mut out = BTreeMap::new();
        for row in t.iter()? {
            let (k, v) = row?;
            out.insert(k.value().to_string(), Hlc(v.value()));
        }
        Ok(out)
    }

    /// Every record (including tombstones) the holder of `vv` has not seen:
    /// those whose author's HLC exceeds the vector entry.
    pub fn delta_since(&self, vv: &VersionVector) -> Result<Vec<Record>> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(RECORDS)?;
        let mut out = Vec::new();
        for row in t.iter()? {
            let (_, v) = row?;
            let rec: Record = serde_json::from_slice(v.value())?;
            let seen = vv.get(&rec.author).copied().unwrap_or(Hlc::ZERO);
            if rec.hlc > seen {
                out.push(rec);
            }
        }
        Ok(out)
    }

    pub fn len(&self) -> Result<u64> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(RECORDS)?.len()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    fn write(&self, rec: &Record) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(RECORDS)?;
            t.insert(rec.key.as_str(), serde_json::to_vec(rec)?.as_slice())?;
            let mut vv = txn.open_table(VV)?;
            let seen = vv.get(rec.author.as_str())?.map(|v| v.value()).unwrap_or(0);
            if rec.hlc.0 > seen {
                vv.insert(rec.author.as_str(), rec.hlc.0)?;
            }
        }
        txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn put_get_list_delete() {
        let s = Store::in_memory("a").unwrap();
        s.put("job/1", json!({"n": 1})).unwrap();
        s.put("job/2", json!({"n": 2})).unwrap();
        s.put("node/x/facts", json!({})).unwrap();
        assert_eq!(s.get("job/1").unwrap().unwrap().value, json!({"n": 1}));
        let jobs = s.list("job/").unwrap();
        assert_eq!(jobs.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(), ["job/1", "job/2"]);
        assert!(s.delete("job/1").unwrap().is_some());
        assert!(s.get("job/1").unwrap().is_none());
        assert!(s.get_raw("job/1").unwrap().unwrap().deleted);
        assert_eq!(s.list("job/").unwrap().len(), 1);
        assert!(s.delete("job/1").unwrap().is_none());
        assert!(s.delete("nope").unwrap().is_none());
    }

    #[test]
    fn overwrite_bumps_hlc_and_vv() {
        let s = Store::in_memory("a").unwrap();
        let r1 = s.put("k", json!(1)).unwrap();
        let r2 = s.put("k", json!(2)).unwrap();
        assert!(r2.hlc > r1.hlc);
        assert_eq!(s.version_vector().unwrap()["a"], r2.hlc);
        assert_eq!(s.len().unwrap(), 1);
    }

    #[test]
    fn merge_applies_newer_and_rejects_older() {
        let a = Store::in_memory("a").unwrap();
        let b = Store::in_memory("b").unwrap();
        let old = a.put("k", json!("old")).unwrap();
        let new = a.put("k", json!("new")).unwrap();
        assert!(b.merge(&new).unwrap());
        assert!(!b.merge(&old).unwrap());
        assert_eq!(b.get("k").unwrap().unwrap().value, json!("new"));
        // idempotent
        assert!(!b.merge(&new).unwrap());
        assert_eq!(b.version_vector().unwrap()["a"], new.hlc);
    }

    #[test]
    fn merge_observes_remote_clock() {
        let a = Store::in_memory_with_clock("a", Arc::new(Clock::with_wall(|| 1_000_000))).unwrap();
        let b = Store::in_memory_with_clock("b", Arc::new(Clock::with_wall(|| 10))).unwrap();
        let ra = a.put("k", json!("a")).unwrap();
        b.merge(&ra).unwrap();
        let rb = b.put("k", json!("b")).unwrap();
        assert!(rb.hlc > ra.hlc, "b's later write must sort after a's");
        assert!(rb.wins_over(&ra));
    }

    #[test]
    fn delta_since_returns_only_unseen() {
        let a = Store::in_memory("a").unwrap();
        let r1 = a.put("k1", json!(1)).unwrap();
        let r2 = a.put("k2", json!(2)).unwrap();
        let mut vv = VersionVector::new();
        assert_eq!(a.delta_since(&vv).unwrap().len(), 2);
        vv.insert("a".into(), r1.hlc);
        let d = a.delta_since(&vv).unwrap();
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].key, "k2");
        vv.insert("a".into(), r2.hlc);
        assert!(a.delta_since(&vv).unwrap().is_empty());
    }

    #[test]
    fn delta_includes_tombstones() {
        let a = Store::in_memory("a").unwrap();
        a.put("k", json!(1)).unwrap();
        a.delete("k").unwrap();
        let d = a.delta_since(&VersionVector::new()).unwrap();
        assert_eq!(d.len(), 1);
        assert!(d[0].deleted);
    }

    #[test]
    fn reopen_keeps_data_and_clock() {
        let dir = std::env::temp_dir().join(format!("flotilla-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.redb");
        let last = {
            let s = Store::open(&path, "a").unwrap();
            s.put("k", json!(1)).unwrap().hlc
        };
        let s = Store::open(&path, "a").unwrap();
        assert_eq!(s.get("k").unwrap().unwrap().value, json!(1));
        assert!(s.clock().last() >= last);
        let next = s.put("k", json!(2)).unwrap();
        assert!(next.hlc > last);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
