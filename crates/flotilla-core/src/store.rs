//! Last-writer-wins replicated record store backed by redb.
//!
//! Tables:
//! - `records`: key -> JSON record (live or tombstone)
//! - `vv`: author -> max HLC seen from that author (the version vector)
//! - `by_version`: (author, hlc) -> key, so `delta_since` walks only the
//!   records a peer has not seen instead of scanning everything
//! - `meta`: small counters, currently the GC horizon
//!
//! The version vector is maintained explicitly on every write and merge so
//! sync deltas stay monotonic even after a key is overwritten by a
//! different author.
//!
//! Garbage collection: tombstones older than a horizon are dropped, and the
//! horizon is remembered so that a record older than it arriving later (from
//! a node that was away for a long time) is rejected rather than resurrected.

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
const BY_VERSION: TableDefinition<(&str, u64), &str> = TableDefinition::new("by_version");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const META_HORIZON: &str = "gc_horizon";

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

/// Why a merge did or did not apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    /// Replaced (or created) the local record.
    Applied,
    /// The local record is the same or newer.
    Superseded,
    /// Older than the GC horizon; may have had its tombstone collected.
    BeforeHorizon,
    /// Timestamp too far in the future relative to this node's clock.
    ClockSkew,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeStats {
    pub applied: usize,
    pub superseded: usize,
    pub before_horizon: usize,
    pub clock_skew: usize,
}

impl MergeStats {
    pub fn rejected(&self) -> usize {
        self.before_horizon + self.clock_skew
    }
}

#[derive(Clone, Debug)]
pub struct StoreOptions {
    /// Reject merges whose wall clock is further ahead of ours than this.
    pub max_skew_ms: u64,
}

impl Default for StoreOptions {
    fn default() -> Self {
        StoreOptions {
            max_skew_ms: 60 * 60 * 1000,
        }
    }
}

pub struct Store {
    db: Database,
    me: NodeId,
    clock: Arc<Clock>,
    opts: StoreOptions,
}

impl Store {
    pub fn open(path: &Path, me: impl Into<NodeId>) -> Result<Store> {
        Store::open_with(path, me, StoreOptions::default())
    }

    pub fn open_with(path: &Path, me: impl Into<NodeId>, opts: StoreOptions) -> Result<Store> {
        let db = Database::create(path)?;
        Store::init(db, me.into(), Arc::new(Clock::new()), opts)
    }

    pub fn in_memory(me: impl Into<NodeId>) -> Result<Store> {
        Store::in_memory_with_clock(me, Arc::new(Clock::new()))
    }

    pub fn in_memory_with_clock(me: impl Into<NodeId>, clock: Arc<Clock>) -> Result<Store> {
        let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;
        Store::init(db, me.into(), clock, StoreOptions::default())
    }

    fn init(db: Database, me: NodeId, clock: Arc<Clock>, opts: StoreOptions) -> Result<Store> {
        let txn = db.begin_write()?;
        {
            txn.open_table(RECORDS)?;
            txn.open_table(BY_VERSION)?;
            txn.open_table(META)?;
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
        let store = Store {
            db,
            me,
            clock,
            opts,
        };
        store.rebuild_index_if_missing()?;
        Ok(store)
    }

    /// Stores created before the index existed have records but no
    /// `by_version` rows. Build them once.
    fn rebuild_index_if_missing(&self) -> Result<()> {
        let needs = {
            let txn = self.db.begin_read()?;
            let records = txn.open_table(RECORDS)?.len()?;
            let index = txn.open_table(BY_VERSION)?.len()?;
            records > 0 && index == 0
        };
        if !needs {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
            let records = txn.open_table(RECORDS)?;
            let mut index = txn.open_table(BY_VERSION)?;
            for row in records.iter()? {
                let (k, v) = row?;
                let rec: Record = serde_json::from_slice(v.value())?;
                index.insert((rec.author.as_str(), rec.hlc.0), k.value())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    pub fn me(&self) -> &NodeId {
        &self.me
    }

    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Write a value authored by this node.
    pub fn put(&self, key: &str, value: Value) -> Result<Record> {
        let rec = Record {
            key: key.to_string(),
            value,
            author: self.me.clone(),
            hlc: self.clock.now(),
            deleted: false,
        };
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
        Ok(self
            .list_raw(prefix)?
            .into_iter()
            .filter(|r| !r.deleted)
            .collect())
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
    /// version. See `merge_checked` for the reason when it did not.
    pub fn merge(&self, incoming: &Record) -> Result<bool> {
        Ok(self.merge_checked(incoming)? == MergeOutcome::Applied)
    }

    pub fn merge_checked(&self, incoming: &Record) -> Result<MergeOutcome> {
        let now_wall = crate::now_ms();
        if incoming.hlc.wall_ms() > now_wall.saturating_add(self.opts.max_skew_ms) {
            return Ok(MergeOutcome::ClockSkew);
        }
        if incoming.hlc < self.horizon()? {
            return Ok(MergeOutcome::BeforeHorizon);
        }
        self.clock.observe(incoming.hlc);
        let txn = self.db.begin_write()?;
        let outcome = {
            let mut t = txn.open_table(RECORDS)?;
            let mut index = txn.open_table(BY_VERSION)?;
            let current: Option<Record> = match t.get(incoming.key.as_str())? {
                Some(v) => Some(serde_json::from_slice(v.value())?),
                None => None,
            };
            let applied = match &current {
                Some(cur) => incoming.wins_over(cur),
                None => true,
            };
            if applied {
                if let Some(cur) = &current {
                    index.remove((cur.author.as_str(), cur.hlc.0))?;
                }
                t.insert(
                    incoming.key.as_str(),
                    serde_json::to_vec(incoming)?.as_slice(),
                )?;
                index.insert(
                    (incoming.author.as_str(), incoming.hlc.0),
                    incoming.key.as_str(),
                )?;
            }
            let mut vv = txn.open_table(VV)?;
            let seen = vv
                .get(incoming.author.as_str())?
                .map(|v| v.value())
                .unwrap_or(0);
            if incoming.hlc.0 > seen {
                vv.insert(incoming.author.as_str(), incoming.hlc.0)?;
            }
            if applied {
                MergeOutcome::Applied
            } else {
                MergeOutcome::Superseded
            }
        };
        txn.commit()?;
        Ok(outcome)
    }

    pub fn merge_all<'a>(
        &self,
        records: impl IntoIterator<Item = &'a Record>,
    ) -> Result<MergeStats> {
        let mut stats = MergeStats::default();
        for r in records {
            match self.merge_checked(r)? {
                MergeOutcome::Applied => stats.applied += 1,
                MergeOutcome::Superseded => stats.superseded += 1,
                MergeOutcome::BeforeHorizon => stats.before_horizon += 1,
                MergeOutcome::ClockSkew => stats.clock_skew += 1,
            }
        }
        Ok(stats)
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
    /// those whose author's HLC exceeds the vector entry. Walks the
    /// `(author, hlc)` index, so the cost is proportional to the delta.
    pub fn delta_since(&self, vv: &VersionVector) -> Result<Vec<Record>> {
        let txn = self.db.begin_read()?;
        let records = txn.open_table(RECORDS)?;
        let index = txn.open_table(BY_VERSION)?;
        let authors: Vec<String> = {
            let t = txn.open_table(VV)?;
            let mut a = Vec::new();
            for row in t.iter()? {
                let (k, _) = row?;
                a.push(k.value().to_string());
            }
            a
        };
        let mut out = Vec::new();
        for author in authors {
            let from = vv.get(&author).map(|h| h.0.saturating_add(1)).unwrap_or(0);
            let lo = (author.as_str(), from);
            let hi = (author.as_str(), u64::MAX);
            for row in index.range(lo..=hi)? {
                let (_, key) = row?;
                if let Some(v) = records.get(key.value())? {
                    out.push(serde_json::from_slice(v.value())?);
                }
            }
        }
        Ok(out)
    }

    /// Records older than this are refused on merge (see `gc`).
    pub fn horizon(&self) -> Result<Hlc> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(META)?;
        Ok(t.get(META_HORIZON)?
            .map(|v| Hlc(v.value()))
            .unwrap_or(Hlc::ZERO))
    }

    /// Drop tombstones older than `horizon` and remember the horizon so
    /// they cannot be resurrected by a late peer. Returns how many were
    /// removed. The horizon never moves backwards.
    pub fn gc(&self, horizon: Hlc) -> Result<usize> {
        let horizon = horizon.max(self.horizon()?);
        let txn = self.db.begin_write()?;
        let mut removed = 0;
        {
            let mut records = txn.open_table(RECORDS)?;
            let mut index = txn.open_table(BY_VERSION)?;
            let mut victims: Vec<(String, String, u64)> = Vec::new();
            for row in records.iter()? {
                let (k, v) = row?;
                let rec: Record = serde_json::from_slice(v.value())?;
                if rec.deleted && rec.hlc < horizon {
                    victims.push((k.value().to_string(), rec.author, rec.hlc.0));
                }
            }
            for (key, author, hlc) in victims {
                records.remove(key.as_str())?;
                index.remove((author.as_str(), hlc))?;
                removed += 1;
            }
            let mut meta = txn.open_table(META)?;
            meta.insert(META_HORIZON, horizon.0)?;
        }
        txn.commit()?;
        Ok(removed)
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
            let mut index = txn.open_table(BY_VERSION)?;
            if let Some(v) = t.get(rec.key.as_str())? {
                let cur: Record = serde_json::from_slice(v.value())?;
                index.remove((cur.author.as_str(), cur.hlc.0))?;
            }
            t.insert(rec.key.as_str(), serde_json::to_vec(rec)?.as_slice())?;
            index.insert((rec.author.as_str(), rec.hlc.0), rec.key.as_str())?;
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
        assert_eq!(
            jobs.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            ["job/1", "job/2"]
        );
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
        // the index holds exactly one row for the key
        assert_eq!(s.delta_since(&VersionVector::new()).unwrap().len(), 1);
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
        assert_eq!(
            b.merge_checked(&new).unwrap(),
            MergeOutcome::Superseded,
            "idempotent"
        );
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
    fn delta_covers_records_from_every_author() {
        let a = Store::in_memory("a").unwrap();
        let b = Store::in_memory("b").unwrap();
        let rb = b.put("from-b", json!(1)).unwrap();
        a.merge(&rb).unwrap();
        a.put("from-a", json!(2)).unwrap();
        let mut vv = VersionVector::new();
        assert_eq!(a.delta_since(&vv).unwrap().len(), 2);
        vv.insert("b".into(), rb.hlc);
        let d = a.delta_since(&vv).unwrap();
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].key, "from-a");
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
    fn gc_drops_old_tombstones_and_blocks_resurrection() {
        let a = Store::in_memory("a").unwrap();
        let b = Store::in_memory("b").unwrap();
        let live = a.put("keep", json!(1)).unwrap();
        a.put("gone", json!(1)).unwrap();
        let tomb = a.delete("gone").unwrap().unwrap();
        // b holds a stale live copy of "gone" from before the delete
        let stale = Record {
            hlc: Hlc(tomb.hlc.0 - 1),
            deleted: false,
            value: json!("zombie"),
            ..tomb.clone()
        };
        let horizon = Hlc(tomb.hlc.0 + 1);
        assert_eq!(a.gc(horizon).unwrap(), 1);
        assert!(a.get_raw("gone").unwrap().is_none(), "tombstone collected");
        assert_eq!(
            a.get("keep").unwrap().unwrap(),
            live,
            "live records untouched"
        );
        assert_eq!(a.horizon().unwrap(), horizon);
        assert_eq!(
            a.merge_checked(&stale).unwrap(),
            MergeOutcome::BeforeHorizon,
            "a late copy older than the horizon must not come back"
        );
        assert!(a.get("gone").unwrap().is_none());
        // b never GC'd, so it still accepts the stale record normally
        assert!(b.merge(&stale).unwrap());
        // horizon never moves backwards
        a.gc(Hlc::ZERO).unwrap();
        assert_eq!(a.horizon().unwrap(), horizon);
    }

    #[test]
    fn merge_rejects_far_future_timestamps() {
        let a = Store::in_memory("a").unwrap();
        let far = Record {
            key: "k".into(),
            value: json!(1),
            author: "b".into(),
            hlc: Hlc::from_parts(crate::now_ms() + 2 * 60 * 60 * 1000, 0),
            deleted: false,
        };
        assert_eq!(a.merge_checked(&far).unwrap(), MergeOutcome::ClockSkew);
        assert!(a.get("k").unwrap().is_none());
        assert!(
            a.clock().last() < far.hlc,
            "clock must not be dragged forward"
        );
        let near = Record {
            hlc: Hlc::from_parts(crate::now_ms() + 5_000, 0),
            ..far
        };
        assert_eq!(a.merge_checked(&near).unwrap(), MergeOutcome::Applied);
    }

    #[test]
    fn reopen_keeps_data_clock_and_horizon() {
        let dir = std::env::temp_dir().join(format!("flotilla-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.redb");
        let (last, horizon) = {
            let s = Store::open(&path, "a").unwrap();
            let r = s.put("k", json!(1)).unwrap();
            s.gc(Hlc(5)).unwrap();
            (r.hlc, s.horizon().unwrap())
        };
        let s = Store::open(&path, "a").unwrap();
        assert_eq!(s.get("k").unwrap().unwrap().value, json!(1));
        assert!(s.clock().last() >= last);
        assert_eq!(s.horizon().unwrap(), horizon);
        let next = s.put("k", json!(2)).unwrap();
        assert!(next.hlc > last);
        assert_eq!(s.delta_since(&VersionVector::new()).unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
