//! Anti-entropy sync between two stores. Push-pull in two round trips:
//!
//! 1. Requester sends its version vector (no records).
//! 2. Responder replies with the records the requester lacks, plus the
//!    responder's own vector.
//! 3. Requester merges, then sends back the records the responder lacks.
//!
//! Both halves are pure functions over a `Store`, so the daemon just wires
//! them to HTTP and the tests call them directly.

use crate::record::Record;
use crate::store::{Result, Store, VersionVector};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SyncMessage {
    pub vv: VersionVector,
    #[serde(default)]
    pub records: Vec<Record>,
}

/// Responder side: merge whatever was pushed, then answer with what the
/// requester is missing.
pub fn respond(store: &Store, incoming: &SyncMessage) -> Result<SyncMessage> {
    store.merge_all(&incoming.records)?;
    Ok(SyncMessage {
        vv: store.version_vector()?,
        records: store.delta_since(&incoming.vv)?,
    })
}

/// Requester side, step 1: what to send first.
pub fn open(store: &Store) -> Result<SyncMessage> {
    Ok(SyncMessage {
        vv: store.version_vector()?,
        records: Vec::new(),
    })
}

/// Requester side, step 2: merge the reply and build the push (may be empty).
/// Returns (records applied locally, push message).
pub fn close(store: &Store, reply: &SyncMessage) -> Result<(usize, SyncMessage)> {
    let applied = store.merge_all(&reply.records)?;
    let push = store.delta_since(&reply.vv)?;
    Ok((
        applied,
        SyncMessage {
            vv: store.version_vector()?,
            records: push,
        },
    ))
}

/// Run a full in-process sync between two stores. Used by tests.
pub fn sync_pair(requester: &Store, responder: &Store) -> Result<(usize, usize)> {
    let first = open(requester)?;
    let reply = respond(responder, &first)?;
    let (pulled, push) = close(requester, &reply)?;
    let pushed = push.records.len();
    if pushed > 0 {
        respond(responder, &push)?;
    }
    Ok((pulled, pushed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Clock;
    use rand::prelude::*;
    use serde_json::json;
    use std::sync::Arc;

    fn contents(s: &Store) -> Vec<Record> {
        s.list_raw("").unwrap()
    }

    #[test]
    fn two_stores_converge_both_directions() {
        let a = Store::in_memory("a").unwrap();
        let b = Store::in_memory("b").unwrap();
        a.put("only-a", json!(1)).unwrap();
        b.put("only-b", json!(2)).unwrap();
        let (pulled, pushed) = sync_pair(&a, &b).unwrap();
        assert_eq!((pulled, pushed), (1, 1));
        assert_eq!(contents(&a), contents(&b));
        let (pulled, pushed) = sync_pair(&a, &b).unwrap();
        assert_eq!((pulled, pushed), (0, 0), "second sync is a no-op");
    }

    #[test]
    fn concurrent_writes_resolve_to_one_winner_everywhere() {
        let a = Store::in_memory_with_clock("a", Arc::new(Clock::with_wall(|| 500))).unwrap();
        let b = Store::in_memory_with_clock("b", Arc::new(Clock::with_wall(|| 500))).unwrap();
        let c = Store::in_memory("c").unwrap();
        a.put("k", json!("from-a")).unwrap();
        b.put("k", json!("from-b")).unwrap();
        // Same HLC on both, so the author tiebreak decides: "b" > "a".
        sync_pair(&c, &a).unwrap();
        sync_pair(&c, &b).unwrap();
        sync_pair(&a, &c).unwrap();
        for s in [&a, &b, &c] {
            assert_eq!(s.get("k").unwrap().unwrap().value, json!("from-b"));
        }
    }

    #[test]
    fn tombstones_replicate_and_beat_older_writes() {
        let a = Store::in_memory("a").unwrap();
        let b = Store::in_memory("b").unwrap();
        a.put("k", json!(1)).unwrap();
        sync_pair(&a, &b).unwrap();
        b.delete("k").unwrap();
        sync_pair(&a, &b).unwrap();
        assert!(a.get("k").unwrap().is_none());
        assert!(a.get_raw("k").unwrap().unwrap().deleted);
    }

    #[test]
    fn overwritten_author_still_syncs_monotonically() {
        // a writes k, b overwrites k. A third node that has seen neither
        // must end with b's value, and a later sync with a must not
        // resurrect a's version.
        let a = Store::in_memory("a").unwrap();
        let b = Store::in_memory("b").unwrap();
        let c = Store::in_memory("c").unwrap();
        a.put("k", json!("a")).unwrap();
        sync_pair(&b, &a).unwrap();
        b.put("k", json!("b")).unwrap();
        sync_pair(&c, &b).unwrap();
        sync_pair(&c, &a).unwrap();
        assert_eq!(c.get("k").unwrap().unwrap().value, json!("b"));
        assert_eq!(a.get("k").unwrap().unwrap().value, json!("b"));
    }

    #[test]
    fn random_ops_random_sync_order_converge() {
        let mut rng = StdRng::seed_from_u64(42);
        let ids = ["n1", "n2", "n3", "n4"];
        let stores: Vec<Store> = ids
            .iter()
            .map(|id| Store::in_memory(*id).unwrap())
            .collect();
        for round in 0..30 {
            for (i, s) in stores.iter().enumerate() {
                for _ in 0..rng.random_range(0..4) {
                    let key = format!("k{}", rng.random_range(0..12));
                    if rng.random_bool(0.15) {
                        s.delete(&key).unwrap();
                    } else {
                        s.put(&key, json!({"by": i, "round": round})).unwrap();
                    }
                }
            }
            // a few random pairwise syncs, not a full mesh
            for _ in 0..3 {
                let x = rng.random_range(0..stores.len());
                let mut y = rng.random_range(0..stores.len());
                while y == x {
                    y = rng.random_range(0..stores.len());
                }
                sync_pair(&stores[x], &stores[y]).unwrap();
            }
        }
        // full mesh until quiescent, then everyone must agree
        for _ in 0..3 {
            for x in 0..stores.len() {
                for y in 0..stores.len() {
                    if x != y {
                        sync_pair(&stores[x], &stores[y]).unwrap();
                    }
                }
            }
        }
        let reference = contents(&stores[0]);
        assert!(!reference.is_empty());
        for s in &stores[1..] {
            assert_eq!(contents(s), reference);
        }
    }
}
