//! Core primitives for flotilla: a hybrid logical clock, a last-writer-wins
//! replicated record store, the sync protocol, and the record schemas that
//! the status, job, and desired-state layers are built from.

pub mod api;
pub mod hlc;
pub mod keys;
pub mod record;
pub mod schema;
pub mod selector;
pub mod store;
pub mod sync;

pub use hlc::{Clock, Hlc};
pub use record::{NodeId, Record};
pub use store::{Store, VersionVector};

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
