//! Housekeeping: collect old tombstones and retire finished jobs.

use crate::server::{pause, AppState};
use flotilla_core::keys;
use flotilla_core::schema::{JobClaim, JobResult, JobSpec};
use flotilla_core::Hlc;
use std::time::Duration;

const INTERVAL: Duration = Duration::from_secs(15 * 60);

pub async fn run(state: AppState) {
    // First pass shortly after start so a long-idle node catches up.
    pause(Duration::from_secs(30)).await;
    loop {
        if let Err(e) = pass(&state) {
            tracing::error!(error = %e, "gc pass failed");
        }
        pause(INTERVAL).await;
    }
}

pub fn pass(state: &AppState) -> anyhow::Result<()> {
    let now = flotilla_core::now_ms();
    let retired = retire_jobs(state, now)?;
    let horizon_ms = now.saturating_sub(state.cfg.gc_horizon_days * 24 * 3600 * 1000);
    let forget_ms = now.saturating_sub(state.cfg.gc_forget_days * 24 * 3600 * 1000);
    let removed = state.store.gc(
        Hlc::from_parts(horizon_ms, 0),
        Hlc::from_parts(forget_ms, 0),
    )?;
    if retired > 0 || removed > 0 {
        tracing::info!(
            retired_jobs = retired,
            tombstones_removed = removed,
            "gc pass"
        );
    }
    Ok(())
}

/// Tombstone spec/claim/result for jobs that finished (or were cancelled
/// without ever running) longer ago than the retention window, and drop the
/// local log file if we were the executor. Any node may do this; deletes are
/// idempotent under LWW.
fn retire_jobs(state: &AppState, now: u64) -> anyhow::Result<usize> {
    let cutoff = now.saturating_sub(state.cfg.job_retention_hours * 3600 * 1000);
    let mut retired = 0;
    for rec in state.store.list(keys::JOB)? {
        let Ok(spec) = rec.parse::<JobSpec>() else {
            continue;
        };
        let result = state
            .store
            .get(&keys::result(&spec.id))?
            .and_then(|r| r.parse::<JobResult>().ok());
        let claim = state
            .store
            .get(&keys::claim(&spec.id))?
            .and_then(|r| r.parse::<JobClaim>().ok());
        let done_at = match (&result, spec.cancelled, &claim) {
            (Some(r), _, _) => Some(r.finished_at_ms),
            // Cancelled and never claimed: dead since submission.
            (None, true, None) => Some(spec.submitted_at_ms),
            // Cancelled while claimed: the executor is killing it and will
            // write a result. Only retire once its lease has clearly lapsed.
            (None, true, Some(c)) if c.lease_expired_at(now) => Some(c.lease_until_ms),
            _ => None,
        };
        let Some(done_at) = done_at else { continue };
        if done_at >= cutoff {
            continue;
        }
        for key in [
            keys::job(&spec.id),
            keys::claim(&spec.id),
            keys::result(&spec.id),
        ] {
            state.store.delete(&key)?;
        }
        let _ = std::fs::remove_file(state.cfg.job_log_path(&spec.id));
        retired += 1;
    }
    Ok(retired)
}
