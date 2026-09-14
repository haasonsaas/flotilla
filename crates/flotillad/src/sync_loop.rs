//! Anti-entropy: every interval, sync with one online peer. Peers that
//! already have a facts record in our store (known fleet members) are
//! preferred so we don't spend most rounds on tailnet nodes that don't run
//! flotillad. A known peer is dialed on the port it advertises in its facts;
//! unknown peers and configured seeds are tried at their given/default port.
//! Failures back off exponentially per peer, and the per-peer state is
//! exposed on `/v1/syncstate`.

use crate::server::{pause, AppState};
use flotilla_core::api::{base_url, peer_url, PeerSyncState, SyncNowResult};
use flotilla_core::keys;
use flotilla_core::schema::NodeFacts;
use flotilla_core::sync::{self, SyncMessage};
use rand::prelude::*;
use std::time::Duration;

const MAX_BACKOFF: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
pub struct Candidate {
    pub key: String,
    pub name: String,
    pub url: String,
    pub known: bool,
}

pub async fn run(state: AppState) {
    let interval = Duration::from_secs(state.cfg.sync_interval_secs);
    loop {
        if let Err(e) = round(&state).await {
            tracing::debug!(error = %e, "sync round failed");
        }
        pause(interval).await;
    }
}

/// Every peer we could sync with right now, ignoring backoff.
pub async fn candidates(state: &AppState) -> anyhow::Result<Vec<Candidate>> {
    let peers = state.identity.peers().await?;
    let mut out = Vec::new();
    for p in peers
        .into_iter()
        .filter(|p| p.online && p.node_id != state.me.node_id && !p.ips.is_empty())
    {
        let facts = state
            .store
            .get(&keys::node_facts(&p.node_id))
            .ok()
            .flatten()
            .and_then(|r| r.parse::<NodeFacts>().ok());
        let (url, known) = match facts {
            Some(f) => (peer_url(&p.ips, f.port), true),
            None => (peer_url(&p.ips, state.cfg.port), false),
        };
        if let Some(url) = url {
            out.push(Candidate {
                key: p.node_id.clone(),
                name: p.name.clone(),
                url,
                known,
            });
        }
    }
    for seed in &state.cfg.seeds {
        if let Some(url) = base_url(seed, state.cfg.port) {
            if !out.iter().any(|c| c.url == url) {
                out.push(Candidate {
                    key: seed.clone(),
                    name: format!("seed {seed}"),
                    url,
                    known: false,
                });
            }
        }
    }
    Ok(out)
}

async fn round(state: &AppState) -> anyhow::Result<()> {
    let now = flotilla_core::now_ms();
    let all = candidates(state).await?;
    let ready: Vec<&Candidate> = {
        let st = state.sync_state.lock().unwrap();
        all.iter()
            .filter(|c| st.get(&c.key).map(|s| s.next_try_ms <= now).unwrap_or(true))
            .collect()
    };
    let known: Vec<&Candidate> = ready.iter().copied().filter(|c| c.known).collect();
    let unknown: Vec<&Candidate> = ready.iter().copied().filter(|c| !c.known).collect();
    let pick = {
        let mut rng = rand::rng();
        if !known.is_empty() && (unknown.is_empty() || rng.random_bool(0.8)) {
            known.choose(&mut rng).copied()
        } else {
            unknown.choose(&mut rng).copied()
        }
    };
    let Some(c) = pick else { return Ok(()) };
    sync_candidate(state, c).await;
    Ok(())
}

/// Sync with one candidate and record the outcome.
pub async fn sync_candidate(state: &AppState, c: &Candidate) -> SyncNowResult {
    let outcome = sync_with(state, &c.name, &c.url).await;
    let now = flotilla_core::now_ms();
    let mut st = state.sync_state.lock().unwrap();
    let entry = st.entry(c.key.clone()).or_insert_with(|| PeerSyncState {
        key: c.key.clone(),
        ..Default::default()
    });
    entry.name = c.name.clone();
    entry.url = c.url.clone();
    match &outcome {
        Ok((pulled, pushed)) => {
            entry.last_ok_ms = Some(now);
            entry.consecutive_failures = 0;
            entry.next_try_ms = 0;
            SyncNowResult {
                name: c.name.clone(),
                url: c.url.clone(),
                ok: true,
                pulled: *pulled,
                pushed: *pushed,
                error: None,
            }
        }
        Err(e) => {
            entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
            entry.last_error = Some(e.to_string());
            entry.last_error_ms = Some(now);
            let base = Duration::from_secs(state.cfg.sync_interval_secs.max(1));
            let backoff = base
                .saturating_mul(1u32 << entry.consecutive_failures.min(10))
                .min(MAX_BACKOFF);
            entry.next_try_ms = now + backoff.as_millis() as u64;
            tracing::debug!(peer = %c.name, failures = entry.consecutive_failures, backoff_s = backoff.as_secs(), error = %e, "sync failed");
            SyncNowResult {
                name: c.name.clone(),
                url: c.url.clone(),
                ok: false,
                pulled: 0,
                pushed: 0,
                error: Some(e.to_string()),
            }
        }
    }
}

/// Raw protocol round trip. Returns (pulled, pushed).
pub async fn sync_with(state: &AppState, name: &str, base: &str) -> anyhow::Result<(usize, usize)> {
    let url = format!("{base}/v1/sync");
    let first = sync::open(&state.store)?;
    let reply: SyncMessage = state
        .http
        .post(&url)
        .json(&first)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let (pulled, push) = sync::close(&state.store, &reply)?;
    let pushed = push.records.len();
    if pushed > 0 {
        state
            .http
            .post(&url)
            .json(&push)
            .send()
            .await?
            .error_for_status()?;
    }
    if pulled > 0 || pushed > 0 {
        tracing::info!(peer = name, pulled, pushed, "synced");
    } else {
        tracing::debug!(peer = name, "in sync");
    }
    Ok((pulled, pushed))
}
