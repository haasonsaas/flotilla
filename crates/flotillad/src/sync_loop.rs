//! Anti-entropy: every interval, sync with one random online peer. Peers
//! that already have a facts record in our store (known fleet members) are
//! preferred so we don't spend most rounds on tailnet nodes that don't run
//! flotillad. A known peer is dialed on the port it advertises in its facts;
//! unknown peers and configured seeds are tried at their given/default port.

use crate::server::{pause, AppState};
use flotilla_core::api::{base_url, peer_url, PeerInfo};
use flotilla_core::keys;
use flotilla_core::schema::NodeFacts;
use flotilla_core::sync::{self, SyncMessage};
use rand::prelude::*;
use std::time::Duration;

pub async fn run(state: AppState) {
    let interval = Duration::from_secs(state.cfg.sync_interval_secs);
    loop {
        if let Err(e) = round(&state).await {
            tracing::debug!(error = %e, "sync round failed");
        }
        pause(interval).await;
    }
}

async fn round(state: &AppState) -> anyhow::Result<()> {
    let peers: Vec<PeerInfo> = state
        .identity
        .peers()
        .await?
        .into_iter()
        .filter(|p| p.online && p.node_id != state.me.node_id && !p.ips.is_empty())
        .collect();
    let mut known: Vec<(String, String)> = Vec::new();
    let mut unknown: Vec<(String, String)> = Vec::new();
    for p in &peers {
        let facts = state
            .store
            .get(&keys::node_facts(&p.node_id))
            .ok()
            .flatten()
            .and_then(|r| r.parse::<NodeFacts>().ok());
        match facts {
            Some(f) => {
                if let Some(url) = peer_url(&p.ips, f.port) {
                    known.push((p.name.clone(), url));
                }
            }
            None => {
                if let Some(url) = peer_url(&p.ips, state.cfg.port) {
                    unknown.push((p.name.clone(), url));
                }
            }
        }
    }
    for seed in &state.cfg.seeds {
        if let Some(url) = base_url(seed, state.cfg.port) {
            if !known.iter().any(|(_, u)| u == &url) {
                unknown.push((format!("seed {seed}"), url));
            }
        }
    }
    let pick = {
        let mut rng = rand::rng();
        if !known.is_empty() && (unknown.is_empty() || rng.random_bool(0.8)) {
            known.choose(&mut rng).cloned()
        } else {
            unknown.choose(&mut rng).cloned()
        }
    };
    let Some((name, base)) = pick else {
        return Ok(());
    };
    sync_with(state, &name, &base).await
}

pub async fn sync_with(state: &AppState, name: &str, base: &str) -> anyhow::Result<()> {
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
    Ok(())
}
