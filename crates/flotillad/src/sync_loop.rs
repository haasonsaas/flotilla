//! Anti-entropy: every interval, sync with one random online peer. Peers
//! that already have a facts record in our store (known fleet members) are
//! preferred so we don't spend most rounds on tailnet nodes that don't run
//! flotillad.

use crate::server::{pause, AppState};
use flotilla_core::api::{peer_url, PeerInfo};
use flotilla_core::keys;
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
    if peers.is_empty() {
        return Ok(());
    }
    let known: Vec<&PeerInfo> = peers
        .iter()
        .filter(|p| {
            state
                .store
                .get(&keys::node_facts(&p.node_id))
                .ok()
                .flatten()
                .is_some()
        })
        .collect();
    let pick = {
        let mut rng = rand::rng();
        if !known.is_empty() && rng.random_bool(0.8) {
            (*known.choose(&mut rng).unwrap()).clone()
        } else {
            peers.choose(&mut rng).unwrap().clone()
        }
    };
    let Some(base) = peer_url(&pick.ips, state.cfg.port) else {
        return Ok(());
    };
    sync_with(state, &pick.name, &base).await
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
