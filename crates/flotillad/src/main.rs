//! flotillad: the per-node fleet daemon.

mod auth;
mod config;
mod exec;
mod facts;
mod identity;
mod reconcile;
mod scheduler;
mod server;
mod sync_loop;
#[cfg(test)]
mod tests;

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug)]
#[command(name = "flotillad", version, about = "flotilla fleet daemon")]
struct Args {
    /// Config file (default: ~/.config/flotilla/config.toml)
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override the data directory
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Override the port
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let args = Args::parse();
    let mut cfg = config::Config::load(args.config.as_deref())?;
    if let Some(d) = args.data_dir {
        cfg.data_dir = d;
    }
    if let Some(p) = args.port {
        cfg.port = p;
    }
    std::fs::create_dir_all(&cfg.data_dir)
        .with_context(|| format!("creating {}", cfg.data_dir.display()))?;
    std::fs::create_dir_all(cfg.jobs_dir())?;

    let identity = Arc::new(identity::IdentityProvider::from_config(&cfg)?);
    let me = identity.me().await.context("resolving own identity")?;
    if cfg.allowed_users.is_empty() && cfg.allowed_tags.is_empty() {
        if let Some(login) = identity.own_login().await? {
            tracing::info!(login, "no allowed_users configured; allowing own login");
            cfg.allowed_users.push(login);
        }
    }
    let cfg = Arc::new(cfg);

    let store = Arc::new(flotilla_core::Store::open(
        &cfg.data_dir.join("store.redb"),
        me.node_id.clone(),
    )?);
    tracing::info!(node = %me.name, id = %me.node_id, records = store.len()?, "store opened");

    let state = server::AppState::new(cfg.clone(), identity.clone(), store.clone(), me.clone());

    tokio::spawn(facts::run(state.clone()));
    tokio::spawn(sync_loop::run(state.clone()));
    tokio::spawn(scheduler::run(state.clone()));
    tokio::spawn(reconcile::run(state.clone()));

    server::serve(state).await
}
