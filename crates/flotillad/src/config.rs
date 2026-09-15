use anyhow::{Context, Result};
use flotilla_core::api::DEFAULT_PORT;
use flotilla_core::selector::Labels;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    /// Tailscale login names allowed to call this daemon (e.g. `jhaas@corp.example.com`).
    pub allowed_users: Vec<String>,
    /// Tailscale node tags allowed to call this daemon (e.g. `tag:fleet`).
    pub allowed_tags: Vec<String>,
    /// Tailscale application capability consulted for roles, e.g. a policy
    /// grant `"app": {"haasonsaas.dev/cap/flotilla": [{"roles": ["read"]}]}`.
    /// Callers matched by allowed_users/allowed_tags get every role.
    pub grant_cap: String,
    /// Extra labels advertised in this node's facts.
    pub labels: Labels,
    /// Path to the tailscale CLI. Auto-detected if unset.
    pub tailscale_bin: Option<PathBuf>,
    pub max_concurrent_jobs: usize,
    /// Tombstones older than this are collected, and records older than
    /// the collection horizon are refused on merge.
    pub gc_horizon_days: u64,
    /// Finished jobs (spec, claim, result, local log) are removed after this.
    pub job_retention_hours: u64,
    /// Records stamped further in the future than this are refused.
    pub max_clock_skew_secs: u64,
    /// How long a claim stays valid without renewal. Executors renew at a
    /// third of this; a lapsed lease lets another node take the job over.
    pub job_lease_secs: u64,
    /// macOS: wrap jobs in `caffeinate -i` so the machine stays awake while
    /// one runs. Ignored where caffeinate is absent.
    pub caffeinate_jobs: bool,
    pub sync_interval_secs: u64,
    pub facts_interval_secs: u64,
    pub scheduler_interval_secs: u64,
    pub reconcile_interval_secs: u64,
    /// Extra listen addresses (host:port). Loopback and Tailscale IPs are always bound.
    pub listen: Vec<String>,
    /// Peers to sync with that may not be discoverable yet or that listen on a
    /// non-default port, as `host:port`. Once a peer's facts are known its
    /// advertised port is used instead.
    pub seeds: Vec<String>,
    /// "tailscale" (default) or "static" (tests).
    pub identity: String,
    pub static_identity: Option<StaticIdentityConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StaticIdentityConfig {
    pub node_id: String,
    pub name: String,
    pub ips: Vec<String>,
    #[serde(default)]
    pub peers: Vec<StaticPeer>,
    /// Login name reported for every caller.
    #[serde(default = "default_static_login")]
    pub login: String,
}

fn default_static_login() -> String {
    "static@local".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StaticPeer {
    pub node_id: String,
    pub name: String,
    pub ips: Vec<String>,
    #[serde(default)]
    pub port: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: DEFAULT_PORT,
            data_dir: default_data_dir(),
            allowed_users: Vec::new(),
            allowed_tags: Vec::new(),
            grant_cap: "haasonsaas.dev/cap/flotilla".into(),
            labels: Labels::new(),
            tailscale_bin: None,
            max_concurrent_jobs: 2,
            gc_horizon_days: 7,
            job_retention_hours: 72,
            max_clock_skew_secs: 3600,
            job_lease_secs: 60,
            caffeinate_jobs: true,
            sync_interval_secs: 3,
            facts_interval_secs: 15,
            scheduler_interval_secs: 3,
            reconcile_interval_secs: 60,
            listen: Vec::new(),
            seeds: Vec::new(),
            identity: "tailscale".into(),
            static_identity: None,
        }
    }
}

pub fn home() -> PathBuf {
    directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"))
}

pub fn default_config_path() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".config"))
        .join("flotilla")
        .join("config.toml")
}

pub fn default_data_dir() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".local").join("share"))
        .join("flotilla")
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Config> {
        let path = path
            .map(Path::to_path_buf)
            .unwrap_or_else(default_config_path);
        if !path.exists() {
            tracing::info!(path = %path.display(), "no config file, using defaults");
            return Ok(Config::default());
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        tracing::info!(path = %path.display(), "config loaded");
        Ok(cfg)
    }

    pub fn jobs_dir(&self) -> PathBuf {
        self.data_dir.join("jobs")
    }

    pub fn job_log_path(&self, id: &str) -> PathBuf {
        self.jobs_dir().join(format!("{id}.log"))
    }

    pub fn settle_window(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.sync_interval_secs * 2 + 1)
    }

    pub fn lease(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.job_lease_secs.max(3))
    }

    /// Extra time past lease expiry before takeover, so a renewal in flight
    /// through sync is not mistaken for a dead executor.
    pub fn lease_grace(&self) -> std::time::Duration {
        self.settle_window()
    }
}
