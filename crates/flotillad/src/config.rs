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
    /// Extra labels advertised in this node's facts.
    pub labels: Labels,
    /// Path to the tailscale CLI. Auto-detected if unset.
    pub tailscale_bin: Option<PathBuf>,
    pub max_concurrent_jobs: usize,
    pub sync_interval_secs: u64,
    pub facts_interval_secs: u64,
    pub scheduler_interval_secs: u64,
    pub reconcile_interval_secs: u64,
    /// Extra listen addresses (host:port). Loopback and Tailscale IPs are always bound.
    pub listen: Vec<String>,
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
            labels: Labels::new(),
            tailscale_bin: None,
            max_concurrent_jobs: 2,
            sync_interval_secs: 3,
            facts_interval_secs: 15,
            scheduler_interval_secs: 3,
            reconcile_interval_secs: 60,
            listen: Vec::new(),
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
}
