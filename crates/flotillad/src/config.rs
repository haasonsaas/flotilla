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
    /// grant `"app": {"haasonsaas.com/cap/flotilla": [{"roles": ["read"]}]}`.
    /// Callers matched by allowed_users/allowed_tags get every role.
    pub grant_cap: String,
    /// Extra labels advertised in this node's facts.
    pub labels: Labels,
    /// Path to the tailscale CLI. Auto-detected if unset.
    pub tailscale_bin: Option<PathBuf>,
    pub max_concurrent_jobs: usize,
    /// Most tmux sessions `flotilla session start` may place here without
    /// `-n`. Unset means no cap. Advertised in facts.
    pub max_sessions: Option<usize>,
    /// Tombstones older than this are collected. Peers that have not
    /// synced within this window may keep a stale copy of a deleted key.
    pub gc_horizon_days: u64,
    /// How long to remember collected keys so stale copies stay rejected.
    pub gc_forget_days: u64,
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
    /// Optional push notifications (ntfy) for job outcomes on this node.
    pub notify: Option<NotifyConfig>,
    /// Watchdog thresholds for this node's own health.
    pub alerts: AlertsConfig,
    /// Extra listen addresses (host:port). Loopback and Tailscale IPs are always bound.
    pub listen: Vec<String>,
    /// Peers to sync with that may not be discoverable yet or that listen on a
    /// non-default port, as `host:port`. Once a peer's facts are known its
    /// advertised port is used instead.
    pub seeds: Vec<String>,
    /// Override the node name (default: the first tailnet's node name).
    pub name: Option<String>,
    /// Tailnets this node is on, each with its own tailscaled. The first is
    /// the primary: its node id is this node's fleet identity. Empty means
    /// one implicit tailnet reached through `tailscale_bin` / the default
    /// socket.
    pub tailnet: Vec<TailnetConfig>,
    /// Build caches this node advertises as warm in its facts.
    pub warm_cache: Vec<WarmCacheConfig>,
    /// "tailscale" (default) or "static" (tests).
    pub identity: String,
    pub static_identity: Option<StaticIdentityConfig>,
}

/// `[[warm_cache]]`: a build cache directory this node keeps and advertises
/// as `warm.<name>` in its facts (label `<key>@<age>`, plus size and
/// last-used time in the structured `warm` field).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WarmCacheConfig {
    /// Cache name, e.g. `mono-rust`. Jobs ask for it with `--prefer-warm`.
    pub name: String,
    /// The cache directory, e.g. `/builds/mono/target`. Not warm if missing.
    pub path: PathBuf,
    /// Shell command whose first output line identifies what the cache was
    /// built from, e.g. `git -C /builds/mono rev-parse --short HEAD`.
    pub key_cmd: Option<String>,
}

/// `[[tailnet]]`: one tailnet this node is on.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TailnetConfig {
    /// Label shown in `flotilla status` / `peers`, e.g. `evalops`.
    pub name: String,
    /// tailscaled socket for this tailnet (`tailscale --socket=...`).
    /// Unset uses the CLI's default socket.
    pub socket: Option<PathBuf>,
    /// tailscale CLI for this tailnet. Falls back to `tailscale_bin`, then
    /// auto-detection.
    pub bin: Option<PathBuf>,
    /// For a tailscaled in userspace-networking mode, whose Tailscale
    /// addresses are not local interfaces: a proxy URL through which
    /// outbound requests to this tailnet's peers are sent, e.g.
    /// `socks5://127.0.0.1:1056` (its `--socks5-server`) or
    /// `http://127.0.0.1:1057` (its `--outbound-http-proxy-listen`).
    pub proxy: Option<String>,
    /// Shorthand for `proxy = "socks5://<host:port>"`.
    pub socks5: Option<String>,
    /// For userspace-networking: a loopback `host:port` on which this daemon
    /// accepts PROXY-protocol connections, fed by
    /// `tailscale serve --tcp=<port> --proxy-protocol=2 tcp://<host:port>`.
    /// The real caller address comes from the PROXY header and is resolved
    /// with `whois` against this tailnet.
    pub proxy_listen: Option<String>,
    /// Replace the global `allowed_users` for callers arriving on this tailnet.
    pub allowed_users: Option<Vec<String>>,
    /// Replace the global `allowed_tags` for callers arriving on this tailnet.
    pub allowed_tags: Option<Vec<String>>,
    /// Tests only, with `identity = "static"`.
    pub static_identity: Option<StaticIdentityConfig>,
}

impl TailnetConfig {
    /// The outbound proxy URL for this tailnet, if any.
    pub fn proxy_url(&self) -> Option<String> {
        self.proxy
            .clone()
            .or_else(|| self.socks5.as_ref().map(|h| format!("socks5://{h}")))
    }
}

impl Config {
    /// The allow-lists that apply to callers arriving on tailnet `idx`:
    /// the tailnet's own if it sets either, else the global ones.
    pub fn allow_lists(&self, idx: usize) -> (&[String], &[String]) {
        match self.tailnet.get(idx) {
            Some(t) if t.allowed_users.is_some() || t.allowed_tags.is_some() => (
                t.allowed_users.as_deref().unwrap_or(&[]),
                t.allowed_tags.as_deref().unwrap_or(&[]),
            ),
            _ => (&self.allowed_users, &self.allowed_tags),
        }
    }
}

/// `[notify]`: POST a line to an ntfy topic when a job this node ran
/// finishes, and when this node takes over a lost job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotifyConfig {
    /// e.g. `https://ntfy.sh` or `http://ntfy:80`
    pub ntfy_url: String,
    pub topic: String,
    /// Which outcomes to send: any of succeeded, failed, cancelled, lost. Default: failed, lost.
    #[serde(default = "default_notify_on")]
    pub on: Vec<String>,
    /// Optional bearer/basic token for the ntfy server.
    #[serde(default)]
    pub token: Option<String>,
}

/// `[alerts]`: thresholds checked against this node's facts every facts
/// interval. A tripped threshold writes an `alert/<node>/<id>` record and,
/// if a destination is configured, POSTs once when it starts firing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertsConfig {
    /// Fire when any tracked volume has less than this percent free.
    /// 0 disables.
    pub disk_free_pct_min: f64,
    /// Fire when load per core exceeds this. 0 (default) disables.
    pub load_per_core_max: f64,
    /// Fire when a tailnet's tailscaled is logged out (NeedsLogin), stopped,
    /// or does not answer.
    pub tailscale_logged_out: bool,
    /// Seconds a condition must persist before it fires, so a tailscaled
    /// restart or a brief spike does not page anyone. Default 60.
    pub for_secs: u64,
    /// ntfy server; falls back to `[notify] ntfy_url`.
    pub ntfy_url: Option<String>,
    /// ntfy topic; falls back to `[notify] topic`.
    pub topic: Option<String>,
    /// ntfy token; falls back to `[notify] token`.
    pub token: Option<String>,
    /// URL that receives the alert record as JSON in a POST.
    pub webhook_url: Option<String>,
    /// Also send when an alert clears.
    pub notify_resolved: bool,
    /// Send again every this many hours while still firing. 0 sends once.
    pub renotify_hours: u64,
}

impl Default for AlertsConfig {
    fn default() -> Self {
        AlertsConfig {
            disk_free_pct_min: 10.0,
            load_per_core_max: 0.0,
            tailscale_logged_out: true,
            for_secs: 60,
            ntfy_url: None,
            topic: None,
            token: None,
            webhook_url: None,
            notify_resolved: false,
            renotify_hours: 0,
        }
    }
}

fn default_notify_on() -> Vec<String> {
    vec!["failed".into(), "lost".into()]
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
            grant_cap: "haasonsaas.com/cap/flotilla".into(),
            labels: Labels::new(),
            tailscale_bin: None,
            max_concurrent_jobs: 2,
            max_sessions: None,
            gc_horizon_days: 30,
            gc_forget_days: 365,
            job_retention_hours: 72,
            max_clock_skew_secs: 3600,
            job_lease_secs: 60,
            caffeinate_jobs: true,
            sync_interval_secs: 3,
            facts_interval_secs: 15,
            scheduler_interval_secs: 3,
            reconcile_interval_secs: 60,
            notify: None,
            alerts: AlertsConfig::default(),
            listen: Vec::new(),
            seeds: Vec::new(),
            name: None,
            tailnet: Vec::new(),
            warm_cache: Vec::new(),
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

    /// Files a job writes here (env `FLOTILLA_ARTIFACTS`) are served by
    /// `/v1/jobs/{id}/artifacts` and pulled by `flotilla job pull`.
    pub fn job_artifacts_dir(&self, id: &str) -> PathBuf {
        self.jobs_dir().join("artifacts").join(id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_warm_cache_tables() {
        let cfg: Config = toml::from_str(
            r#"
            [[warm_cache]]
            name = "mono-rust"
            path = "/builds/mono/target"
            key_cmd = "git -C /builds/mono rev-parse --short HEAD"
            [[warm_cache]]
            name = "npm"
            path = "/builds/npm"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.warm_cache.len(), 2);
        assert_eq!(cfg.warm_cache[0].name, "mono-rust");
        assert!(cfg.warm_cache[1].key_cmd.is_none());
        assert!(Config::default().warm_cache.is_empty());
    }

    #[test]
    fn parses_tailnet_tables() {
        let cfg: Config = toml::from_str(
            r#"
            name = "mac-mini"
            allowed_users = ["me@example.com"]
            [[tailnet]]
            name = "evalops"
            socket = "/var/run/tailscaled-evalops.sock"
            proxy = "http://127.0.0.1:1057"
            proxy_listen = "127.0.0.1:7411"
            [[tailnet]]
            name = "homelab"
            allowed_users = ["home@example.com"]
            "#,
        )
        .unwrap();
        assert_eq!(cfg.tailnet.len(), 2);
        assert_eq!(
            cfg.tailnet[0].proxy_url().as_deref(),
            Some("http://127.0.0.1:1057")
        );
        assert_eq!(cfg.allow_lists(0).0, ["me@example.com"]);
        assert_eq!(cfg.allow_lists(1).0, ["home@example.com"]);
        assert!(
            cfg.allow_lists(1).1.is_empty(),
            "a tailnet's own list replaces both global lists"
        );
        let s5 = TailnetConfig {
            socks5: Some("127.0.0.1:1056".into()),
            ..Default::default()
        };
        assert_eq!(s5.proxy_url().as_deref(), Some("socks5://127.0.0.1:1056"));
        // no [[tailnet]]: today's single-tailnet config is unchanged
        let old: Config = toml::from_str("allowed_tags = [\"tag:x\"]").unwrap();
        assert!(old.tailnet.is_empty());
        assert_eq!(old.allow_lists(0).1, ["tag:x"]);
    }

    #[test]
    fn parses_alerts_table() {
        let cfg: Config = toml::from_str(
            r#"
            [alerts]
            disk_free_pct_min = 15
            tailscale_logged_out = false
            ntfy_url = "https://ntfy.sh"
            topic = "fleet"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.alerts.disk_free_pct_min, 15.0);
        assert!(!cfg.alerts.tailscale_logged_out);
        assert_eq!(cfg.alerts.for_secs, 60);
        let d: Config = toml::from_str("").unwrap();
        assert_eq!(d.alerts.disk_free_pct_min, 10.0);
        assert!(d.alerts.tailscale_logged_out);
    }
}
