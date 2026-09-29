//! Who am I, who are my peers, who is calling. Backed by the tailscale CLI
//! in production and by static config in tests.

use crate::config::{Config, StaticIdentityConfig, TailnetConfig};
use anyhow::{anyhow, bail, Context, Result};
use flotilla_core::api::PeerInfo;
use flotilla_core::schema::{TailnetHealth, TailnetInfo};
use serde_json::Value;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::process::Command;

#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node_id: String,
    pub name: String,
    pub hostname: String,
    pub os: String,
    /// Addresses on every active tailnet, primary tailnet first.
    pub ips: Vec<IpAddr>,
    /// Per-tailnet presence, primary first. The primary's `node_id` is
    /// `node_id` above.
    pub tailnets: Vec<TailnetInfo>,
}

#[derive(Clone, Debug)]
pub struct WhoIs {
    pub node_id: String,
    pub name: String,
    pub login: String,
    pub tags: Vec<String>,
    /// Application capabilities granted to this caller by the tailnet
    /// policy (`grants[].app`), as returned in `whois`'s `CapMap`.
    pub caps: std::collections::BTreeMap<String, Vec<Value>>,
}

/// One tailnet's source of identity.
enum Backend {
    Tailscale(Tailscale),
    Static(StaticIdentity),
}

impl Backend {
    async fn me(&self) -> Result<NodeInfo> {
        match self {
            Backend::Tailscale(t) => t.me().await,
            Backend::Static(s) => Ok(s.me.clone()),
        }
    }

    async fn peers(&self) -> Result<Vec<PeerInfo>> {
        match self {
            Backend::Tailscale(t) => t.peers().await,
            Backend::Static(s) => Ok(s.peers.clone()),
        }
    }

    async fn whois(&self, ip: IpAddr) -> Result<Option<WhoIs>> {
        match self {
            Backend::Tailscale(t) => t.whois(ip).await,
            Backend::Static(s) => Ok(s.whois(ip)),
        }
    }

    async fn own_login(&self) -> Result<Option<String>> {
        match self {
            Backend::Tailscale(t) => {
                let me = t.me().await?;
                for ip in me.ips {
                    if let Some(w) = t.whois(ip).await? {
                        return Ok(Some(w.login));
                    }
                }
                Ok(None)
            }
            Backend::Static(s) => Ok(Some(s.login.clone())),
        }
    }
}

struct Handle {
    name: String,
    conf: TailnetConfig,
    backend: Backend,
}

/// Tailnet name -> (peer addresses, proxy URL).
type ProxyRoutes = HashMap<String, (Vec<IpAddr>, String)>;

/// Who am I, who are my peers, who is calling, across every tailnet this
/// node is on. Tailnet 0 is the primary: its node id is the fleet identity.
pub struct IdentityProvider {
    tailnets: Vec<Handle>,
    name_override: Option<String>,
    /// tailnet name -> (peer addresses, socks5 URL) for userspace tailnets,
    /// refreshed on every peer list.
    routes: Arc<RwLock<ProxyRoutes>>,
}

/// How long a secondary tailnet gets to come up at startup (its tailscaled
/// may still be starting after a reboot) before the daemon runs without it.
const SECONDARY_WAIT: Duration = Duration::from_secs(30);

impl IdentityProvider {
    pub fn from_config(cfg: &Config) -> Result<IdentityProvider> {
        let confs: Vec<TailnetConfig> = if cfg.tailnet.is_empty() {
            vec![TailnetConfig {
                name: "default".into(),
                static_identity: cfg.static_identity.clone(),
                ..Default::default()
            }]
        } else {
            cfg.tailnet.clone()
        };
        let mut seen = std::collections::HashSet::new();
        let mut tailnets = Vec::new();
        for mut conf in confs {
            if conf.name.is_empty() {
                if cfg.tailnet.len() > 1 {
                    bail!("every [[tailnet]] needs a name");
                }
                conf.name = "default".into();
            }
            if !seen.insert(conf.name.clone()) {
                bail!("duplicate tailnet name {:?}", conf.name);
            }
            let backend = match cfg.identity.as_str() {
                "tailscale" => Backend::Tailscale(Tailscale::new(
                    conf.bin.clone().or_else(|| cfg.tailscale_bin.clone()),
                    conf.socket.clone(),
                )?),
                "static" => {
                    let sc = conf
                        .static_identity
                        .clone()
                        .ok_or_else(|| anyhow!("identity=static needs [static_identity]"))?;
                    Backend::Static(StaticIdentity::new(sc)?)
                }
                other => bail!("unknown identity provider {other:?}"),
            };
            tailnets.push(Handle {
                name: conf.name.clone(),
                conf,
                backend,
            });
        }
        Ok(IdentityProvider {
            tailnets,
            name_override: cfg.name.clone(),
            routes: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn tailnet_conf(&self, idx: usize) -> Option<&TailnetConfig> {
        self.tailnets.get(idx).map(|h| &h.conf)
    }

    pub fn tailnet_index(&self, name: &str) -> Option<usize> {
        self.tailnets.iter().position(|h| h.name == name)
    }

    /// This node: the primary tailnet's identity plus its presence on every
    /// secondary tailnet that is up. Secondaries that are down or logged out
    /// are skipped (with a warning) after a grace period.
    pub async fn me(&self) -> Result<NodeInfo> {
        let mut primary = self.tailnets[0].backend.me().await?;
        let mut tailnets = vec![TailnetInfo {
            name: self.tailnets[0].name.clone(),
            node_id: primary.node_id.clone(),
            ips: primary.ips.iter().map(ToString::to_string).collect(),
        }];
        let mut ips = primary.ips.clone();
        for h in &self.tailnets[1..] {
            let deadline = Instant::now() + SECONDARY_WAIT;
            let n = loop {
                let r = h.backend.me().await;
                let ready = match &r {
                    Ok(n) => !n.ips.is_empty() || matches!(h.backend, Backend::Static(_)),
                    Err(_) => false,
                };
                if ready {
                    break r.ok();
                }
                if Instant::now() >= deadline {
                    tracing::warn!(tailnet = %h.name, error = ?r.err(), "tailnet is not up; running without it");
                    break None;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            };
            if let Some(n) = n {
                ips.extend(n.ips.iter().copied());
                tailnets.push(TailnetInfo {
                    name: h.name.clone(),
                    node_id: n.node_id,
                    ips: n.ips.iter().map(ToString::to_string).collect(),
                });
            }
        }
        primary.ips = ips;
        primary.tailnets = tailnets;
        if let Some(n) = &self.name_override {
            primary.name = n.clone();
        }
        Ok(primary)
    }

    /// Login state of each tailnet's tailscaled, asked fresh (the status
    /// cache would keep reporting `Running` for a daemon that just died).
    pub async fn health(&self) -> Vec<TailnetHealth> {
        let mut out = Vec::new();
        for h in &self.tailnets {
            let state = match &h.backend {
                Backend::Static(_) => "Running".to_string(),
                Backend::Tailscale(t) => t.backend_state().await,
            };
            out.push(TailnetHealth {
                name: h.name.clone(),
                state,
            });
        }
        out
    }

    /// Peers from every tailnet, each tagged with the tailnet that reported
    /// it. One tailnet failing does not hide the others.
    pub async fn peers(&self) -> Result<Vec<PeerInfo>> {
        let mut out = Vec::new();
        let mut first_err = None;
        let mut ok = false;
        for h in &self.tailnets {
            match h.backend.peers().await {
                Ok(peers) => {
                    ok = true;
                    if let Some(proxy) = h.conf.proxy_url() {
                        let ips = peers
                            .iter()
                            .flat_map(|p| p.ips.iter())
                            .filter_map(|s| s.parse().ok())
                            .collect();
                        self.routes
                            .write()
                            .unwrap()
                            .insert(h.name.clone(), (ips, proxy));
                    }
                    out.extend(peers.into_iter().map(|mut p| {
                        p.tailnet = h.name.clone();
                        p
                    }));
                }
                Err(e) => {
                    tracing::debug!(tailnet = %h.name, error = %e, "peer list failed");
                    first_err.get_or_insert(e);
                }
            }
        }
        match (ok, first_err) {
            (false, Some(e)) => Err(e),
            _ => Ok(out),
        }
    }

    /// Who owns `ip`, according to tailnet `idx`. Addresses can overlap
    /// between tailnets, so callers must say which tailnet the connection
    /// arrived on.
    pub async fn whois_in(&self, idx: usize, ip: IpAddr) -> Result<Option<WhoIs>> {
        match self.tailnets.get(idx) {
            Some(h) => h.backend.whois(ip).await,
            None => Ok(None),
        }
    }

    /// Login name of tailnet `idx`'s own user, for the default allow-list.
    pub async fn own_login_in(&self, idx: usize) -> Result<Option<String>> {
        match self.tailnets.get(idx) {
            Some(h) => h.backend.own_login().await,
            None => Ok(None),
        }
    }

    /// Route requests to peers of userspace-networking tailnets through
    /// their SOCKS5 servers. None when no tailnet needs it.
    pub fn proxy(&self) -> Option<reqwest::Proxy> {
        let proxied: Vec<String> = self
            .tailnets
            .iter()
            .filter_map(|h| h.conf.proxy_url())
            .collect();
        if proxied.is_empty() {
            return None;
        }
        let routes = self.routes.clone();
        Some(reqwest::Proxy::custom(move |url| {
            let ip: IpAddr = url
                .host_str()?
                .trim_matches(|c| c == '[' || c == ']')
                .parse()
                .ok()?;
            let known = routes
                .read()
                .unwrap()
                .values()
                .find(|(ips, _)| ips.contains(&ip))
                .map(|(_, proxy)| proxy.clone());
            // An address not (yet) in any peer list, e.g. a configured seed,
            // still has to go through the proxy if only one tailnet has one.
            let chosen = known.or_else(|| {
                (proxied.len() == 1 && in_tailscale_range(ip)).then(|| proxied[0].clone())
            })?;
            chosen.parse::<reqwest::Url>().ok()
        }))
    }
}

// ---------------------------------------------------------------------------

pub struct Tailscale {
    bin: PathBuf,
    socket: Option<PathBuf>,
    status_cache: Arc<Mutex<Option<(Instant, Value)>>>,
    /// Set while a background refresh of the status cache is in flight.
    refreshing: Arc<std::sync::atomic::AtomicBool>,
    whois_cache: Mutex<HashMap<IpAddr, (Instant, Option<WhoIs>)>>,
}

const STATUS_TTL: Duration = Duration::from_secs(5);
const WHOIS_TTL: Duration = Duration::from_secs(20);
/// Bound on a single `tailscale` invocation. Under heavy load the CLI can
/// stall; requests must not hang on it.
const CLI_TIMEOUT: Duration = Duration::from_secs(8);

impl Tailscale {
    pub fn new(bin: Option<PathBuf>, socket: Option<PathBuf>) -> Result<Tailscale> {
        let bin = match bin {
            Some(b) => b,
            None => find_tailscale()
                .ok_or_else(|| anyhow!("tailscale CLI not found; set tailscale_bin in config"))?,
        };
        Ok(Tailscale {
            bin,
            socket,
            status_cache: Arc::new(Mutex::new(None)),
            refreshing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            whois_cache: Mutex::new(HashMap::new()),
        })
    }

    async fn run(&self, args: &[&str]) -> Result<Value> {
        run_cli(&self.bin, self.socket.as_deref(), args).await
    }
}

/// One `tailscale` invocation, bounded by CLI_TIMEOUT.
async fn run_cli(bin: &Path, socket: Option<&Path>, args: &[&str]) -> Result<Value> {
    {
        let mut cmd = Command::new(bin);
        if let Some(sock) = socket {
            let mut flag = std::ffi::OsString::from("--socket=");
            flag.push(sock);
            cmd.arg(flag);
        }
        let out = tokio::time::timeout(CLI_TIMEOUT, cmd.args(args).output())
            .await
            .with_context(|| format!("tailscale {:?} timed out after {:?}", args, CLI_TIMEOUT))?
            .with_context(|| format!("running {}", bin.display()))?;
        if !out.status.success() {
            bail!(
                "tailscale {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        serde_json::from_slice(&out.stdout)
            .with_context(|| format!("parsing tailscale {:?} output", args))
    }
}

impl Tailscale {
    /// `BackendState` from a fresh `status --json`, or `unreachable`.
    pub async fn backend_state(&self) -> String {
        match self.run(&["status", "--json"]).await {
            Ok(v) => parse_backend_state(&v),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("NeedsLogin") || msg.contains("Logged out") {
                    "NeedsLogin".into()
                } else {
                    "unreachable".into()
                }
            }
        }
    }

    /// The peer list, never blocking on the tailscale CLI once we have one:
    /// a stale cache is returned immediately and refreshed in the
    /// background. Only the very first call (no cache yet) waits.
    async fn status(&self) -> Result<Value> {
        let cached = self.status_cache.lock().unwrap().clone();
        match cached {
            Some((at, v)) if at.elapsed() < STATUS_TTL => Ok(v),
            Some((at, v)) => {
                self.refresh_in_background(at.elapsed());
                Ok(v)
            }
            None => {
                let v = run_cli(&self.bin, self.socket.as_deref(), &["status", "--json"]).await?;
                *self.status_cache.lock().unwrap() = Some((Instant::now(), v.clone()));
                Ok(v)
            }
        }
    }

    fn refresh_in_background(&self, age: Duration) {
        use std::sync::atomic::Ordering;
        if self.refreshing.swap(true, Ordering::SeqCst) {
            return;
        }
        let bin = self.bin.clone();
        let socket = self.socket.clone();
        let cache = self.status_cache.clone();
        let flag = self.refreshing.clone();
        tokio::spawn(async move {
            match run_cli(&bin, socket.as_deref(), &["status", "--json"]).await {
                Ok(v) => *cache.lock().unwrap() = Some((Instant::now(), v)),
                Err(e) => {
                    if age > STATUS_TTL * 6 {
                        tracing::warn!(error = %e, age_s = age.as_secs(), "tailscale status keeps failing; peer list is stale");
                    }
                }
            }
            flag.store(false, Ordering::SeqCst);
        });
    }

    pub async fn me(&self) -> Result<NodeInfo> {
        let st = self.status().await?;
        let s = st
            .get("Self")
            .ok_or_else(|| anyhow!("tailscale status has no Self"))?;
        Ok(NodeInfo {
            node_id: str_field(s, "ID")?,
            name: short_name(&str_field(s, "DNSName")?),
            hostname: s
                .get("HostName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            os: s
                .get("OS")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            ips: ips_of(s),
            tailnets: Vec::new(),
        })
    }

    pub async fn peers(&self) -> Result<Vec<PeerInfo>> {
        let st = self.status().await?;
        let mut out = Vec::new();
        if let Some(peers) = st.get("Peer").and_then(Value::as_object) {
            for p in peers.values() {
                out.push(PeerInfo {
                    node_id: str_field(p, "ID")?,
                    name: short_name(p.get("DNSName").and_then(Value::as_str).unwrap_or("")),
                    online: p.get("Online").and_then(Value::as_bool).unwrap_or(false),
                    ips: ips_of(p).iter().map(ToString::to_string).collect(),
                    os: p
                        .get("OS")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    tags: p
                        .get("Tags")
                        .and_then(Value::as_array)
                        .map(|a| {
                            a.iter()
                                .filter_map(Value::as_str)
                                .map(String::from)
                                .collect()
                        })
                        .unwrap_or_default(),
                    tailnet: String::new(),
                });
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    pub async fn whois(&self, ip: IpAddr) -> Result<Option<WhoIs>> {
        if let Some((at, w)) = self.whois_cache.lock().unwrap().get(&ip) {
            if at.elapsed() < WHOIS_TTL {
                return Ok(w.clone());
            }
        }
        let ip_s = ip.to_string();
        let result = match self.run(&["whois", "--json", &ip_s]).await {
            Ok(v) => {
                let node = v.get("Node").ok_or_else(|| anyhow!("whois has no Node"))?;
                Some(WhoIs {
                    node_id: node
                        .get("StableID")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    name: short_name(node.get("Name").and_then(Value::as_str).unwrap_or("")),
                    login: v
                        .pointer("/UserProfile/LoginName")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    tags: node
                        .get("Tags")
                        .and_then(Value::as_array)
                        .map(|a| {
                            a.iter()
                                .filter_map(Value::as_str)
                                .map(String::from)
                                .collect()
                        })
                        .unwrap_or_default(),
                    caps: v
                        .get("CapMap")
                        .and_then(Value::as_object)
                        .map(|m| {
                            m.iter()
                                .map(|(k, v)| {
                                    (k.clone(), v.as_array().cloned().unwrap_or_default())
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                })
            }
            Err(e) => {
                // Keep the last answer for this IP if we have one: a slow
                // CLI must not lock out (or admit) a peer at random.
                if let Some((_, prev)) = self.whois_cache.lock().unwrap().get(&ip) {
                    tracing::warn!(%ip, error = %e, "whois failed; using cached identity");
                    return Ok(prev.clone());
                }
                tracing::debug!(%ip, error = %e, "whois failed");
                None
            }
        };
        self.whois_cache
            .lock()
            .unwrap()
            .insert(ip, (Instant::now(), result.clone()));
        Ok(result)
    }
}

/// 100.64.0.0/10 or fd7a:115c:a1e0::/48, the ranges Tailscale hands out.
/// `BackendState` of a `tailscale status --json` document.
fn parse_backend_state(v: &Value) -> String {
    v.get("BackendState")
        .and_then(Value::as_str)
        .unwrap_or("unreachable")
        .to_string()
}

fn in_tailscale_range(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            o[0] == 100 && (o[1] & 0xc0) == 64
        }
        IpAddr::V6(v6) => v6.segments()[..3] == [0xfd7a, 0x115c, 0xa1e0],
    }
}

fn str_field(v: &Value, k: &str) -> Result<String> {
    v.get(k)
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or_else(|| anyhow!("missing {k}"))
}

fn ips_of(v: &Value) -> Vec<IpAddr> {
    v.get("TailscaleIPs")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .filter_map(|s| s.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// `jonathan-air.example.ts.net.` -> `jonathan-air`
pub fn short_name(dns: &str) -> String {
    dns.split('.').next().unwrap_or(dns).to_string()
}

fn find_tailscale() -> Option<PathBuf> {
    if let Some(p) = which("tailscale") {
        return Some(p);
    }
    for c in [
        "/opt/homebrew/bin/tailscale",
        "/usr/local/bin/tailscale",
        "/usr/bin/tailscale",
        "/Applications/Tailscale.app/Contents/MacOS/Tailscale",
    ] {
        let p = PathBuf::from(c);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

fn which(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|d| d.join(name))
            .find(|p| p.is_file())
    })
}

// ---------------------------------------------------------------------------

pub struct StaticIdentity {
    me: NodeInfo,
    peers: Vec<PeerInfo>,
    login: String,
    by_ip: HashMap<IpAddr, WhoIs>,
}

impl StaticIdentity {
    fn new(sc: StaticIdentityConfig) -> Result<StaticIdentity> {
        let me = NodeInfo {
            node_id: sc.node_id.clone(),
            name: sc.name.clone(),
            hostname: sc.name.clone(),
            os: std::env::consts::OS.to_string(),
            ips: sc.ips.iter().map(|s| s.parse()).collect::<Result<_, _>>()?,
            tailnets: Vec::new(),
        };
        let mut by_ip = HashMap::new();
        for ip in &me.ips {
            by_ip.insert(
                *ip,
                WhoIs {
                    node_id: me.node_id.clone(),
                    name: me.name.clone(),
                    login: sc.login.clone(),
                    tags: vec![],
                    caps: Default::default(),
                },
            );
        }
        let mut peers = Vec::new();
        for p in &sc.peers {
            for ip in &p.ips {
                let ip: IpAddr = ip.parse()?;
                by_ip.insert(
                    ip,
                    WhoIs {
                        node_id: p.node_id.clone(),
                        name: p.name.clone(),
                        login: sc.login.clone(),
                        tags: vec![],
                        caps: Default::default(),
                    },
                );
            }
            peers.push(PeerInfo {
                node_id: p.node_id.clone(),
                name: p.name.clone(),
                online: true,
                ips: p
                    .ips
                    .iter()
                    .map(|ip| match p.port {
                        Some(port) => format!("{ip}:{port}"),
                        None => ip.clone(),
                    })
                    .collect(),
                os: std::env::consts::OS.to_string(),
                tags: vec![],
                tailnet: String::new(),
            });
        }
        Ok(StaticIdentity {
            me,
            peers,
            login: sc.login,
            by_ip,
        })
    }

    fn whois(&self, ip: IpAddr) -> Option<WhoIs> {
        self.by_ip.get(&ip).cloned().or_else(|| {
            if ip.is_loopback() {
                Some(WhoIs {
                    node_id: "loopback".into(),
                    name: "loopback".into(),
                    login: self.login.clone(),
                    tags: vec![],
                    caps: Default::default(),
                })
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StaticIdentityConfig;

    #[test]
    fn short_names() {
        assert_eq!(
            short_name("jonathan-air.angler-centauri.ts.net."),
            "jonathan-air"
        );
        assert_eq!(short_name("plain"), "plain");
    }

    fn static_tailnet(
        name: &str,
        me_id: &str,
        ips: &[&str],
        peers: &[(&str, &str, &str)],
    ) -> TailnetConfig {
        TailnetConfig {
            name: name.into(),
            static_identity: Some(StaticIdentityConfig {
                node_id: me_id.into(),
                name: "box".into(),
                ips: ips.iter().map(|s| s.to_string()).collect(),
                login: format!("{name}-user@example.com"),
                peers: peers
                    .iter()
                    .map(|(id, n, ip)| crate::config::StaticPeer {
                        node_id: id.to_string(),
                        name: n.to_string(),
                        ips: vec![ip.to_string()],
                        port: None,
                    })
                    .collect(),
            }),
            ..Default::default()
        }
    }

    fn two_tailnets() -> IdentityProvider {
        // Both tailnets hand out 100.64.0.5, to different machines.
        let cfg = Config {
            identity: "static".into(),
            name: Some("boxname".into()),
            tailnet: vec![
                static_tailnet(
                    "evalops",
                    "e-1",
                    &["100.64.0.1"],
                    &[("e-p", "peer-e", "100.64.0.5")],
                ),
                static_tailnet(
                    "homelab",
                    "h-1",
                    &["100.99.0.1"],
                    &[("h-p", "peer-h", "100.64.0.5")],
                ),
            ],
            ..Config::default()
        };
        IdentityProvider::from_config(&cfg).unwrap()
    }

    #[tokio::test]
    async fn me_merges_tailnets_primary_first() {
        let id = two_tailnets();
        let me = id.me().await.unwrap();
        assert_eq!(me.node_id, "e-1", "primary tailnet's id is the fleet id");
        assert_eq!(me.name, "boxname", "name override applies");
        assert_eq!(
            me.ips,
            vec![
                "100.64.0.1".parse::<IpAddr>().unwrap(),
                "100.99.0.1".parse().unwrap()
            ]
        );
        let names: Vec<_> = me
            .tailnets
            .iter()
            .map(|t| (t.name.as_str(), t.node_id.as_str()))
            .collect();
        assert_eq!(names, vec![("evalops", "e-1"), ("homelab", "h-1")]);
    }

    #[tokio::test]
    async fn peers_are_merged_and_tagged_by_tailnet() {
        let id = two_tailnets();
        let peers = id.peers().await.unwrap();
        let got: Vec<_> = peers
            .iter()
            .map(|p| (p.name.as_str(), p.tailnet.as_str()))
            .collect();
        assert_eq!(got, vec![("peer-e", "evalops"), ("peer-h", "homelab")]);
    }

    #[tokio::test]
    async fn whois_is_asked_of_the_tailnet_the_caller_arrived_on() {
        let id = two_tailnets();
        let ip: IpAddr = "100.64.0.5".parse().unwrap();
        let e = id.whois_in(0, ip).await.unwrap().unwrap();
        let h = id.whois_in(1, ip).await.unwrap().unwrap();
        assert_eq!(
            (e.node_id.as_str(), e.login.as_str()),
            ("e-p", "evalops-user@example.com")
        );
        assert_eq!(
            (h.node_id.as_str(), h.login.as_str()),
            ("h-p", "homelab-user@example.com")
        );
        assert!(id.whois_in(7, ip).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dead_primary_tailnet_is_fatal() {
        let cfg = Config {
            identity: "tailscale".into(),
            tailscale_bin: Some(PathBuf::from("/bin/false")),
            tailnet: vec![TailnetConfig {
                name: "only".into(),
                ..Default::default()
            }],
            ..Config::default()
        };
        let id = IdentityProvider::from_config(&cfg).unwrap();
        assert!(id.me().await.is_err(), "a dead primary is fatal");
    }

    #[test]
    fn tailnet_names_must_be_unique_and_present() {
        let dup = Config {
            identity: "static".into(),
            tailnet: vec![
                static_tailnet("a", "1", &[], &[]),
                static_tailnet("a", "2", &[], &[]),
            ],
            ..Config::default()
        };
        assert!(IdentityProvider::from_config(&dup).is_err());
        let unnamed = Config {
            identity: "static".into(),
            tailnet: vec![
                static_tailnet("", "1", &[], &[]),
                static_tailnet("b", "2", &[], &[]),
            ],
            ..Config::default()
        };
        assert!(IdentityProvider::from_config(&unnamed).is_err());
    }

    #[test]
    fn tailscale_range() {
        assert!(in_tailscale_range("100.64.0.1".parse().unwrap()));
        assert!(in_tailscale_range("100.127.255.1".parse().unwrap()));
        assert!(!in_tailscale_range("100.128.0.1".parse().unwrap()));
        assert!(!in_tailscale_range("10.0.0.1".parse().unwrap()));
        assert!(in_tailscale_range("fd7a:115c:a1e0::1".parse().unwrap()));
        assert!(!in_tailscale_range("fd00::1".parse().unwrap()));
    }

    #[test]
    fn proxy_routes_only_tailscale_addresses() {
        let cfg = Config {
            identity: "static".into(),
            tailnet: vec![TailnetConfig {
                proxy: Some("http://127.0.0.1:1057".into()),
                ..static_tailnet("evalops", "e-1", &[], &[])
            }],
            ..Config::default()
        };
        let id = IdentityProvider::from_config(&cfg).unwrap();
        assert!(id.proxy().is_some());
        let plain = Config {
            identity: "static".into(),
            tailnet: vec![static_tailnet("evalops", "e-1", &[], &[])],
            ..Config::default()
        };
        assert!(IdentityProvider::from_config(&plain)
            .unwrap()
            .proxy()
            .is_none());
    }

    #[test]
    fn parses_status_shape() {
        let v: Value = serde_json::json!({
            "Self": {"ID": "abc", "DNSName": "me.x.ts.net.", "HostName": "me", "OS": "macOS",
                     "TailscaleIPs": ["100.1.1.1", "fd7a::1"]},
            "Peer": {"k": {"ID": "p1", "DNSName": "peer.x.ts.net.", "OS": "linux", "Online": true,
                            "TailscaleIPs": ["100.2.2.2"], "Tags": ["tag:a"]}}
        });
        let ts = Tailscale {
            bin: PathBuf::from("/bin/false"),
            socket: None,
            status_cache: Arc::new(Mutex::new(Some((Instant::now(), v)))),
            refreshing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            whois_cache: Mutex::new(HashMap::new()),
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let me = rt.block_on(ts.me()).unwrap();
        assert_eq!(me.node_id, "abc");
        assert_eq!(me.name, "me");
        assert_eq!(me.ips.len(), 2);
        let peers = rt.block_on(ts.peers()).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].name, "peer");
        assert!(peers[0].online);
        assert_eq!(peers[0].tags, vec!["tag:a"]);
    }

    #[test]
    fn backend_state_from_status_json() {
        let v = serde_json::json!({"BackendState": "NeedsLogin", "Self": {}});
        assert_eq!(parse_backend_state(&v), "NeedsLogin");
        assert_eq!(parse_backend_state(&serde_json::json!({})), "unreachable");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn health_reads_fresh_state_from_the_cli() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("flotilla-ts-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let fake = dir.join("tailscale");
        std::fs::write(
            &fake,
            "#!/bin/sh\necho '{\"BackendState\":\"NeedsLogin\"}'\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let logged_out = Tailscale::new(Some(fake), None).unwrap();
        assert_eq!(logged_out.backend_state().await, "NeedsLogin");
        // a dead daemon makes the CLI exit non-zero
        let dead = Tailscale::new(Some(PathBuf::from("/bin/false")), None).unwrap();
        assert_eq!(dead.backend_state().await, "unreachable");
        // a static tailnet always reports Running
        let p = two_tailnets();
        let h = p.health().await;
        assert_eq!(h.len(), 2);
        assert!(h.iter().all(|t| t.state == "Running"));
    }
}
