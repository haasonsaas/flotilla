//! Who am I, who are my peers, who is calling. Backed by the tailscale CLI
//! in production and by static config in tests.

use crate::config::{Config, StaticIdentityConfig};
use anyhow::{anyhow, bail, Context, Result};
use flotilla_core::api::PeerInfo;
use serde_json::Value;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::process::Command;

#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node_id: String,
    pub name: String,
    pub hostname: String,
    pub os: String,
    pub ips: Vec<IpAddr>,
}

#[derive(Clone, Debug)]
pub struct WhoIs {
    pub node_id: String,
    pub name: String,
    pub login: String,
    pub tags: Vec<String>,
}

pub enum IdentityProvider {
    Tailscale(Tailscale),
    Static(StaticIdentity),
}

impl IdentityProvider {
    pub fn from_config(cfg: &Config) -> Result<IdentityProvider> {
        match cfg.identity.as_str() {
            "tailscale" => Ok(IdentityProvider::Tailscale(Tailscale::new(
                cfg.tailscale_bin.clone(),
            )?)),
            "static" => {
                let sc = cfg
                    .static_identity
                    .clone()
                    .ok_or_else(|| anyhow!("identity=static needs [static_identity]"))?;
                Ok(IdentityProvider::Static(StaticIdentity::new(sc)?))
            }
            other => bail!("unknown identity provider {other:?}"),
        }
    }

    pub async fn me(&self) -> Result<NodeInfo> {
        match self {
            IdentityProvider::Tailscale(t) => t.me().await,
            IdentityProvider::Static(s) => Ok(s.me.clone()),
        }
    }

    pub async fn peers(&self) -> Result<Vec<PeerInfo>> {
        match self {
            IdentityProvider::Tailscale(t) => t.peers().await,
            IdentityProvider::Static(s) => Ok(s.peers.clone()),
        }
    }

    pub async fn whois(&self, ip: IpAddr) -> Result<Option<WhoIs>> {
        match self {
            IdentityProvider::Tailscale(t) => t.whois(ip).await,
            IdentityProvider::Static(s) => Ok(s.whois(ip)),
        }
    }

    /// Login name of this node's own user, for the default allow-list.
    pub async fn own_login(&self) -> Result<Option<String>> {
        match self {
            IdentityProvider::Tailscale(t) => {
                let me = t.me().await?;
                for ip in me.ips {
                    if let Some(w) = t.whois(ip).await? {
                        return Ok(Some(w.login));
                    }
                }
                Ok(None)
            }
            IdentityProvider::Static(s) => Ok(Some(s.login.clone())),
        }
    }
}

// ---------------------------------------------------------------------------

pub struct Tailscale {
    bin: PathBuf,
    status_cache: Mutex<Option<(Instant, Value)>>,
    whois_cache: Mutex<HashMap<IpAddr, (Instant, Option<WhoIs>)>>,
}

const STATUS_TTL: Duration = Duration::from_secs(5);
const WHOIS_TTL: Duration = Duration::from_secs(120);

impl Tailscale {
    pub fn new(bin: Option<PathBuf>) -> Result<Tailscale> {
        let bin = match bin {
            Some(b) => b,
            None => find_tailscale()
                .ok_or_else(|| anyhow!("tailscale CLI not found; set tailscale_bin in config"))?,
        };
        Ok(Tailscale {
            bin,
            status_cache: Mutex::new(None),
            whois_cache: Mutex::new(HashMap::new()),
        })
    }

    async fn run(&self, args: &[&str]) -> Result<Value> {
        let out = Command::new(&self.bin)
            .args(args)
            .output()
            .await
            .with_context(|| format!("running {}", self.bin.display()))?;
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

    async fn status(&self) -> Result<Value> {
        if let Some((at, v)) = self.status_cache.lock().unwrap().as_ref() {
            if at.elapsed() < STATUS_TTL {
                return Ok(v.clone());
            }
        }
        let v = self.run(&["status", "--json"]).await?;
        *self.status_cache.lock().unwrap() = Some((Instant::now(), v.clone()));
        Ok(v)
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
                })
            }
            Err(e) => {
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

    #[test]
    fn short_names() {
        assert_eq!(
            short_name("jonathan-air.angler-centauri.ts.net."),
            "jonathan-air"
        );
        assert_eq!(short_name("plain"), "plain");
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
            status_cache: Mutex::new(Some((Instant::now(), v))),
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
}
