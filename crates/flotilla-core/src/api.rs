//! Wire types for the daemon's HTTP API, shared by daemon and CLI.

use crate::record::Record;
use crate::schema::NodeFacts;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DEFAULT_PORT: u16 = 7400;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SelfInfo {
    pub node_id: String,
    pub name: String,
    pub version: String,
    pub tailscale_ips: Vec<String>,
    pub port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: String,
    pub name: String,
    pub online: bool,
    pub ips: Vec<String>,
    pub os: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeersResponse {
    pub me: SelfInfo,
    pub peers: Vec<PeerInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordsResponse {
    pub records: Vec<Record>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutRecordRequest {
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecRequest {
    pub cmd: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// One newline-delimited JSON frame of an exec stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ExecFrame {
    Stdout { data: String },
    Stderr { data: String },
    Exit { code: Option<i32> },
    Error { message: String },
}

/// One node's view of the fleet, as returned by `/v1/status`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub facts: NodeFacts,
    /// Tailscale says the node is online right now.
    pub online: bool,
    /// Seconds since the facts record was written.
    pub facts_age_secs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub me: String,
    pub nodes: Vec<NodeStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

/// Turn a peer address as advertised (`100.1.2.3`, `100.1.2.3:7401`,
/// `fd7a::1`) into an HTTP base URL.
pub fn base_url(addr: &str, default_port: u16) -> Option<String> {
    if let Ok(sa) = addr.parse::<std::net::SocketAddr>() {
        return Some(format!("http://{sa}"));
    }
    match addr.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(ip)) => Some(format!("http://{ip}:{default_port}")),
        Ok(std::net::IpAddr::V6(ip)) => Some(format!("http://[{ip}]:{default_port}")),
        Err(_) => None,
    }
}

/// Pick the best advertised address for a peer: IPv4 first.
pub fn peer_url(ips: &[String], default_port: u16) -> Option<String> {
    ips.iter()
        .filter(|s| !s.contains("::"))
        .chain(ips.iter().filter(|s| s.contains("::")))
        .find_map(|s| base_url(s, default_port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        assert_eq!(
            base_url("100.1.2.3", 7400).unwrap(),
            "http://100.1.2.3:7400"
        );
        assert_eq!(base_url("100.1.2.3:9", 7400).unwrap(), "http://100.1.2.3:9");
        assert_eq!(base_url("fd7a::1", 7400).unwrap(), "http://[fd7a::1]:7400");
        assert!(base_url("nope", 7400).is_none());
        let ips = vec!["fd7a::1".to_string(), "100.1.2.3".to_string()];
        assert_eq!(peer_url(&ips, 1).unwrap(), "http://100.1.2.3:1");
    }
}

/// The local daemon's view of its sync relationship with one peer.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PeerSyncState {
    /// Node id for discovered peers, the seed address for seeds.
    pub key: String,
    pub name: String,
    pub url: String,
    pub last_ok_ms: Option<u64>,
    pub last_error: Option<String>,
    pub last_error_ms: Option<u64>,
    pub consecutive_failures: u32,
    /// Unix ms before which the peer will not be retried.
    pub next_try_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncStateResponse {
    pub peers: Vec<PeerSyncState>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SyncNowRequest {
    /// Node name, node id, or seed address. None = every candidate.
    #[serde(default)]
    pub peer: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncNowResult {
    pub name: String,
    pub url: String,
    pub ok: bool,
    pub pulled: usize,
    pub pushed: usize,
    pub error: Option<String>,
}

/// Result of `PUT /v1/files`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileWriteResponse {
    /// Absolute path as written on the node.
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

/// Query for `/v1/files`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FileQuery {
    /// Absolute, or `~/...` relative to the daemon user's home.
    pub path: String,
    /// Octal mode for writes, e.g. "0755". Default 0644.
    #[serde(default)]
    pub mode: Option<String>,
}

/// One change to the replicated store, as streamed by `GET /v1/events`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordEvent {
    pub key: String,
    pub author: String,
    pub hlc: crate::hlc::Hlc,
    pub deleted: bool,
    pub value: serde_json::Value,
    /// Unix ms when this node observed the change.
    pub at_ms: u64,
}

impl From<&Record> for RecordEvent {
    fn from(r: &Record) -> Self {
        RecordEvent {
            key: r.key.clone(),
            author: r.author.clone(),
            hlc: r.hlc,
            deleted: r.deleted,
            value: r.value.clone(),
            at_ms: crate::now_ms(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WakeRequest {
    /// MAC address in any common notation.
    pub mac: String,
    /// Extra unicast/broadcast IPv4 targets besides the local broadcasts.
    #[serde(default)]
    pub targets: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WakeResponse {
    /// Destinations the magic packet was sent to.
    pub sent_to: Vec<String>,
}

/// Parse `aa:bb:cc:dd:ee:ff`, `aa-bb-...`, or `aabbccddeeff` into bytes.
pub fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() != 12 {
        return None;
    }
    let mut out = [0u8; 6];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// A wake-on-LAN magic packet: 6 x 0xFF then the MAC 16 times.
pub fn magic_packet(mac: [u8; 6]) -> Vec<u8> {
    let mut p = vec![0xFFu8; 6];
    for _ in 0..16 {
        p.extend_from_slice(&mac);
    }
    p
}

#[cfg(test)]
mod wake_tests {
    use super::*;

    #[test]
    fn mac_and_packet() {
        let m = parse_mac("A4:83:E7:12:34:56").unwrap();
        assert_eq!(m, [0xa4, 0x83, 0xe7, 0x12, 0x34, 0x56]);
        assert_eq!(parse_mac("a4-83-e7-12-34-56"), Some(m));
        assert_eq!(parse_mac("a483e7123456"), Some(m));
        assert!(parse_mac("nope").is_none());
        let p = magic_packet(m);
        assert_eq!(p.len(), 102);
        assert_eq!(&p[..6], &[0xff; 6]);
        assert_eq!(&p[6..12], &m);
        assert_eq!(&p[96..], &m);
    }
}
