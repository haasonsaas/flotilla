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
