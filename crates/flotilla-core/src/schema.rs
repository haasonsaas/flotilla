//! Record schemas for each layer. These are what lives inside `Record.value`.

use crate::selector::{Labels, Selector};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `node/<id>/facts`: written by each node about itself.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeFacts {
    pub node_id: String,
    pub name: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub version: String,
    pub cpus: usize,
    pub load_1m: f64,
    pub mem_total_mb: u64,
    pub mem_free_mb: u64,
    pub disk_total_gb: u64,
    pub disk_free_gb: u64,
    pub uptime_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub battery_pct: Option<u8>,
    #[serde(default)]
    pub on_ac: Option<bool>,
    pub labels: Labels,
    pub tailscale_ips: Vec<String>,
    pub port: u16,
    /// Unix ms when this record was written by its node.
    pub reported_at_ms: u64,
    #[serde(default)]
    pub running_jobs: Vec<String>,
    /// Where flotillad is running from, so `flotilla upgrade` knows what to replace.
    #[serde(default)]
    pub exe_path: String,
    /// tmux sessions on this node, for the sessions layer.
    #[serde(default)]
    pub sessions: Vec<SessionInfo>,
    /// Wired/wireless interfaces with a private IPv4, for wake-on-LAN.
    #[serde(default)]
    pub lan: Vec<LanInterface>,
}

/// A LAN interface as reported by a node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LanInterface {
    pub name: String,
    pub ip: String,
    pub prefix: u8,
    /// Lowercase colon-separated MAC, e.g. `a4:83:e7:12:34:56`.
    pub mac: String,
}

/// A tmux session as seen on a node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub name: String,
    pub created_ms: u64,
    pub windows: u32,
    pub attached: bool,
    #[serde(default)]
    pub cwd: String,
    /// Command running in the active pane.
    #[serde(default)]
    pub command: String,
}

/// `job/<id>`: written by the submitter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobSpec {
    pub id: String,
    pub cmd: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub selector: Selector,
    /// Pin to one node id; overrides selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    pub submitted_by: String,
    pub submitted_at_ms: u64,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub cancelled: bool,
    /// Placement hint: `least-load` lets only the eligible node with the
    /// lowest load-per-cpu claim the job. Default: any eligible node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pick: Option<String>,
    /// Run inside a detached tmux session of this name on the executor, so
    /// it can be attached to while it runs. Output still goes to the job log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmux: Option<String>,
    /// Free-form kind for listing, e.g. `agent`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// `claim/<id>`: written by a node that intends to run the job.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobClaim {
    pub job_id: String,
    pub node: String,
    pub claimed_at_ms: u64,
    /// The executor renews this while it holds the job. Once it is in the
    /// past (plus a grace window) and there is still no result, any eligible
    /// node may take the job over. Zero means an unleased legacy claim.
    #[serde(default)]
    pub lease_until_ms: u64,
    /// How many times the job has been (re)claimed.
    #[serde(default = "one")]
    pub attempt: u32,
    /// Set by the executor when the process actually starts (after the
    /// settle window), so `claimed` and `running` are distinguishable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
}

fn one() -> u32 {
    1
}

impl JobClaim {
    pub fn lease_expired_at(&self, now_ms: u64) -> bool {
        now_ms > self.lease_until_ms
    }
}

/// `result/<id>`: written by the executor when the job finishes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: String,
    pub node: String,
    pub exit_code: Option<i32>,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    /// Last few KiB of combined output, for `job show` without a round trip.
    pub output_tail: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    /// Submitted, nobody has claimed it.
    Queued,
    /// A node claimed it and is settling or about to start.
    Claimed,
    /// The executor reported the process started.
    Running,
    Succeeded,
    Failed,
    /// Terminated on request (or cancelled before it ran).
    Cancelled,
    /// The executor's lease lapsed with no result: it went away and the
    /// outcome is unknown until a node takes the job over.
    Lost,
}

impl JobState {
    pub fn derive(
        spec: &JobSpec,
        claim: Option<&JobClaim>,
        result: Option<&JobResult>,
    ) -> JobState {
        JobState::derive_at(spec, claim, result, 0)
    }

    /// Like `derive`, but a claim whose lease lapsed before `now_ms` reads
    /// as `Lost`. `now_ms == 0` disables the lease check.
    pub fn derive_at(
        spec: &JobSpec,
        claim: Option<&JobClaim>,
        result: Option<&JobResult>,
        now_ms: u64,
    ) -> JobState {
        match (result, claim) {
            (Some(r), _) => {
                if r.error.as_deref() == Some("cancelled")
                    || (spec.cancelled && r.exit_code.is_none())
                {
                    JobState::Cancelled
                } else if r.exit_code == Some(0) {
                    JobState::Succeeded
                } else {
                    JobState::Failed
                }
            }
            (None, _) if spec.cancelled => JobState::Cancelled,
            (None, Some(c)) if now_ms > 0 && c.lease_expired_at(now_ms) => JobState::Lost,
            (None, Some(c)) if c.started_at_ms.is_some() => JobState::Running,
            (None, Some(_)) => JobState::Claimed,
            (None, None) => JobState::Queued,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Claimed => "claimed",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
            JobState::Lost => "lost",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Failed | JobState::Cancelled
        )
    }
}

/// `desired/<node>`: what an admin wants a node to look like.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DesiredState {
    #[serde(default)]
    pub files: Vec<DesiredFile>,
    #[serde(default)]
    pub ensure: Vec<Ensure>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DesiredFile {
    pub path: String,
    pub content: String,
    /// Octal mode, e.g. "0644". Default 0644.
    #[serde(default)]
    pub mode: Option<String>,
}

/// Run `apply` whenever `check` exits non-zero.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Ensure {
    pub name: String,
    pub check: Vec<String>,
    pub apply: Vec<String>,
}

/// `reconcile/<node>`: outcome of the last reconcile pass.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReconcileReport {
    pub node: String,
    pub at_ms: u64,
    pub desired_hlc: Option<crate::hlc::Hlc>,
    pub converged: bool,
    pub changes: Vec<String>,
    pub errors: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> JobSpec {
        JobSpec {
            id: "j".into(),
            cmd: vec!["true".into()],
            cwd: None,
            env: Default::default(),
            selector: Default::default(),
            node: None,
            submitted_by: "a".into(),
            submitted_at_ms: 0,
            timeout_secs: None,
            cancelled: false,
            pick: None,
            tmux: None,
            kind: None,
        }
    }

    #[test]
    fn job_state_derivation() {
        let claim = JobClaim {
            job_id: "j".into(),
            node: "n".into(),
            claimed_at_ms: 0,
            lease_until_ms: 100,
            attempt: 1,
            started_at_ms: None,
        };
        let running = JobClaim {
            started_at_ms: Some(5),
            ..claim.clone()
        };
        let ok = JobResult {
            job_id: "j".into(),
            node: "n".into(),
            exit_code: Some(0),
            started_at_ms: 0,
            finished_at_ms: 0,
            output_tail: String::new(),
            error: None,
        };
        let bad = JobResult {
            exit_code: Some(1),
            ..ok.clone()
        };
        let killed = JobResult {
            exit_code: None,
            error: Some("cancelled".into()),
            ..ok.clone()
        };
        let timed_out = JobResult {
            exit_code: None,
            error: Some("timed out".into()),
            ..ok.clone()
        };
        assert_eq!(JobState::derive(&spec(), None, None), JobState::Queued);
        assert_eq!(
            JobState::derive(&spec(), Some(&claim), None),
            JobState::Claimed
        );
        assert_eq!(
            JobState::derive(&spec(), Some(&running), None),
            JobState::Running
        );
        assert_eq!(
            JobState::derive(&spec(), Some(&claim), Some(&ok)),
            JobState::Succeeded
        );
        assert_eq!(
            JobState::derive(&spec(), Some(&claim), Some(&bad)),
            JobState::Failed
        );
        assert_eq!(
            JobState::derive(&spec(), Some(&claim), Some(&timed_out)),
            JobState::Failed
        );
        assert_eq!(
            JobState::derive(&spec(), Some(&claim), Some(&killed)),
            JobState::Cancelled,
            "a kill on request is not a failure"
        );
        let cancelled = JobSpec {
            cancelled: true,
            ..spec()
        };
        assert_eq!(
            JobState::derive(&cancelled, None, None),
            JobState::Cancelled
        );
        assert_eq!(
            JobState::derive(&cancelled, Some(&running), None),
            JobState::Cancelled
        );
        assert_eq!(
            JobState::derive(&cancelled, Some(&claim), Some(&ok)),
            JobState::Succeeded,
            "finished before the cancel landed"
        );
        assert_eq!(
            JobState::derive_at(&spec(), Some(&running), None, 50),
            JobState::Running
        );
        assert_eq!(
            JobState::derive_at(&spec(), Some(&running), None, 101),
            JobState::Lost
        );
        assert_eq!(
            JobState::derive_at(&spec(), Some(&claim), Some(&ok), 101),
            JobState::Succeeded
        );
        assert!(JobState::Cancelled.is_terminal() && !JobState::Lost.is_terminal());
    }

    #[test]
    fn legacy_claim_without_lease_deserializes() {
        let c: JobClaim =
            serde_json::from_str(r#"{"job_id":"j","node":"n","claimed_at_ms":5}"#).unwrap();
        assert_eq!(c.lease_until_ms, 0);
        assert_eq!(c.attempt, 1);
        assert!(c.lease_expired_at(1));
    }

    #[test]
    fn facts_round_trip_json() {
        let f = NodeFacts {
            node_id: "x".into(),
            name: "x".into(),
            cpus: 8,
            ..Default::default()
        };
        let back: NodeFacts = serde_json::from_value(serde_json::to_value(&f).unwrap()).unwrap();
        assert_eq!(back, f);
    }
}
