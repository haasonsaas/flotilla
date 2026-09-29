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
    /// Every tailnet this node is on, with its node id and addresses there.
    /// Empty on nodes older than multi-tailnet support.
    #[serde(default)]
    pub tailnets: Vec<TailnetInfo>,
    /// Build caches this node holds warm, by name (`[[warm_cache]]` in the
    /// node's config). Absent when the cache directory does not exist.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub warm: BTreeMap<String, WarmCache>,
    /// Root plus every other mount of 50 GB or more, for the watchdog.
    #[serde(default)]
    pub disks: Vec<DiskInfo>,
    /// `load_1m / cpus`.
    #[serde(default)]
    pub load_per_core: f64,
    /// Running `cargo` and `rustc` processes.
    #[serde(default)]
    pub build_procs: u32,
    /// Login state of each configured tailnet's tailscaled.
    #[serde(default)]
    pub tailscale: Vec<TailnetHealth>,
}

/// A mounted volume as reported by a node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DiskInfo {
    pub mount: String,
    pub total_gb: u64,
    pub free_gb: u64,
}

impl DiskInfo {
    pub fn free_pct(&self) -> f64 {
        if self.total_gb == 0 {
            return 100.0;
        }
        self.free_gb as f64 * 100.0 / self.total_gb as f64
    }
}

/// State of one tailnet's tailscaled: the `BackendState` from
/// `tailscale status --json` (`Running`, `NeedsLogin`, `Stopped`, ...), or
/// `unreachable` when the daemon did not answer.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TailnetHealth {
    pub name: String,
    pub state: String,
}

/// `alert/<node id>/<alert id>`: written by a node about itself when a
/// `[alerts]` threshold trips, and rewritten with `firing = false` when it
/// clears.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Alert {
    pub node_id: String,
    pub node: String,
    /// `disk_free`, `load`, or `tailscale`.
    pub kind: String,
    /// What it is about: a mount point or a tailnet name.
    pub subject: String,
    pub message: String,
    pub firing: bool,
    pub since_ms: u64,
    pub updated_ms: u64,
    /// When the node last sent a notification for this alert.
    #[serde(default)]
    pub notified_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_ms: Option<u64>,
}

/// One warm build cache on a node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WarmCache {
    /// What the cache was built from, e.g. a short commit hash. Empty when
    /// the node has no `key_cmd` for it.
    pub key: String,
    pub path: String,
    /// Approximate size on disk, refreshed every few minutes.
    pub size_mb: u64,
    /// Unix ms of the newest change under the cache directory.
    pub last_used_ms: u64,
    /// Seconds between `last_used_ms` and when these facts were written.
    pub age_secs: u64,
}

impl WarmCache {
    /// The `warm.<name>` label value: `<key>@<age>`, e.g. `3fa9c1e@12m`.
    pub fn label_value(&self) -> String {
        let key = if self.key.is_empty() { "-" } else { &self.key };
        format!("{key}@{}", human_age(self.age_secs))
    }
}

pub fn human_age(secs: u64) -> String {
    match secs {
        0..=59 => format!("{secs}s"),
        60..=3599 => format!("{}m", secs / 60),
        3600..=86399 => format!("{}h", secs / 3600),
        _ => format!("{}d", secs / 86400),
    }
}

/// One node's presence on one tailnet. `node_id` is the id the tailnet's
/// peer list uses for it, which differs per tailnet for the same machine.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TailnetInfo {
    pub name: String,
    pub node_id: String,
    pub ips: Vec<String>,
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
    /// Jobs that must have succeeded before this one is eligible. If any of
    /// them ends without success, this job is cancelled with a reason.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    /// Batch label for grouping (`flotilla batch`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<String>,
    /// Automatic resubmissions after a failed result (not cancels).
    #[serde(default)]
    pub retries: u32,
    /// How many retries have been used. Bumped when the failed result is
    /// cleared for another attempt.
    #[serde(default)]
    pub retry: u32,
    /// Why the job was cancelled, when the fleet did it (dependency failed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
    /// Affinity placement: among eligible nodes prefer the one holding this
    /// warm cache (`warm` in its facts), freshest first, then least loaded.
    /// Nodes at their `max_jobs` cap are passed over. Falls back to plain
    /// least-load when no eligible node holds the cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefer_warm: Option<String>,
    /// With `prefer_warm`: the cache key the job wants (e.g. a commit). A
    /// node whose key equals it, or is a prefix of it or extends it, wins
    /// over a merely fresher cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warm_key: Option<String>,
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
            after: Vec::new(),
            batch: None,
            retries: 0,
            retry: 0,
            cancel_reason: None,
            prefer_warm: None,
            warm_key: None,
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

    #[test]
    fn warm_label_value_formats_key_and_age() {
        let mut w = WarmCache {
            key: "3fa9c1e".into(),
            age_secs: 720,
            ..Default::default()
        };
        assert_eq!(w.label_value(), "3fa9c1e@12m");
        w.key.clear();
        w.age_secs = 5;
        assert_eq!(w.label_value(), "-@5s");
        assert_eq!(human_age(7200), "2h");
        assert_eq!(human_age(3 * 86400), "3d");
    }

    #[test]
    fn facts_without_warm_deserialize() {
        let f = NodeFacts::default();
        let mut v = serde_json::to_value(&f).unwrap();
        assert!(v.get("warm").is_none(), "empty warm is not serialized");
        v.as_object_mut().unwrap().remove("warm");
        let back: NodeFacts = serde_json::from_value(v).unwrap();
        assert!(back.warm.is_empty());
    }
}
