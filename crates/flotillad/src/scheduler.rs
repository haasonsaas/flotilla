//! Leaderless job scheduling. Every node runs this loop; claims are LWW
//! records, so after one settle window all nodes agree on the executor.
//!
//! Claims carry a lease. The executor renews it while the job runs; if the
//! lease lapses (plus a grace window) with no result, any eligible node
//! takes the job over with a new claim. An executor that observes someone
//! else's claim on its job kills the process and writes no result, so
//! ownership converges to one node even after partitions. Jobs are
//! at-least-once.

use crate::exec::{self, Tail};
use crate::server::{pause, AppState};
use flotilla_core::api::{ExecFrame, ExecRequest};
use flotilla_core::keys;
use flotilla_core::schema::{JobClaim, JobResult, JobSpec};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

const TAIL_BYTES: usize = 4096;

pub async fn run(state: AppState) {
    let interval = Duration::from_secs(state.cfg.scheduler_interval_secs);
    loop {
        if let Err(e) = tick(&state).await {
            tracing::error!(error = %e, "scheduler tick failed");
        }
        pause(interval).await;
    }
}

fn current_claim(state: &AppState, id: &str) -> Option<JobClaim> {
    state
        .store
        .get(&keys::claim(id))
        .ok()
        .flatten()
        .and_then(|r| r.parse().ok())
}

fn job_cancelled(state: &AppState, id: &str) -> bool {
    state
        .store
        .get(&keys::job(id))
        .ok()
        .flatten()
        .and_then(|r| r.parse::<JobSpec>().ok())
        .map(|s| s.cancelled)
        .unwrap_or(true)
}

fn write_claim(state: &AppState, id: &str, attempt: u32) -> anyhow::Result<JobClaim> {
    let now = flotilla_core::now_ms();
    let claim = JobClaim {
        job_id: id.to_string(),
        node: state.me.node_id.clone(),
        claimed_at_ms: now,
        lease_until_ms: now + state.cfg.lease().as_millis() as u64,
        attempt,
    };
    state.store.put_json(&keys::claim(id), &claim)?;
    Ok(claim)
}

async fn tick(state: &AppState) -> anyhow::Result<()> {
    let Some(facts) = state.my_facts() else {
        return Ok(());
    };
    let now = flotilla_core::now_ms();
    let grace = state.cfg.lease_grace().as_millis() as u64;
    for rec in state.store.list(keys::JOB)? {
        let spec: JobSpec = match rec.parse() {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(key = %rec.key, error = %e, "unparseable job");
                continue;
            }
        };
        if spec.cancelled || state.store.get(&keys::result(&spec.id))?.is_some() {
            continue;
        }
        if state.running.lock().unwrap().contains_key(&spec.id) {
            continue;
        }
        let eligible = match &spec.node {
            Some(n) => n == &state.me.node_id || n == &state.me.name,
            None => spec.selector.matches(&facts.labels),
        };
        if !eligible {
            continue;
        }
        let has_capacity = state.running.lock().unwrap().len() < state.cfg.max_concurrent_jobs;
        match current_claim(state, &spec.id) {
            Some(claim) if claim.node == state.me.node_id => {
                // Ours (e.g. after a restart) but not running: resume it.
                tracing::info!(job = %spec.id, "resuming own claim");
                write_claim(state, &spec.id, claim.attempt)?;
                start(state.clone(), spec, false);
            }
            Some(claim) => {
                if has_capacity && now > claim.lease_until_ms.saturating_add(grace) {
                    tracing::warn!(job = %spec.id, holder = %claim.node, attempt = claim.attempt + 1, "lease expired, taking over");
                    write_claim(state, &spec.id, claim.attempt + 1)?;
                    start(state.clone(), spec, true);
                }
            }
            None => {
                if !has_capacity {
                    continue;
                }
                write_claim(state, &spec.id, 1)?;
                tracing::info!(job = %spec.id, "claimed, settling");
                start(state.clone(), spec, true);
            }
        }
    }
    Ok(())
}

fn start(state: AppState, spec: JobSpec, settle: bool) {
    let cancel = CancellationToken::new();
    state
        .running
        .lock()
        .unwrap()
        .insert(spec.id.clone(), cancel.clone());
    tokio::spawn(async move {
        let id = spec.id.clone();
        if settle {
            pause(state.cfg.settle_window()).await;
            match current_claim(&state, &id) {
                Some(c) if c.node == state.me.node_id => {}
                other => {
                    tracing::info!(job = %id, winner = ?other.map(|c| c.node), "lost claim");
                    state.running.lock().unwrap().remove(&id);
                    return;
                }
            }
        }
        if job_cancelled(&state, &id) {
            state.running.lock().unwrap().remove(&id);
            return;
        }
        tracing::info!(job = %id, cmd = ?spec.cmd, "running");
        match execute(&state, &spec, cancel).await {
            Outcome::Finished(result) => {
                if let Err(e) = state.store.put_json(&keys::result(&id), &result) {
                    tracing::error!(job = %id, error = %e, "writing result");
                }
                tracing::info!(job = %id, exit = ?result.exit_code, "finished");
            }
            Outcome::LostOwnership(to) => {
                tracing::warn!(job = %id, to = %to, "stopped: another node holds the claim");
            }
        }
        state.running.lock().unwrap().remove(&id);
    });
}

enum Outcome {
    Finished(JobResult),
    LostOwnership(String),
}

async fn execute(state: &AppState, spec: &JobSpec, cancel: CancellationToken) -> Outcome {
    let started = flotilla_core::now_ms();
    let req = ExecRequest {
        cmd: spec.cmd.clone(),
        cwd: spec.cwd.clone(),
        env: spec.env.clone(),
        timeout_secs: spec.timeout_secs,
    };
    let mut rx = exec::spawn(req, cancel.clone());
    let mut tail = Tail::new(TAIL_BYTES);
    let mut log = tokio::fs::File::create(state.cfg.job_log_path(&spec.id))
        .await
        .ok();
    let mut exit = None;
    let mut error = None;

    // Lease keeper: renew our claim while running, kill the job if a cancel
    // lands or if another node now holds the claim.
    let lost = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
    let keeper = {
        let state = state.clone();
        let id = spec.id.clone();
        let cancel = cancel.clone();
        let lost = lost.clone();
        let renew_every = state.cfg.lease() / 3;
        tokio::spawn(async move {
            let mut since_renew = Duration::ZERO;
            let step = Duration::from_secs(1);
            loop {
                pause(step).await;
                since_renew += step;
                if job_cancelled(&state, &id) {
                    cancel.cancel();
                    return;
                }
                match current_claim(&state, &id) {
                    Some(c) if c.node == state.me.node_id => {
                        if since_renew >= renew_every {
                            since_renew = Duration::ZERO;
                            if let Err(e) = write_claim(&state, &id, c.attempt) {
                                tracing::warn!(job = %id, error = %e, "lease renewal failed");
                            }
                        }
                    }
                    other => {
                        *lost.lock().unwrap() =
                            Some(other.map(|c| c.node).unwrap_or_else(|| "nobody".into()));
                        cancel.cancel();
                        return;
                    }
                }
            }
        })
    };

    while let Some(frame) = rx.recv().await {
        match &frame {
            ExecFrame::Stdout { data } | ExecFrame::Stderr { data } => {
                tail.push(data);
                if let Some(f) = log.as_mut() {
                    let _ = f.write_all(data.as_bytes()).await;
                }
            }
            ExecFrame::Error { message } => error = Some(message.clone()),
            ExecFrame::Exit { code } => exit = *code,
        }
    }
    keeper.abort();
    if let Some(f) = log.as_mut() {
        let _ = f.flush().await;
    }
    if let Some(to) = lost.lock().unwrap().take() {
        return Outcome::LostOwnership(to);
    }
    Outcome::Finished(JobResult {
        job_id: spec.id.clone(),
        node: state.me.node_id.clone(),
        exit_code: exit,
        started_at_ms: started,
        finished_at_ms: flotilla_core::now_ms(),
        output_tail: tail.into_string(),
        error,
    })
}
