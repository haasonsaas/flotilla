//! Leaderless job scheduling. Every node runs this loop; claims are LWW
//! records, so after one settle window all nodes agree on the executor.

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

async fn tick(state: &AppState) -> anyhow::Result<()> {
    let Some(facts) = state.my_facts() else {
        return Ok(());
    };
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
        match state
            .store
            .get(&keys::claim(&spec.id))?
            .map(|r| r.parse::<JobClaim>())
            .transpose()?
        {
            Some(claim) if claim.node == state.me.node_id => {
                // Ours (e.g. after a restart) but not running: run it now.
                start(state.clone(), spec, false);
            }
            Some(_) => {}
            None => {
                if state.running.lock().unwrap().len() >= state.cfg.max_concurrent_jobs {
                    continue;
                }
                let claim = JobClaim {
                    job_id: spec.id.clone(),
                    node: state.me.node_id.clone(),
                    claimed_at_ms: flotilla_core::now_ms(),
                };
                state.store.put_json(&keys::claim(&spec.id), &claim)?;
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
            let winner = state
                .store
                .get(&keys::claim(&id))
                .ok()
                .flatten()
                .and_then(|r| r.parse::<JobClaim>().ok());
            match winner {
                Some(c) if c.node == state.me.node_id => {}
                other => {
                    tracing::info!(job = %id, winner = ?other.map(|c| c.node), "lost claim");
                    state.running.lock().unwrap().remove(&id);
                    return;
                }
            }
        }
        // A cancel may have landed during settling.
        let cancelled_now = state
            .store
            .get(&keys::job(&id))
            .ok()
            .flatten()
            .and_then(|r| r.parse::<JobSpec>().ok())
            .map(|s| s.cancelled)
            .unwrap_or(true);
        if cancelled_now {
            state.running.lock().unwrap().remove(&id);
            return;
        }
        tracing::info!(job = %id, cmd = ?spec.cmd, "running");
        let result = execute(&state, &spec, cancel).await;
        if let Err(e) = state.store.put_json(&keys::result(&id), &result) {
            tracing::error!(job = %id, error = %e, "writing result");
        }
        tracing::info!(job = %id, exit = ?result.exit_code, "finished");
        state.running.lock().unwrap().remove(&id);
    });
}

async fn execute(state: &AppState, spec: &JobSpec, cancel: CancellationToken) -> JobResult {
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

    // Watch for cancellation written by any node.
    let watcher = {
        let state = state.clone();
        let id = spec.id.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            loop {
                pause(Duration::from_secs(1)).await;
                let cancelled = state
                    .store
                    .get(&keys::job(&id))
                    .ok()
                    .flatten()
                    .and_then(|r| r.parse::<JobSpec>().ok())
                    .map(|s| s.cancelled)
                    .unwrap_or(false);
                if cancelled {
                    cancel.cancel();
                    return;
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
    watcher.abort();
    if let Some(f) = log.as_mut() {
        let _ = f.flush().await;
    }
    JobResult {
        job_id: spec.id.clone(),
        node: state.me.node_id.clone(),
        exit_code: exit,
        started_at_ms: started,
        finished_at_ms: flotilla_core::now_ms(),
        output_tail: tail.into_string(),
        error,
    }
}
