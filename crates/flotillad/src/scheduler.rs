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
    // Once shutdown has started, never claim or resume anything: a job we
    // just killed must stay unclaimed-by-us until the restarted daemon (or a
    // peer) picks it up.
    if state
        .shutting_down
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return Ok(());
    }
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
        // Placement hint: with `least-load`, only the least loaded eligible
        // node (by the facts everyone replicates) claims. Ties break on node
        // id, and LWW on the claim still resolves any disagreement.
        if spec.node.is_none()
            && spec.pick.as_deref() == Some("least-load")
            && !least_loaded(state, &spec, &facts)
        {
            continue;
        }
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
            Outcome::Finished(_)
                if state
                    .shutting_down
                    .load(std::sync::atomic::Ordering::SeqCst) =>
            {
                tracing::info!(job = %id, "killed by shutdown; claim left for resume or takeover");
            }
            Outcome::Finished(_) if state.store.get(&keys::job(&id)).ok().flatten().is_none() => {
                tracing::info!(job = %id, "job was removed while running; not writing a result");
            }
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
    let log_path = state.cfg.job_log_path(&spec.id);
    let req = match &spec.tmux {
        Some(session) => match tmux_request(state, spec, session, &log_path).await {
            Ok(r) => r,
            Err(e) => {
                return Outcome::Finished(JobResult {
                    job_id: spec.id.clone(),
                    node: state.me.node_id.clone(),
                    exit_code: None,
                    started_at_ms: started,
                    finished_at_ms: flotilla_core::now_ms(),
                    output_tail: String::new(),
                    error: Some(format!("tmux session: {e:#}")),
                })
            }
        },
        None => ExecRequest {
            cmd: wrap_caffeinate(state, spec.cmd.clone()),
            cwd: spec.cwd.clone(),
            env: spec.env.clone(),
            timeout_secs: spec.timeout_secs,
        },
    };
    let mut rx = exec::spawn(req, cancel.clone());
    let mut tail = Tail::new(TAIL_BYTES);
    // In tmux mode the session's shell writes the log itself.
    let mut log = if spec.tmux.is_some() {
        None
    } else {
        tokio::fs::File::create(&log_path).await.ok()
    };
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
            ExecFrame::Keepalive => {}
        }
    }
    keeper.abort();
    if let Some(f) = log.as_mut() {
        let _ = f.flush().await;
    }
    let lost_to = lost.lock().unwrap().take();
    if let Some(to) = lost_to {
        if let Some(session) = &spec.tmux {
            tmux_kill(session).await;
        }
        return Outcome::LostOwnership(to);
    }
    if let Some(session) = &spec.tmux {
        // The waiter exited: either the session finished (exit file present)
        // or we were cancelled / timed out and must tear the session down.
        let exit_path = tmux_exit_path(state, &spec.id);
        match tokio::fs::read_to_string(&exit_path).await {
            Ok(code) => exit = code.trim().parse().ok(),
            Err(_) => {
                tmux_kill(session).await;
                exit = None;
                if error.is_none() {
                    error = Some("session ended without an exit status".into());
                }
            }
        }
        let _ = tokio::fs::remove_file(&exit_path).await;
        let mut t = Tail::new(TAIL_BYTES);
        if let Ok(text) = tokio::fs::read_to_string(&log_path).await {
            t.push(&text);
        }
        tail = t;
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

/// On macOS, keep the machine awake for the duration of the job. caffeinate
/// runs the command and exits with its status, so nothing else changes.
fn wrap_caffeinate(state: &AppState, cmd: Vec<String>) -> Vec<String> {
    if !state.cfg.caffeinate_jobs || !cfg!(target_os = "macos") || cmd.is_empty() {
        return cmd;
    }
    let present = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).any(|d| d.join("caffeinate").is_file()))
        .unwrap_or(false);
    if !present {
        return cmd;
    }
    let mut wrapped = vec!["caffeinate".to_string(), "-i".to_string()];
    wrapped.extend(cmd);
    wrapped
}

/// True if this node is the least loaded (load per cpu) among the nodes
/// whose facts are fresh and whose labels match the job's selector.
fn least_loaded(state: &AppState, spec: &JobSpec, mine: &flotilla_core::schema::NodeFacts) -> bool {
    let now = flotilla_core::now_ms();
    let fresh_ms = state.cfg.facts_interval_secs * 4 * 1000;
    let score = |f: &flotilla_core::schema::NodeFacts| f.load_1m / f.cpus.max(1) as f64;
    let my_score = score(mine);
    let Ok(records) = state.store.list(keys::NODE) else {
        return true;
    };
    for rec in records {
        let Ok(other) = rec.parse::<flotilla_core::schema::NodeFacts>() else {
            continue;
        };
        if other.node_id == mine.node_id || now.saturating_sub(other.reported_at_ms) > fresh_ms {
            continue;
        }
        if !spec.selector.matches(&other.labels) {
            continue;
        }
        let s = score(&other);
        if s < my_score || (s == my_score && other.node_id < mine.node_id) {
            return false;
        }
    }
    true
}

fn tmux_exit_path(state: &AppState, id: &str) -> std::path::PathBuf {
    state.cfg.jobs_dir().join(format!("{id}.exit"))
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Start the job inside a detached tmux session whose shell writes the log
/// and exit status to files and then signals a tmux wait channel. The
/// returned request is the waiter for that channel; killing it (cancel or
/// timeout) is followed by killing the session.
async fn tmux_request(
    state: &AppState,
    spec: &JobSpec,
    session: &str,
    log_path: &std::path::Path,
) -> anyhow::Result<ExecRequest> {
    let exit_path = tmux_exit_path(state, &spec.id);
    let _ = tokio::fs::remove_file(&exit_path).await;
    let chan = format!("flotilla-{}", spec.id);
    let cmd: Vec<String> = wrap_caffeinate(state, spec.cmd.clone())
        .iter()
        .map(|a| shell_quote(a))
        .collect();
    let inner = format!(
        "exec > {log} 2>&1; {cmd}; code=$?; echo $code > {exit}; tmux wait-for -S {chan}",
        log = shell_quote(&log_path.to_string_lossy()),
        cmd = cmd.join(" "),
        exit = shell_quote(&exit_path.to_string_lossy()),
        chan = chan,
    );
    let mut args: Vec<String> = vec![
        "new-session".into(),
        "-d".into(),
        "-s".into(),
        session.into(),
    ];
    if let Some(cwd) = &spec.cwd {
        args.push("-c".into());
        args.push(crate::exec::expand_home(cwd));
    }
    for (k, v) in &spec.env {
        args.push("-e".into());
        args.push(format!("{k}={v}"));
    }
    args.push("--".into());
    args.push("sh".into());
    args.push("-c".into());
    args.push(inner);
    let out = tokio::process::Command::new("tmux")
        .args(&args)
        .output()
        .await?;
    if !out.status.success() {
        anyhow::bail!(
            "tmux new-session failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(ExecRequest {
        cmd: vec!["tmux".into(), "wait-for".into(), chan],
        cwd: None,
        env: Default::default(),
        timeout_secs: spec.timeout_secs,
    })
}

async fn tmux_kill(session: &str) {
    let _ = tokio::process::Command::new("tmux")
        .args(["kill-session", "-t", session])
        .output()
        .await;
}
