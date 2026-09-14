//! Desired-state convergence for this node.

use crate::exec::expand_home;
use crate::server::{pause, AppState};
use flotilla_core::keys;
use flotilla_core::schema::{DesiredState, ReconcileReport};
use std::time::Duration;

pub async fn run(state: AppState) {
    let interval = Duration::from_secs(state.cfg.reconcile_interval_secs);
    loop {
        if let Err(e) = pass(&state).await {
            tracing::error!(error = %e, "reconcile failed");
        }
        pause(interval).await;
    }
}

async fn pass(state: &AppState) -> anyhow::Result<()> {
    let Some(rec) = state.store.get(&keys::desired(&state.me.node_id))?.or(state.store.get(&keys::desired(&state.me.name))?) else {
        return Ok(());
    };
    let desired: DesiredState = rec.parse()?;
    let mut report = ReconcileReport { node: state.me.node_id.clone(), at_ms: flotilla_core::now_ms(), desired_hlc: Some(rec.hlc), converged: true, changes: vec![], errors: vec![] };

    for f in &desired.files {
        let path = expand_home(&f.path);
        let current = tokio::fs::read_to_string(&path).await.ok();
        if current.as_deref() != Some(f.content.as_str()) {
            if let Some(parent) = std::path::Path::new(&path).parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            match tokio::fs::write(&path, &f.content).await {
                Ok(()) => report.changes.push(format!("wrote {path}")),
                Err(e) => report.errors.push(format!("write {path}: {e}")),
            }
        }
        #[cfg(unix)]
        if let Some(mode) = &f.mode {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(bits) = u32::from_str_radix(mode.trim_start_matches("0o"), 8) {
                if let Ok(meta) = std::fs::metadata(&path) {
                    if meta.permissions().mode() & 0o7777 != bits {
                        if let Err(e) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(bits)) {
                            report.errors.push(format!("chmod {path}: {e}"));
                        } else {
                            report.changes.push(format!("chmod {mode} {path}"));
                        }
                    }
                }
            }
        }
    }

    for e in &desired.ensure {
        if e.check.is_empty() {
            report.errors.push(format!("{}: empty check", e.name));
            continue;
        }
        let ok = run_quiet(&e.check).await;
        if ok {
            continue;
        }
        if e.apply.is_empty() {
            report.errors.push(format!("{}: check failed and no apply", e.name));
            continue;
        }
        if run_quiet(&e.apply).await && run_quiet(&e.check).await {
            report.changes.push(format!("applied {}", e.name));
        } else {
            report.errors.push(format!("{}: apply did not satisfy check", e.name));
        }
    }

    report.converged = report.errors.is_empty();
    if !report.changes.is_empty() || !report.errors.is_empty() {
        tracing::info!(changes = ?report.changes, errors = ?report.errors, "reconciled");
    }
    state.store.put_json(&keys::reconcile(&state.me.node_id), &report)?;
    Ok(())
}

async fn run_quiet(cmd: &[String]) -> bool {
    tokio::process::Command::new(&cmd[0])
        .args(&cmd[1..])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}
