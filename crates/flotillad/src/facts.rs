//! Collect this node's facts and publish them into the store.

use crate::server::{pause, AppState};
use flotilla_core::keys;
use flotilla_core::schema::NodeFacts;
use flotilla_core::selector::Labels;
use std::time::Duration;
use sysinfo::{Disks, MemoryRefreshKind, RefreshKind, System};

pub async fn run(state: AppState) {
    let interval = Duration::from_secs(state.cfg.facts_interval_secs);
    loop {
        match tokio::task::spawn_blocking({
            let state = state.clone();
            move || collect(&state)
        })
        .await
        {
            Ok(facts) => {
                if let Err(e) = state.store.put_json(&keys::node_facts(&state.me.node_id), &facts) {
                    tracing::error!(error = %e, "writing facts");
                }
            }
            Err(e) => tracing::error!(error = %e, "facts collector panicked"),
        }
        pause(interval).await;
    }
}

pub fn collect(state: &AppState) -> NodeFacts {
    let sys = System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()));
    let disks = Disks::new_with_refreshed_list();
    let root = disks.iter().find(|d| d.mount_point() == std::path::Path::new("/"));
    let (bat, ac) = battery();
    let mut labels = Labels::new();
    labels.insert("os".into(), std::env::consts::OS.into());
    labels.insert("arch".into(), std::env::consts::ARCH.into());
    labels.insert("node".into(), state.me.name.clone());
    for (k, v) in &state.cfg.labels {
        labels.insert(k.clone(), v.clone());
    }
    NodeFacts {
        node_id: state.me.node_id.clone(),
        name: state.me.name.clone(),
        hostname: System::host_name().unwrap_or_else(|| state.me.hostname.clone()),
        os: match (System::name(), System::os_version()) {
            (Some(n), Some(v)) => format!("{n} {v}"),
            (Some(n), None) => n,
            _ => state.me.os.clone(),
        },
        arch: std::env::consts::ARCH.into(),
        version: crate::VERSION.into(),
        cpus: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        load_1m: System::load_average().one,
        mem_total_mb: sys.total_memory() / (1024 * 1024),
        mem_free_mb: sys.available_memory() / (1024 * 1024),
        disk_total_gb: root.map(|d| d.total_space() / 1_000_000_000).unwrap_or(0),
        disk_free_gb: root.map(|d| d.available_space() / 1_000_000_000).unwrap_or(0),
        uptime_secs: System::uptime(),
        battery_pct: bat,
        on_ac: ac,
        labels,
        tailscale_ips: state.me.ips.iter().map(ToString::to_string).collect(),
        port: state.cfg.port,
        reported_at_ms: flotilla_core::now_ms(),
        running_jobs: state.running_jobs(),
    }
}

#[cfg(target_os = "macos")]
fn battery() -> (Option<u8>, Option<bool>) {
    let out = match std::process::Command::new("pmset").args(["-g", "batt"]).output() {
        Ok(o) => String::from_utf8_lossy(&o.stdout).into_owned(),
        Err(_) => return (None, None),
    };
    parse_pmset(&out)
}

#[cfg(target_os = "macos")]
fn parse_pmset(out: &str) -> (Option<u8>, Option<bool>) {
    let ac = if out.contains("AC Power") {
        Some(true)
    } else if out.contains("Battery Power") {
        Some(false)
    } else {
        None
    };
    let pct = out.lines().find_map(|l| {
        let (before, _) = l.split_once('%')?;
        before.rsplit(|c: char| !c.is_ascii_digit()).next()?.parse().ok()
    });
    (pct, ac)
}

#[cfg(target_os = "linux")]
fn battery() -> (Option<u8>, Option<bool>) {
    let mut pct = None;
    let mut ac = None;
    if let Ok(rd) = std::fs::read_dir("/sys/class/power_supply") {
        for e in rd.flatten() {
            let p = e.path();
            let ty = std::fs::read_to_string(p.join("type")).unwrap_or_default();
            if ty.trim() == "Battery" {
                pct = std::fs::read_to_string(p.join("capacity")).ok().and_then(|s| s.trim().parse().ok());
            } else if ty.trim() == "Mains" {
                ac = std::fs::read_to_string(p.join("online")).ok().map(|s| s.trim() == "1");
            }
        }
    }
    (pct, ac)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn battery() -> (Option<u8>, Option<bool>) {
    (None, None)
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn parses_pmset_output() {
        let s = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=123)\t87%; charging; 0:42 remaining present: true\n";
        assert_eq!(parse_pmset(s), (Some(87), Some(true)));
        let s = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1)\t100%; discharging; (no estimate) present: true\n";
        assert_eq!(parse_pmset(s), (Some(100), Some(false)));
        assert_eq!(parse_pmset("Now drawing from 'AC Power'\n"), (None, Some(true)));
    }
}
