//! Collect this node's facts and publish them into the store.

use crate::server::{pause, AppState};
use flotilla_core::keys;
use flotilla_core::schema::{DiskInfo, NodeFacts, TailnetHealth};
use flotilla_core::selector::Labels;
use std::time::Duration;
use sysinfo::{
    Disks, MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System,
};

pub async fn run(state: AppState) {
    let interval = Duration::from_secs(state.cfg.facts_interval_secs);
    loop {
        let tailscale = state.identity.health().await;
        match tokio::task::spawn_blocking({
            let state = state.clone();
            move || collect(&state, tailscale)
        })
        .await
        {
            Ok(facts) => {
                if let Err(e) = state
                    .store
                    .put_json(&keys::node_facts(&state.me.node_id), &facts)
                {
                    tracing::error!(error = %e, "writing facts");
                }
                crate::alerts::check(&state, &facts).await;
            }
            Err(e) => tracing::error!(error = %e, "facts collector panicked"),
        }
        pause(interval).await;
    }
}

pub fn collect(state: &AppState, tailscale: Vec<TailnetHealth>) -> NodeFacts {
    let sys = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
    );
    let disks = Disks::new_with_refreshed_list();
    let root = disks
        .iter()
        .find(|d| d.mount_point() == std::path::Path::new("/"));
    let (bat, ac) = battery();
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    let load_1m = System::load_average().one;
    let mut labels = Labels::new();
    labels.insert("os".into(), std::env::consts::OS.into());
    labels.insert("arch".into(), std::env::consts::ARCH.into());
    labels.insert("node".into(), state.me.name.clone());
    if let Some(x) = xcode_version() {
        labels.insert("xcode".into(), x);
    }
    if which_exists("tmux") {
        labels.insert("tmux".into(), "yes".into());
    }
    let warm = crate::warm::collect(&state.cfg.warm_cache);
    for (name, w) in &warm {
        labels.insert(format!("warm.{name}"), w.label_value());
    }
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
        cpus,
        load_1m,
        load_per_core: load_1m / cpus.max(1) as f64,
        disks: select_disks(
            disks
                .iter()
                .map(|d| {
                    (
                        d.mount_point().to_string_lossy().into_owned(),
                        d.total_space(),
                        d.available_space(),
                    )
                })
                .collect(),
        ),
        build_procs: count_build_procs(),
        tailscale,
        mem_total_mb: sys.total_memory() / (1024 * 1024),
        // sysinfo reports available_memory() as 0 on macOS; fall back to total - used.
        mem_free_mb: match sys.available_memory() {
            0 => sys.total_memory().saturating_sub(sys.used_memory()) / (1024 * 1024),
            a => a / (1024 * 1024),
        },
        disk_total_gb: root.map(|d| d.total_space() / 1_000_000_000).unwrap_or(0),
        disk_free_gb: root
            .map(|d| d.available_space() / 1_000_000_000)
            .unwrap_or(0),
        uptime_secs: System::uptime(),
        battery_pct: bat,
        on_ac: ac,
        labels,
        tailscale_ips: state.me.ips.iter().map(ToString::to_string).collect(),
        port: state.cfg.port,
        reported_at_ms: flotilla_core::now_ms(),
        running_jobs: state.running_jobs(),
        sessions: tmux_sessions(),
        lan: lan_interfaces(),
        tailnets: state.me.tailnets.clone(),
        warm,
        exe_path: std::env::current_exe()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default(),
    }
}

/// Volumes worth watching: the root filesystem and every mount of at least
/// 50 GB. macOS shows one APFS container as several mounts with the same
/// totals; those collapse to the shortest mount path.
const WATCH_MIN_BYTES: u64 = 50_000_000_000;

pub fn select_disks(mut all: Vec<(String, u64, u64)>) -> Vec<DiskInfo> {
    all.retain(|(m, total, _)| m == "/" || *total >= WATCH_MIN_BYTES);
    all.sort_by(|a, b| a.0.len().cmp(&b.0.len()).then(a.0.cmp(&b.0)));
    let mut out: Vec<DiskInfo> = Vec::new();
    let mut seen: Vec<(u64, u64)> = Vec::new();
    for (mount, total, free) in all {
        let gb = (total / 1_000_000_000, free / 1_000_000_000);
        if mount != "/" && seen.contains(&gb) {
            continue;
        }
        seen.push(gb);
        if out.iter().any(|d| d.mount == mount) {
            continue;
        }
        out.push(DiskInfo {
            mount,
            total_gb: gb.0,
            free_gb: gb.1,
        });
    }
    out
}

/// Running `cargo` and `rustc` processes, a proxy for "a build is on".
fn count_build_procs() -> u32 {
    let mut sys = System::new();
    sys.refresh_processes_specifics(ProcessesToUpdate::All, true, ProcessRefreshKind::nothing());
    sys.processes()
        .values()
        .filter(|p| matches!(p.name().to_str(), Some("cargo" | "rustc")))
        .count() as u32
}

/// tmux sessions visible to the daemon's user (same default socket the
/// user's terminals use). Empty if tmux is absent or no server runs.
fn which_exists(name: &str) -> bool {
    std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).any(|d| d.join(name).is_file()))
        .unwrap_or(false)
}

/// `xcodebuild -version` is slow, so it is read once per process.
fn xcode_version() -> Option<String> {
    static CACHE: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    CACHE
        .get_or_init(|| {
            if !cfg!(target_os = "macos") || !which_exists("xcodebuild") {
                return None;
            }
            let out = std::process::Command::new("xcodebuild")
                .arg("-version")
                .output()
                .ok()?;
            let text = String::from_utf8_lossy(&out.stdout);
            text.lines()
                .next()?
                .strip_prefix("Xcode ")
                .map(|v| v.trim().to_string())
        })
        .clone()
}

fn ifconfig_mac(iface: &str) -> Option<String> {
    let out = std::process::Command::new("ifconfig")
        .arg(iface)
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let raw = text.lines().find_map(|l| {
        l.trim()
            .strip_prefix("ether ")
            .map(|m| m.split_whitespace().next().unwrap_or("").to_lowercase())
    })?;
    normalise_mac(&raw)
}

/// macOS masks MACs from background processes in `ifconfig`, but
/// `networksetup -getmacaddress` still returns the hardware address, which
/// is also the one wake-on-LAN needs.
#[cfg(target_os = "macos")]
fn networksetup_mac(iface: &str) -> Option<String> {
    let out = std::process::Command::new("networksetup")
        .args(["-getmacaddress", iface])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let raw = text
        .split("Ethernet Address:")
        .nth(1)?
        .split_whitespace()
        .next()?
        .to_lowercase();
    normalise_mac(&raw)
}

#[cfg(not(target_os = "macos"))]
fn networksetup_mac(_iface: &str) -> Option<String> {
    None
}

/// Pad octets that ifconfig prints without leading zeros ("3c:6:30:1:2:3")
/// and reject placeholders.
fn normalise_mac(raw: &str) -> Option<String> {
    let octets: Vec<String> = raw.split(':').map(|o| format!("{:0>2}", o)).collect();
    if octets.len() != 6
        || octets
            .iter()
            .any(|o| o.len() != 2 || u8::from_str_radix(o, 16).is_err())
    {
        return None;
    }
    let mac = octets.join(":");
    (mac != "02:00:00:00:00:00" && mac != "00:00:00:00:00:00").then_some(mac)
}

#[cfg(test)]
mod watchdog_tests {
    use super::*;

    #[test]
    fn selects_root_and_big_mounts_and_collapses_apfs_twins() {
        let g = 1_000_000_000u64;
        let disks = select_disks(vec![
            ("/".into(), 500 * g, 100 * g),
            ("/System/Volumes/Data".into(), 500 * g, 100 * g),
            ("/mnt/build".into(), 590 * g, 0),
            ("/boot".into(), g, g / 2),
            ("/mnt/small".into(), 49 * g, 10 * g),
        ]);
        let mounts: Vec<&str> = disks.iter().map(|d| d.mount.as_str()).collect();
        assert_eq!(mounts, ["/", "/mnt/build"]);
        assert_eq!(disks[1].free_gb, 0);
        assert_eq!(disks[1].free_pct(), 0.0);
    }

    #[test]
    fn small_root_is_still_watched() {
        let d = select_disks(vec![("/".into(), 20_000_000_000, 1_000_000_000)]);
        assert_eq!(d.len(), 1);
        assert!((d[0].free_pct() - 5.0).abs() < 0.01);
    }

    #[test]
    fn counts_only_cargo_and_rustc() {
        // the test binary is neither, but the counter must not panic and
        // returns a plain count
        let _ = count_build_procs();
    }
}

#[cfg(test)]
mod mac_tests {
    #[test]
    fn normalises_and_rejects() {
        assert_eq!(
            super::normalise_mac("3c:6:30:1:2:ab").as_deref(),
            Some("3c:06:30:01:02:ab")
        );
        assert_eq!(super::normalise_mac("02:00:00:00:00:00"), None);
        assert_eq!(super::normalise_mac("nope"), None);
    }
}

/// Physical-looking interfaces with a private IPv4 and a real MAC.
pub fn lan_interfaces() -> Vec<flotilla_core::schema::LanInterface> {
    let nets = sysinfo::Networks::new_with_refreshed_list();
    let mut out = Vec::new();
    for (name, data) in nets.iter() {
        if name.starts_with("lo")
            || name.starts_with("utun")
            || name.starts_with("tailscale")
            || name.starts_with("docker")
            || name.starts_with("br-")
            || name.starts_with("veth")
        {
            continue;
        }
        let mut mac = data.mac_address().to_string().to_lowercase();
        if mac == "00:00:00:00:00:00" || mac == "02:00:00:00:00:00" {
            // macOS reports a placeholder to unentitled processes.
            match networksetup_mac(name).or_else(|| ifconfig_mac(name)) {
                Some(real) => mac = real,
                None => continue,
            }
        }
        for net in data.ip_networks() {
            if let std::net::IpAddr::V4(v4) = net.addr {
                if v4.is_private() {
                    out.push(flotilla_core::schema::LanInterface {
                        name: name.clone(),
                        ip: v4.to_string(),
                        prefix: net.prefix,
                        mac: mac.clone(),
                    });
                }
            }
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

pub fn tmux_sessions() -> Vec<flotilla_core::schema::SessionInfo> {
    let out = match std::process::Command::new("tmux")
        .args([
            "list-sessions",
            "-F",
            "#{session_name}\t#{session_created}\t#{session_windows}\t#{session_attached}\t#{pane_current_path}\t#{pane_current_command}",
        ])
        .output()
    {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
        _ => return Vec::new(),
    };
    out.lines().filter_map(parse_tmux_line).collect()
}

fn parse_tmux_line(line: &str) -> Option<flotilla_core::schema::SessionInfo> {
    let mut f = line.split('\t');
    let name = f.next()?.to_string();
    let created_ms = f.next()?.parse::<u64>().ok()? * 1000;
    let windows = f.next()?.parse().ok()?;
    let attached = f.next()?.parse::<u32>().map(|n| n > 0).unwrap_or(false);
    let cwd = f.next().unwrap_or("").to_string();
    let command = f.next().unwrap_or("").to_string();
    Some(flotilla_core::schema::SessionInfo {
        name,
        created_ms,
        windows,
        attached,
        cwd,
        command,
    })
}

#[cfg(target_os = "macos")]
fn battery() -> (Option<u8>, Option<bool>) {
    let out = match std::process::Command::new("pmset")
        .args(["-g", "batt"])
        .output()
    {
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
        before
            .rsplit(|c: char| !c.is_ascii_digit())
            .next()?
            .parse()
            .ok()
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
                pct = std::fs::read_to_string(p.join("capacity"))
                    .ok()
                    .and_then(|s| s.trim().parse().ok());
            } else if ty.trim() == "Mains" {
                ac = std::fs::read_to_string(p.join("online"))
                    .ok()
                    .map(|s| s.trim() == "1");
            }
        }
    }
    (pct, ac)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn battery() -> (Option<u8>, Option<bool>) {
    (None, None)
}

#[cfg(test)]
mod tmux_tests {
    use super::*;

    #[test]
    fn parses_list_sessions_line() {
        let s = parse_tmux_line("work\t1700000000\t3\t1\t/home/me/proj\tclaude").unwrap();
        assert_eq!(s.name, "work");
        assert_eq!(s.created_ms, 1_700_000_000_000);
        assert_eq!(s.windows, 3);
        assert!(s.attached);
        assert_eq!(s.cwd, "/home/me/proj");
        assert_eq!(s.command, "claude");
        assert!(parse_tmux_line("garbage").is_none());
    }
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
        assert_eq!(
            parse_pmset("Now drawing from 'AC Power'\n"),
            (None, Some(true))
        );
    }
}
