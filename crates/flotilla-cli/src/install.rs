//! Install flotillad as a per-user service.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::path::PathBuf;
use std::process::Command;

#[derive(Args, Debug)]
pub struct InstallArgs {
    /// Path to flotillad (default: next to this binary, else on PATH)
    #[arg(long)]
    pub daemon_path: Option<PathBuf>,
}

fn home() -> PathBuf {
    directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .expect("home dir")
}

fn find_daemon(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(p) = explicit {
        return p
            .canonicalize()
            .with_context(|| format!("{} not found", p.display()));
    }
    if let Ok(exe) = std::env::current_exe() {
        let sibling = exe.with_file_name("flotillad");
        if sibling.is_file() {
            return Ok(sibling);
        }
    }
    if let Some(paths) = std::env::var_os("PATH") {
        if let Some(p) = std::env::split_paths(&paths)
            .map(|d| d.join("flotillad"))
            .find(|p| p.is_file())
        {
            return Ok(p);
        }
    }
    bail!("flotillad not found; pass --daemon-path")
}

#[cfg(target_os = "macos")]
fn log_dir() -> PathBuf {
    let d = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".local/share"))
        .join("flotilla");
    std::fs::create_dir_all(&d).ok();
    d
}

/// Block until the freshly started daemon answers on loopback, so a
/// `flotilla install && flotilla status` never races the restart.
async fn wait_for_health() -> Result<()> {
    let port = crate::configured_port();
    let url = format!("http://127.0.0.1:{port}/v1/health");
    {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(500))
            .build()?;
        for _ in 0..40 {
            if client
                .get(&url)
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
            {
                return Ok(());
            }
            crate::client::pause(std::time::Duration::from_millis(250)).await;
        }
        bail!("daemon did not answer on {url} within 10s; check the log")
    }
}

fn run(cmd: &mut Command) -> Result<()> {
    let out = cmd.output().with_context(|| format!("running {cmd:?}"))?;
    if !out.status.success() {
        bail!(
            "{cmd:?} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
const LABEL: &str = "dev.haasonsaas.flotilla";

#[cfg(target_os = "macos")]
fn plist_path() -> PathBuf {
    home()
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist"))
}

#[cfg(target_os = "macos")]
pub async fn install(args: InstallArgs) -> Result<()> {
    let daemon = find_daemon(args.daemon_path)?;
    let log = log_dir().join("flotillad.log");
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{LABEL}</string>
  <key>ProgramArguments</key><array><string>{daemon}</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ProcessType</key><string>Background</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>RUST_LOG</key><string>info</string>
  </dict>
  <key>StandardOutPath</key><string>{log}</string>
  <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"#,
        daemon = daemon.display(),
        log = log.display()
    );
    let path = plist_path();
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&path, plist)?;
    let uid = unsafe { libc_getuid() };
    let domain = format!("gui/{uid}");
    let _ = Command::new("launchctl")
        .args(["bootout", &domain, &path.to_string_lossy()])
        .output();
    run(Command::new("launchctl").args(["bootstrap", &domain, &path.to_string_lossy()]))?;
    run(Command::new("launchctl").args(["kickstart", "-k", &format!("{domain}/{LABEL}")]))?;
    wait_for_health().await?;
    println!(
        "installed {LABEL} -> {} (log: {})",
        daemon.display(),
        log.display()
    );
    Ok(())
}

#[cfg(target_os = "macos")]
pub async fn uninstall() -> Result<()> {
    let path = plist_path();
    let uid = unsafe { libc_getuid() };
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}"), &path.to_string_lossy()])
        .output();
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    println!("removed {LABEL}");
    Ok(())
}

#[cfg(target_os = "macos")]
unsafe fn libc_getuid() -> u32 {
    extern "C" {
        fn getuid() -> u32;
    }
    unsafe { getuid() }
}

#[cfg(target_os = "linux")]
fn unit_path() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".config"))
        .join("systemd/user/flotillad.service")
}

#[cfg(target_os = "linux")]
pub async fn install(args: InstallArgs) -> Result<()> {
    let daemon = find_daemon(args.daemon_path)?;
    let unit = format!(
        "[Unit]\nDescription=flotilla fleet daemon\nAfter=network-online.target tailscaled.service\n\n[Service]\nExecStart={}\nRestart=always\nRestartSec=3\nEnvironment=RUST_LOG=info\nEnvironment=PATH=/usr/local/bin:/usr/bin:/bin\n\n[Install]\nWantedBy=default.target\n",
        daemon.display()
    );
    let path = unit_path();
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&path, unit)?;
    run(Command::new("systemctl").args(["--user", "daemon-reload"]))?;
    run(Command::new("systemctl").args(["--user", "enable", "--now", "flotillad.service"]))?;
    run(Command::new("systemctl").args(["--user", "restart", "flotillad.service"]))?;
    wait_for_health().await?;
    println!(
        "installed flotillad.service -> {} (logs: journalctl --user -u flotillad)",
        daemon.display()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
pub async fn uninstall() -> Result<()> {
    let _ = Command::new("systemctl")
        .args(["--user", "disable", "--now", "flotillad.service"])
        .output();
    let path = unit_path();
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    let _ = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .output();
    println!("removed flotillad.service");
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub async fn install(_args: InstallArgs) -> Result<()> {
    bail!("install is only supported on macOS and Linux")
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub async fn uninstall() -> Result<()> {
    bail!("uninstall is only supported on macOS and Linux")
}
