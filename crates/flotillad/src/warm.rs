//! Warm build caches: `[[warm_cache]]` entries become `warm.<name>` facts so
//! the scheduler can prefer nodes that already hold a build's dependencies.

use crate::config::WarmCacheConfig;
use flotilla_core::schema::WarmCache;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

/// Walking a cargo target directory takes seconds, so sizes are recomputed
/// in the background at most this often and facts carry the last result.
const SIZE_TTL: Duration = Duration::from_secs(300);
const KEY_CMD_TIMEOUT: Duration = Duration::from_secs(5);

struct SizeEntry {
    size_mb: u64,
    at: Option<Instant>,
    busy: bool,
}

fn sizes() -> &'static Mutex<HashMap<String, SizeEntry>> {
    static S: OnceLock<Mutex<HashMap<String, SizeEntry>>> = OnceLock::new();
    S.get_or_init(Default::default)
}

/// Facts for every configured cache whose directory exists.
pub fn collect(cfgs: &[WarmCacheConfig]) -> BTreeMap<String, WarmCache> {
    let now = flotilla_core::now_ms();
    let mut out = BTreeMap::new();
    for c in cfgs {
        if c.name.is_empty() {
            continue;
        }
        let Some(last_used_ms) = last_used_ms(&c.path) else {
            continue;
        };
        let key = c
            .key_cmd
            .as_deref()
            .and_then(|cmd| run_key_cmd(cmd, KEY_CMD_TIMEOUT))
            .unwrap_or_default();
        out.insert(
            c.name.clone(),
            WarmCache {
                key,
                path: c.path.to_string_lossy().into_owned(),
                size_mb: cached_size_mb(&c.name, &c.path),
                last_used_ms,
                age_secs: now.saturating_sub(last_used_ms) / 1000,
            },
        );
    }
    out
}

/// Newest mtime among the directory and its entries two levels down (for a
/// cargo target: `target`, `target/debug`, `target/debug/deps`, ...). A build
/// adds files to those directories, so this tracks use without a full walk.
pub fn last_used_ms(path: &Path) -> Option<u64> {
    let md = std::fs::metadata(path).ok()?;
    if !md.is_dir() {
        return None;
    }
    let mut newest = mtime_ms(&md);
    for e1 in std::fs::read_dir(path).ok()?.flatten() {
        let Ok(m1) = e1.metadata() else { continue };
        newest = newest.max(mtime_ms(&m1));
        if m1.is_dir() {
            if let Ok(rd) = std::fs::read_dir(e1.path()) {
                for e2 in rd.flatten() {
                    if let Ok(m2) = e2.metadata() {
                        newest = newest.max(mtime_ms(&m2));
                    }
                }
            }
        }
    }
    Some(newest)
}

fn mtime_ms(md: &std::fs::Metadata) -> u64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Total size of regular files under `path`, in whole MB (rounded up).
pub fn dir_size_mb(path: &Path) -> u64 {
    fn walk(p: &Path, total: &mut u64) {
        let Ok(rd) = std::fs::read_dir(p) else { return };
        for e in rd.flatten() {
            let Ok(md) = e.path().symlink_metadata() else {
                continue;
            };
            if md.is_dir() {
                walk(&e.path(), total);
            } else if md.is_file() {
                *total += md.len();
            }
        }
    }
    let mut total = 0;
    walk(path, &mut total);
    total.div_ceil(1024 * 1024)
}

fn cached_size_mb(name: &str, path: &Path) -> u64 {
    let mut map = sizes().lock().unwrap();
    let e = map.entry(name.to_string()).or_insert(SizeEntry {
        size_mb: 0,
        at: None,
        busy: false,
    });
    let stale = e.at.map(|t| t.elapsed() > SIZE_TTL).unwrap_or(true);
    if stale && !e.busy {
        e.busy = true;
        let (name, path) = (name.to_string(), path.to_path_buf());
        std::thread::spawn(move || {
            let mb = dir_size_mb(&path);
            if let Some(e) = sizes().lock().unwrap().get_mut(&name) {
                e.size_mb = mb;
                e.at = Some(Instant::now());
                e.busy = false;
            }
        });
    }
    e.size_mb
}

/// First non-empty output line of `sh -c cmd`, or None on failure/timeout.
pub fn run_key_cmd(cmd: &str, timeout: Duration) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stdout.read_to_string(&mut s);
        s
    });
    let start = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st,
            Ok(None) if start.elapsed() < timeout => std::thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    let out = reader.join().ok()?;
    if !status.success() {
        return None;
    }
    out.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("flotilla-warm-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn key_cmd_first_line_failure_and_timeout() {
        let t = Duration::from_secs(5);
        assert_eq!(
            run_key_cmd("echo abc123; echo two", t).as_deref(),
            Some("abc123")
        );
        assert_eq!(
            run_key_cmd("printf '\\n  \\nk1\\n'", t).as_deref(),
            Some("k1")
        );
        assert_eq!(run_key_cmd("echo x; exit 3", t), None);
        let start = Instant::now();
        assert_eq!(run_key_cmd("sleep 30", Duration::from_millis(200)), None);
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn size_and_last_used_reflect_directory_contents() {
        let d = tmpdir();
        assert_eq!(dir_size_mb(&d), 0);
        std::fs::create_dir_all(d.join("debug/deps")).unwrap();
        std::fs::write(d.join("debug/deps/big"), vec![0u8; 3 * 1024 * 1024]).unwrap();
        assert_eq!(dir_size_mb(&d), 3);
        let used = last_used_ms(&d).unwrap();
        assert!(flotilla_core::now_ms().saturating_sub(used) < 60_000);
        assert!(last_used_ms(&d.join("missing")).is_none());
        assert!(
            last_used_ms(&d.join("debug/deps/big")).is_none(),
            "files are not caches"
        );
        std::fs::remove_dir_all(d).ok();
    }

    #[test]
    fn collect_reports_existing_caches_only() {
        let d = tmpdir();
        let cfgs = vec![
            WarmCacheConfig {
                name: "mono-rust".into(),
                path: d.clone(),
                key_cmd: Some("echo deadbee".into()),
            },
            WarmCacheConfig {
                name: "gone".into(),
                path: d.join("nope"),
                key_cmd: None,
            },
            WarmCacheConfig {
                name: "nokey".into(),
                path: d.clone(),
                key_cmd: Some("exit 1".into()),
            },
        ];
        let w = collect(&cfgs);
        assert_eq!(w.len(), 2);
        assert_eq!(w["mono-rust"].key, "deadbee");
        assert!(w["mono-rust"].age_secs < 60);
        assert!(w["mono-rust"].label_value().starts_with("deadbee@"));
        assert_eq!(w["nokey"].key, "");
        assert!(!w.contains_key("gone"));
        std::fs::remove_dir_all(d).ok();
    }
}
