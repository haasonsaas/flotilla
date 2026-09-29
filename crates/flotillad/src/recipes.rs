//! The desired-state recipes shipped in `docs/recipes/` must parse, be
//! internally consistent, and converge without churn.

use crate::config::{Config, StaticIdentityConfig};
use crate::identity::IdentityProvider;
use crate::server::AppState;
use flotilla_core::schema::{DesiredState, ReconcileReport};
use flotilla_core::{keys, Store};
use std::sync::Arc;

const LINUX: &str = include_str!("../../../docs/recipes/build-node.toml");
const MACOS: &str = include_str!("../../../docs/recipes/build-node-macos.toml");

fn parse(text: &str) -> DesiredState {
    toml::from_str(text).expect("recipe parses as DesiredState")
}

fn cleaner(d: &DesiredState) -> &str {
    &d.files
        .iter()
        .find(|f| f.path == "~/.local/bin/flotilla-build-clean")
        .expect("cleanup script present")
        .content
}

#[test]
fn recipes_have_the_build_node_pieces() {
    for (name, text) in [("linux", LINUX), ("macos", MACOS)] {
        let d = parse(text);
        let names: Vec<&str> = d.ensure.iter().map(|e| e.name.as_str()).collect();
        for want in ["rust-toolchain", "sccache", "cargo-sccache-wrapper"] {
            assert!(names.contains(&want), "{name}: missing ensure {want}");
        }
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len(), "{name}: duplicate ensure names");
        // the toolchain and binary are ensured before the wrapper is wired in
        let pos = |n: &str| names.iter().position(|x| *x == n).unwrap();
        assert!(pos("rust-toolchain") < pos("sccache"));
        assert!(pos("sccache") < pos("cargo-sccache-wrapper"));
        for e in &d.ensure {
            assert!(
                !e.check.is_empty() && !e.apply.is_empty(),
                "{name}/{}",
                e.name
            );
        }
        assert!(cleaner(&d).starts_with("#!/bin/sh\n"));
        assert!(d.files.iter().any(|f| f.mode.as_deref() == Some("0755")));
    }
    // one script, two platforms
    assert_eq!(cleaner(&parse(LINUX)), cleaner(&parse(MACOS)));
    let l = parse(LINUX);
    assert!(l
        .files
        .iter()
        .any(|f| f.path.ends_with(".timer") && f.content.contains("OnUnitActiveSec=6h")));
    let m = parse(MACOS);
    assert!(m
        .files
        .iter()
        .any(|f| f.path.ends_with(".plist") && f.content.contains("<integer>21600</integer>")));
}

#[cfg(unix)]
fn sh_syntax_ok(script: &str) -> bool {
    use std::io::Write;
    let mut child = std::process::Command::new("sh")
        .arg("-n")
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    child.wait().unwrap().success()
}

#[cfg(unix)]
#[test]
fn every_shell_command_in_the_recipes_is_valid_sh() {
    for text in [LINUX, MACOS] {
        let d = parse(text);
        assert!(sh_syntax_ok(cleaner(&d)));
        for e in &d.ensure {
            for cmd in [&e.check, &e.apply] {
                assert_eq!(&cmd[..2], ["sh", "-c"], "{}", e.name);
                assert!(sh_syntax_ok(&cmd[2]), "{}: {}", e.name, cmd[2]);
            }
        }
    }
}

#[cfg(unix)]
mod cleaner_script {
    use super::*;
    use std::path::Path;
    use std::process::Command;

    fn scratch() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("flotilla-rc-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn run(script: &Path, root: &str, days: &str) -> std::process::Output {
        Command::new("sh")
            .arg(script)
            .env("BUILD_ROOT", root)
            .env("BUILD_MAX_AGE_DAYS", days)
            .output()
            .unwrap()
    }

    fn age(path: &Path, days: u32) {
        let t = std::time::SystemTime::now() - std::time::Duration::from_secs(days as u64 * 86400);
        let f = std::fs::File::options().write(true).open(path).unwrap();
        f.set_modified(t).unwrap();
    }

    #[test]
    fn removes_only_stale_build_dirs() {
        let dir = scratch();
        let script = dir.join("clean.sh");
        std::fs::write(&script, cleaner(&parse(LINUX))).unwrap();
        let root = dir.join("builds");
        let stale = root.join("stale");
        let active = root.join("active");
        let fresh = root.join("fresh");
        for d in [&stale, &active, &fresh] {
            std::fs::create_dir_all(d.join("target")).unwrap();
        }
        let old = stale.join("target/a.o");
        std::fs::write(&old, "x").unwrap();
        age(&old, 30);
        // a build still writing deep inside an old directory is kept
        let busy_old = active.join("target/old.o");
        std::fs::write(&busy_old, "x").unwrap();
        age(&busy_old, 30);
        std::fs::write(active.join("target/new.o"), "x").unwrap();
        std::fs::write(fresh.join("target/f.o"), "x").unwrap();
        // a loose file next to the build dirs is not a candidate
        std::fs::write(root.join("notes.txt"), "keep").unwrap();
        age(&root.join("notes.txt"), 90);

        let out = run(&script, root.to_str().unwrap(), "7");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!stale.exists());
        assert!(active.exists() && fresh.exists());
        assert!(root.join("notes.txt").exists());
        // idempotent
        assert!(run(&script, root.to_str().unwrap(), "7").status.success());
        assert!(active.exists() && fresh.exists());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn refuses_dangerous_or_malformed_settings() {
        let dir = scratch();
        let script = dir.join("clean.sh");
        std::fs::write(&script, cleaner(&parse(LINUX))).unwrap();
        let victim = dir.join("home").join("project");
        std::fs::create_dir_all(&victim).unwrap();
        std::fs::write(victim.join("f"), "x").unwrap();
        age(&victim.join("f"), 400);
        let home = dir.join("home");
        let bad = [
            ("/", "7"),
            ("relative/path", "7"),
            (home.to_str().unwrap(), "7"),
            (dir.to_str().unwrap(), "-1"),
            (dir.to_str().unwrap(), "seven"),
        ];
        for (root, days) in bad {
            let out = Command::new("sh")
                .arg(&script)
                .env("HOME", &home)
                .env("BUILD_ROOT", root)
                .env("BUILD_MAX_AGE_DAYS", days)
                .output()
                .unwrap();
            assert_eq!(out.status.code(), Some(2), "root={root:?} days={days:?}");
        }
        assert!(victim.exists(), "nothing was deleted");
        // a missing root is not an error
        let out = run(&script, dir.join("nope").to_str().unwrap(), "7");
        assert!(out.status.success());
        std::fs::remove_dir_all(dir).ok();
    }
}

async fn state_in(dir: &std::path::Path) -> AppState {
    let cfg = Config {
        data_dir: dir.to_path_buf(),
        identity: "static".into(),
        static_identity: Some(StaticIdentityConfig {
            node_id: "id-b".into(),
            name: "builder".into(),
            ips: vec![],
            login: "static@local".into(),
            peers: vec![],
        }),
        ..Config::default()
    };
    let identity = Arc::new(IdentityProvider::from_config(&cfg).unwrap());
    let me = identity.me().await.unwrap();
    let store = Arc::new(Store::open(&dir.join("store.redb"), "id-b").unwrap());
    AppState::new(Arc::new(cfg), identity, store, me)
}

/// The file half of the recipe (paths moved under a scratch directory, the
/// ensures that need systemd, launchd or the network left out) converges on
/// the first pass and does nothing on the second.
#[cfg(unix)]
#[tokio::test]
async fn recipe_files_converge_once_and_then_stay_put() {
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!("flotilla-rc-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let state = state_in(&dir).await;
    let home = dir.join("home");
    let mut d = parse(LINUX);
    d.ensure.clear();
    for f in &mut d.files {
        f.path = f.path.replacen("~", home.to_str().unwrap(), 1);
    }
    state.store.put_json(&keys::desired("id-b"), &d).unwrap();

    let report = || -> ReconcileReport {
        state
            .store
            .get(&keys::reconcile("id-b"))
            .unwrap()
            .unwrap()
            .parse()
            .unwrap()
    };
    crate::reconcile::pass(&state).await.unwrap();
    let first = report();
    assert!(first.converged, "{:?}", first.errors);
    assert_eq!(
        first
            .changes
            .iter()
            .filter(|c| c.starts_with("wrote "))
            .count(),
        d.files.len(),
        "{:?}",
        first.changes
    );
    let script = home.join(".local/bin/flotilla-build-clean");
    assert_eq!(
        std::fs::metadata(&script).unwrap().permissions().mode() & 0o777,
        0o755
    );
    assert!(home
        .join(".config/systemd/user/flotilla-build-clean.timer")
        .exists());

    crate::reconcile::pass(&state).await.unwrap();
    let second = report();
    assert!(second.converged);
    assert!(
        second.changes.is_empty(),
        "second pass: {:?}",
        second.changes
    );

    // drift is repaired, and only what drifted
    std::fs::write(&script, "#!/bin/sh\nrm -rf /\n").unwrap();
    crate::reconcile::pass(&state).await.unwrap();
    let third = report();
    assert!(
        third
            .changes
            .iter()
            .all(|c| c.ends_with("flotilla-build-clean")),
        "only the drifted file is touched: {:?}",
        third.changes
    );
    assert!(third.changes.iter().any(|c| c.starts_with("wrote ")));
    assert!(std::fs::read_to_string(&script)
        .unwrap()
        .contains("refusing BUILD_ROOT"));
    std::fs::remove_dir_all(dir).ok();
}
