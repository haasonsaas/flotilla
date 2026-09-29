//! `flotilla build`: run a command in a fixed checkout of a repo on the node
//! that already has the warm build cache.
//!
//! The checkout path is the same on every node on purpose: a shared sccache
//! backend only hits across machines when source paths are identical. Jobs
//! for the same path share a `lock`, so a node runs them one at a time.

use crate::client::{pause, Client};
use crate::commands::{parse_env, print_json};
use anyhow::{anyhow, bail, Result};
use clap::Args;
use flotilla_core::keys;
use flotilla_core::schema::*;
use flotilla_core::selector::Selector;
use std::io::Write;
use std::time::Duration;

#[derive(Args, Debug)]
pub struct BuildArgs {
    /// GitHub repository to build, e.g. dx-corp/mono
    #[arg(long)]
    pub repo: String,
    /// Branch, tag or commit to check out (detached)
    #[arg(long = "ref", default_value = "main")]
    pub git_ref: String,
    /// Checkout path on the node (default: /builds/<repo name>). Keep it
    /// identical on every node so a shared sccache can hit across machines.
    #[arg(long)]
    pub path: Option<String>,
    /// Warm cache to prefer (default: <repo name>-rust)
    #[arg(long = "prefer-warm")]
    pub prefer_warm: Option<String>,
    /// Clone URL (default: https://github.com/<repo>.git)
    #[arg(long)]
    pub remote: Option<String>,
    /// Only nodes whose labels match may run it, e.g. os=linux
    #[arg(short = 'l', long = "selector")]
    pub selector: Option<Selector>,
    #[arg(long)]
    pub timeout: Option<u64>,
    #[arg(short = 'e', long = "env")]
    pub env: Vec<String>,
    /// Submit and print the job id without following it
    #[arg(long)]
    pub detach: bool,
    /// Command to run inside the checkout, e.g. cargo test -p foo bar
    #[arg(required = true, last = true)]
    pub cmd: Vec<String>,
}

/// Fetch the ref into the fixed checkout, detach onto it, then exec the
/// command (passed as "$@") from the checkout root. Paths and refs arrive in
/// the environment so nothing needs shell quoting.
const SCRIPT: &str = r#"set -eu
dir="$FLOTILLA_BUILD_DIR"
mkdir -p "$dir"
cd "$dir"
[ -d .git ] || git init -q .
if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$FLOTILLA_BUILD_REMOTE"
else
  git remote add origin "$FLOTILLA_BUILD_REMOTE"
fi
git fetch -q --prune --no-tags origin "$FLOTILLA_BUILD_REF"
git checkout -q --detach --force FETCH_HEAD
echo "flotilla build: $FLOTILLA_BUILD_REPO at $(git rev-parse --short HEAD) in $dir" >&2
exec "$@"
"#;

fn repo_name(repo: &str) -> &str {
    repo.rsplit('/').next().unwrap_or(repo)
}

fn looks_like_commit(r: &str) -> bool {
    (7..=40).contains(&r.len()) && r.bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn build_spec(a: &BuildArgs, submitted_by: &str) -> Result<JobSpec> {
    let parts: Vec<&str> = a.repo.split('/').collect();
    if parts.len() != 2 || parts.iter().any(|p| p.is_empty()) {
        bail!("--repo wants owner/name, got {:?}", a.repo);
    }
    let name = repo_name(&a.repo);
    let dir = a.path.clone().unwrap_or_else(|| format!("/builds/{name}"));
    if !dir.starts_with('/') {
        bail!("--path must be absolute (the same path on every node), got {dir:?}");
    }
    let mut env = parse_env(&a.env)?;
    env.insert("FLOTILLA_BUILD_DIR".into(), dir.clone());
    env.insert("FLOTILLA_BUILD_REPO".into(), a.repo.clone());
    env.insert("FLOTILLA_BUILD_REF".into(), a.git_ref.clone());
    env.insert(
        "FLOTILLA_BUILD_REMOTE".into(),
        a.remote
            .clone()
            .unwrap_or_else(|| format!("https://github.com/{}.git", a.repo)),
    );
    let mut cmd: Vec<String> = vec![
        "sh".into(),
        "-c".into(),
        SCRIPT.into(),
        "flotilla-build".into(),
    ];
    cmd.extend(a.cmd.iter().cloned());
    Ok(JobSpec {
        id: uuid::Uuid::new_v4().to_string(),
        cmd,
        cwd: None,
        env,
        selector: a.selector.clone().unwrap_or_default(),
        node: None,
        submitted_by: submitted_by.to_string(),
        submitted_at_ms: flotilla_core::now_ms(),
        timeout_secs: a.timeout,
        cancelled: false,
        pick: None,
        tmux: None,
        kind: Some("build".into()),
        after: Vec::new(),
        batch: None,
        retries: 0,
        retry: 0,
        cancel_reason: None,
        prefer_warm: Some(
            a.prefer_warm
                .clone()
                .unwrap_or_else(|| format!("{name}-rust")),
        ),
        warm_key: looks_like_commit(&a.git_ref).then(|| a.git_ref.clone()),
        lock: Some(dir),
    })
}

pub async fn build(c: &Client, a: BuildArgs, json: bool) -> Result<()> {
    let me = c.me().await?;
    let spec = build_spec(&a, &me.name)?;
    c.put_json(&keys::job(&spec.id), &spec).await?;
    if a.detach {
        if json {
            return print_json(&spec);
        }
        println!("{}", spec.id);
        return Ok(());
    }
    eprintln!("{}: submitted", &spec.id[..8]);
    let code = follow(c, &spec.id).await?;
    let _ = std::io::stdout().flush();
    std::process::exit(code);
}

/// Stream the job's output as it is written and return its exit code.
/// Ctrl-C cancels the job.
async fn follow(c: &Client, id: &str) -> Result<i32> {
    let mut offset = 0u64;
    let mut last_state = None;
    let mut cancelled_polls = 0;
    loop {
        let spec: JobSpec = c
            .get_record(&keys::job(id))
            .await?
            .ok_or_else(|| anyhow!("job {id} disappeared"))?
            .parse()?;
        let claim = c
            .get_record(&keys::claim(id))
            .await?
            .map(|r| r.parse::<JobClaim>())
            .transpose()?;
        let result = c
            .get_record(&keys::result(id))
            .await?
            .map(|r| r.parse::<JobResult>())
            .transpose()?;
        let state = JobState::derive_at(
            &spec,
            claim.as_ref(),
            result.as_ref(),
            flotilla_core::now_ms(),
        );
        if last_state != Some(state) {
            eprintln!("{}: {state:?}", &id[..8]);
            last_state = Some(state);
        }
        if claim.is_some() {
            // The local daemon proxies to the executor. No log yet, or an
            // executor that is briefly unreachable, just means try again.
            if let Ok(Some(bytes)) = c.job_log_from(id, offset).await {
                offset += bytes.len() as u64;
                let mut out = std::io::stdout().lock();
                let _ = out.write_all(&bytes);
                let _ = out.flush();
            }
        }
        if let Some(r) = result {
            if let Some(e) = &r.error {
                eprintln!("error: {e}");
            }
            return Ok(r.exit_code.unwrap_or(1));
        }
        if state == JobState::Cancelled {
            if claim.is_some() && cancelled_polls < 15 {
                cancelled_polls += 1;
            } else {
                bail!(
                    "job cancelled before it ran{}",
                    spec.cancel_reason
                        .map(|r| format!(": {r}"))
                        .unwrap_or_default()
                );
            }
        }
        tokio::select! {
            _ = pause(Duration::from_secs(1)) => {}
            _ = tokio::signal::ctrl_c() => {
                let mut spec = spec;
                spec.cancelled = true;
                c.put_json(&keys::job(id), &spec).await?;
                eprintln!("{}: cancelled", &id[..8]);
                return Ok(130);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(repo: &str, git_ref: &str, cmd: &[&str]) -> BuildArgs {
        BuildArgs {
            repo: repo.into(),
            git_ref: git_ref.into(),
            path: None,
            prefer_warm: None,
            remote: None,
            selector: None,
            timeout: None,
            env: vec![],
            detach: false,
            cmd: cmd.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn spec_uses_fixed_path_lock_and_warm_cache() {
        let a = args(
            "dx-corp/mono",
            "feature/x",
            &["cargo", "test", "-p", "foo", "bar baz"],
        );
        let s = build_spec(&a, "me").unwrap();
        assert_eq!(s.prefer_warm.as_deref(), Some("mono-rust"));
        assert_eq!(s.lock.as_deref(), Some("/builds/mono"));
        assert_eq!(s.env["FLOTILLA_BUILD_DIR"], "/builds/mono");
        assert_eq!(s.env["FLOTILLA_BUILD_REF"], "feature/x");
        assert_eq!(
            s.env["FLOTILLA_BUILD_REMOTE"],
            "https://github.com/dx-corp/mono.git"
        );
        assert_eq!(s.warm_key, None, "a branch name is not a cache key");
        // user args follow the script untouched, as "$@"
        assert_eq!(&s.cmd[..2], ["sh", "-c"]);
        assert_eq!(s.cmd[3], "flotilla-build");
        assert_eq!(&s.cmd[4..], ["cargo", "test", "-p", "foo", "bar baz"]);
    }

    #[test]
    fn overrides_and_validation() {
        let mut a = args("dx-corp/mono", "0123abc", &["true"]);
        a.path = Some("/srv/b/mono".into());
        a.prefer_warm = Some("other".into());
        a.remote = Some("git@github.com:dx-corp/mono.git".into());
        a.env = vec!["A=1".into()];
        let s = build_spec(&a, "me").unwrap();
        assert_eq!(s.lock.as_deref(), Some("/srv/b/mono"));
        assert_eq!(s.prefer_warm.as_deref(), Some("other"));
        assert_eq!(s.warm_key.as_deref(), Some("0123abc"));
        assert_eq!(s.env["A"], "1");
        assert_eq!(
            s.env["FLOTILLA_BUILD_REMOTE"],
            "git@github.com:dx-corp/mono.git"
        );
        assert!(build_spec(&args("mono", "main", &["x"]), "me").is_err());
        assert!(build_spec(&args("/mono", "main", &["x"]), "me").is_err());
        let mut rel = args("a/b", "main", &["x"]);
        rel.path = Some("builds/b".into());
        assert!(build_spec(&rel, "me").is_err());
    }

    fn git(dir: &std::path::Path, args: &[&str]) -> std::process::Output {
        std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@example.com")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@example.com")
            .output()
            .unwrap()
    }

    fn git_ok(dir: &std::path::Path, args: &[&str]) {
        let out = git(dir, args);
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Run the job's command the way the executor would.
    fn run_spec(spec: &JobSpec) -> std::process::Output {
        std::process::Command::new(&spec.cmd[0])
            .args(&spec.cmd[1..])
            .envs(&spec.env)
            .output()
            .unwrap()
    }

    #[test]
    fn script_checks_out_the_ref_detached_and_returns_the_exit_code() {
        if std::process::Command::new("git")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let root = std::env::temp_dir().join(format!("flotilla-build-{}", uuid::Uuid::new_v4()));
        let origin = root.join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        git_ok(&origin, &["init", "-q", "-b", "main"]);
        std::fs::write(origin.join("v.txt"), "one\n").unwrap();
        git_ok(&origin, &["add", "."]);
        git_ok(&origin, &["commit", "-q", "-m", "one"]);
        git_ok(&origin, &["checkout", "-q", "-b", "feature"]);
        std::fs::write(origin.join("v.txt"), "two\n").unwrap();
        git_ok(&origin, &["commit", "-q", "-am", "two"]);
        git_ok(&origin, &["checkout", "-q", "main"]);

        let checkout = root.join("builds/repo");
        let make = |git_ref: &str, cmd: &[&str]| {
            let mut a = args("o/repo", git_ref, cmd);
            a.path = Some(checkout.to_string_lossy().into_owned());
            a.remote = Some(origin.to_string_lossy().into_owned());
            build_spec(&a, "t").unwrap()
        };

        // first run creates the checkout and runs the command inside it
        let out = run_spec(&make("feature", &["cat", "v.txt"]));
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout), "two\n");
        assert!(
            !git(&checkout, &["symbolic-ref", "-q", "HEAD"])
                .status
                .success(),
            "HEAD is detached"
        );
        // the same path is reused and moved to another ref
        let out = run_spec(&make("main", &["cat", "v.txt"]));
        assert_eq!(String::from_utf8_lossy(&out.stdout), "one\n");
        // the command's exit code is the job's
        let out = run_spec(&make("main", &["sh", "-c", "exit 7"]));
        assert_eq!(out.status.code(), Some(7));
        // an unknown ref fails before the command runs
        let out = run_spec(&make("no-such-ref", &["true"]));
        assert!(!out.status.success());
        std::fs::remove_dir_all(root).ok();
    }
}
