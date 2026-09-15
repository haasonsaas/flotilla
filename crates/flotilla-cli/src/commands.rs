use crate::client::{pause, Client};
use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, Table};
use flotilla_core::api::*;
use flotilla_core::keys;
use flotilla_core::schema::*;
use flotilla_core::selector::Selector;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::io::Write;
use std::time::Duration;

fn print_json<T: serde::Serialize>(v: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

fn age(secs: u64) -> String {
    if secs < 90 {
        format!("{secs}s")
    } else if secs < 5400 {
        format!("{}m", secs / 60)
    } else if secs < 172_800 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

fn ms_ago(ms: u64) -> String {
    age(flotilla_core::now_ms().saturating_sub(ms) / 1000)
}

/// Re-run `render` whenever a matching record changes (debounced) and at
/// least every `fallback`, until interrupted.
async fn watch_loop<F, Fut>(c: &Client, prefix: &str, fallback: Duration, render: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    loop {
        print!("\x1b[2J\x1b[H");
        render().await?;
        println!("\n(watching; ctrl-c to stop)");
        let mut events = match c.events(prefix).await {
            Ok(e) => e,
            Err(_) => {
                pause(fallback).await;
                continue;
            }
        };
        // wait for one event or the fallback tick, then debounce briefly
        let _ = tokio::time::timeout(fallback, events.next()).await;
        pause(Duration::from_millis(300)).await;
    }
}

pub async fn status(c: &Client, json: bool, watch: bool) -> Result<()> {
    if watch {
        return watch_loop(c, keys::NODE, Duration::from_secs(5), || {
            status_once(c, json)
        })
        .await;
    }
    status_once(c, json).await
}

pub async fn events(c: &Client, prefix: &str) -> Result<()> {
    let mut stream = c.events(prefix).await?;
    let out = std::io::stdout();
    while let Some(ev) = stream.next().await {
        let ev = ev?;
        let mut o = out.lock();
        let written = writeln!(o, "{}", serde_json::to_string(&ev)?).and_then(|_| o.flush());
        if let Err(e) = written {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                return Ok(()); // reader (e.g. `head`) went away
            }
            return Err(e.into());
        }
    }
    Ok(())
}

async fn status_once(c: &Client, json: bool) -> Result<()> {
    let st = c.status().await?;
    if json {
        return print_json(&st);
    }
    let sync: BTreeMap<String, PeerSyncState> = c
        .sync_state()
        .await
        .map(|r| r.peers.into_iter().map(|p| (p.key.clone(), p)).collect())
        .unwrap_or_default();
    let mut t = Table::new();
    t.load_preset(UTF8_FULL_CONDENSED);
    t.set_header([
        "node",
        "state",
        "os",
        "arch",
        "cpus",
        "load",
        "mem free",
        "disk free",
        "batt",
        "jobs",
        "labels",
        "facts age",
        "synced",
    ]);
    for n in &st.nodes {
        let f = &n.facts;
        let state = if n.online {
            Cell::new("online").fg(Color::Green)
        } else {
            Cell::new("offline").fg(Color::DarkGrey)
        };
        let batt = match (f.battery_pct, f.on_ac) {
            (Some(p), Some(true)) => format!("{p}% ac"),
            (Some(p), _) => format!("{p}%"),
            (None, Some(true)) => "ac".into(),
            _ => "-".into(),
        };
        let labels: Vec<String> = f
            .labels
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "os" | "arch" | "node"))
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        let name = if f.node_id == st.me {
            format!("{} *", f.name)
        } else {
            f.name.clone()
        };
        let synced = if f.node_id == st.me {
            Cell::new("-")
        } else {
            match sync.get(&f.node_id) {
                Some(p) if p.consecutive_failures > 0 => {
                    Cell::new(format!("failing x{}", p.consecutive_failures)).fg(Color::Red)
                }
                Some(p) => Cell::new(
                    p.last_ok_ms
                        .map(|t| ms_ago(t) + " ago")
                        .unwrap_or_else(|| "never".into()),
                ),
                None => Cell::new("never").fg(Color::DarkGrey),
            }
        };
        t.add_row(vec![
            Cell::new(name),
            state,
            Cell::new(&f.os),
            Cell::new(&f.arch),
            Cell::new(f.cpus),
            Cell::new(format!("{:.1}", f.load_1m)),
            Cell::new(format!("{} MB", f.mem_free_mb)),
            Cell::new(format!("{} GB", f.disk_free_gb)),
            Cell::new(batt),
            Cell::new(f.running_jobs.len()),
            Cell::new(labels.join(",")),
            Cell::new(age(n.facts_age_secs)),
            synced,
        ]);
    }
    println!("{t}");
    Ok(())
}

pub async fn whoami(c: &Client, json: bool) -> Result<()> {
    let me = c.me().await?;
    if json {
        return print_json(&me);
    }
    println!(
        "{} ({}) v{} on {}:{}",
        me.name,
        me.node_id,
        me.version,
        me.tailscale_ips.join(","),
        me.port
    );
    Ok(())
}

pub async fn peers(c: &Client, json: bool) -> Result<()> {
    let p = c.peers().await?;
    if json {
        return print_json(&p);
    }
    let mut t = Table::new();
    t.load_preset(UTF8_FULL_CONDENSED);
    t.set_header(["peer", "online", "os", "ips", "tags"]);
    for peer in p.peers.iter().filter(|p| p.online) {
        t.add_row([
            peer.name.clone(),
            "yes".into(),
            peer.os.clone(),
            peer.ips.join(","),
            peer.tags.join(","),
        ]);
    }
    let offline = p.peers.iter().filter(|p| !p.online).count();
    println!("{t}");
    if offline > 0 {
        println!("({offline} offline peers hidden; --json shows all)");
    }
    Ok(())
}

pub async fn sync(c: &Client, peer: Option<String>, json: bool) -> Result<()> {
    let results = c.sync_now(peer).await?;
    if json {
        return print_json(&results);
    }
    if results.is_empty() {
        bail!("no matching sync candidates (online peers or seeds)");
    }
    let mut t = Table::new();
    t.load_preset(UTF8_FULL_CONDENSED);
    t.set_header(["peer", "url", "result", "pulled", "pushed"]);
    let mut failed = false;
    for r in results {
        let cell = if r.ok {
            Cell::new("ok").fg(Color::Green)
        } else {
            failed = true;
            Cell::new(r.error.clone().unwrap_or_default()).fg(Color::Red)
        };
        t.add_row(vec![
            Cell::new(r.name),
            Cell::new(r.url),
            cell,
            Cell::new(r.pulled),
            Cell::new(r.pushed),
        ]);
    }
    println!("{t}");
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

// ---------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct TargetArgs {
    /// Target node by name or id (repeatable)
    #[arg(short = 'n', long = "node")]
    nodes: Vec<String>,
    /// Target nodes whose labels match, e.g. os=macos,arch=aarch64
    #[arg(short = 'l', long = "selector")]
    selector: Option<Selector>,
    /// Every online node
    #[arg(long)]
    all: bool,
}

impl TargetArgs {
    fn pick(&self, st: &StatusResponse) -> Result<Vec<NodeStatus>> {
        if self.nodes.is_empty() && self.selector.is_none() && !self.all {
            bail!("pick targets with --node, --selector or --all");
        }
        let mut out = Vec::new();
        for n in &st.nodes {
            let by_name = self
                .nodes
                .iter()
                .any(|x| x == &n.facts.name || x == &n.facts.node_id);
            let by_sel = self
                .selector
                .as_ref()
                .map(|s| s.matches(&n.facts.labels))
                .unwrap_or(false);
            if by_name || by_sel || self.all {
                out.push(n.clone());
            }
        }
        for want in &self.nodes {
            if !out
                .iter()
                .any(|n| &n.facts.name == want || &n.facts.node_id == want)
            {
                bail!("unknown node {want:?} (see `flotilla status`)");
            }
        }
        Ok(out)
    }
}

#[derive(Args, Debug)]
pub struct RunArgs {
    #[command(flatten)]
    target: TargetArgs,
    /// Working directory on the target
    #[arg(long)]
    cwd: Option<String>,
    /// Kill after this many seconds
    #[arg(long)]
    timeout: Option<u64>,
    /// Environment variable KEY=VALUE (repeatable)
    #[arg(short = 'e', long = "env")]
    env: Vec<String>,
    /// Command and arguments
    #[arg(required = true, last = true)]
    cmd: Vec<String>,
}

fn parse_env(pairs: &[String]) -> Result<BTreeMap<String, String>> {
    pairs
        .iter()
        .map(|p| {
            p.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .ok_or_else(|| anyhow!("bad env {p:?}, want KEY=VALUE"))
        })
        .collect()
}

fn node_url(c: &Client, st: &StatusResponse, n: &NodeStatus) -> Result<Client> {
    if n.facts.node_id == st.me {
        return Ok(c.clone());
    }
    let url = peer_url(&n.facts.tailscale_ips, n.facts.port)
        .ok_or_else(|| anyhow!("{} has no reachable address", n.facts.name))?;
    Ok(c.at(&url))
}

pub async fn run(c: &Client, args: RunArgs) -> Result<()> {
    let st = c.status().await?;
    let targets: Vec<NodeStatus> = args
        .target
        .pick(&st)?
        .into_iter()
        .filter(|n| n.online)
        .collect();
    if targets.is_empty() {
        bail!("no online targets");
    }
    let req = ExecRequest {
        cmd: args.cmd.clone(),
        cwd: args.cwd.clone(),
        env: parse_env(&args.env)?,
        timeout_secs: args.timeout,
    };
    let width = targets
        .iter()
        .map(|n| n.facts.name.len())
        .max()
        .unwrap_or(0);
    let mut handles = Vec::new();
    for n in targets {
        let client = node_url(c, &st, &n)?;
        let req = req.clone();
        let name = n.facts.name.clone();
        handles.push(tokio::spawn(async move {
            (name.clone(), stream_one(client, &name, width, req).await)
        }));
    }
    let mut worst = 0;
    for h in handles {
        let (name, res) = h.await?;
        match res {
            Ok(Some(0)) => {}
            Ok(Some(code)) => {
                eprintln!("[{name}] exit {code}");
                worst = worst.max(code);
            }
            Ok(None) => {
                eprintln!("[{name}] no exit code");
                worst = worst.max(1);
            }
            Err(e) => {
                eprintln!("[{name}] error: {e:#}");
                worst = worst.max(1);
            }
        }
    }
    if worst != 0 {
        std::process::exit(worst);
    }
    Ok(())
}

async fn stream_one(
    client: Client,
    name: &str,
    width: usize,
    req: ExecRequest,
) -> Result<Option<i32>> {
    let mut frames = client.exec(&req).await?;
    let stdout = std::io::stdout();
    let stderr = std::io::stderr();
    let mut code = None;
    while let Some(f) = frames.next().await {
        match f? {
            ExecFrame::Stdout { data } => {
                let mut o = stdout.lock();
                write!(o, "[{name:<width$}] {data}")?;
                if !data.ends_with('\n') {
                    writeln!(o)?;
                }
            }
            ExecFrame::Stderr { data } => {
                let mut e = stderr.lock();
                write!(e, "[{name:<width$}] {data}")?;
                if !data.ends_with('\n') {
                    writeln!(e)?;
                }
            }
            ExecFrame::Error { message } => eprintln!("[{name:<width$}] error: {message}"),
            ExecFrame::Exit { code: c } => code = c,
            ExecFrame::Keepalive => {}
        }
    }
    Ok(code)
}

// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum JobCmd {
    /// Submit a job into the replicated store
    Submit(Box<SubmitArgs>),
    /// List jobs
    Ls {
        /// Include finished jobs older than this many hours (default 24)
        #[arg(long, default_value_t = 24)]
        hours: u64,
        /// Re-render as jobs change
        #[arg(long, short)]
        watch: bool,
    },
    /// Show one job
    Show { id: String },
    /// Fetch the full log from the node that ran the job
    Logs { id: String },
    /// Block until the job finishes, then print its tail and exit with its code
    Wait { id: String },
    /// Mark a job cancelled (the executor kills it)
    Cancel { id: String },
    /// Delete a job and its claim/result records
    Rm { id: String },
}

#[derive(Args, Debug)]
pub struct SubmitArgs {
    /// Pin to one node by name or id
    #[arg(short = 'n', long = "node")]
    node: Option<String>,
    /// Only nodes whose labels match may claim it, e.g. os=macos
    #[arg(short = 'l', long = "selector")]
    selector: Option<Selector>,
    #[arg(long)]
    cwd: Option<String>,
    #[arg(long)]
    timeout: Option<u64>,
    #[arg(short = 'e', long = "env")]
    env: Vec<String>,
    /// Wait for completion and stream the result
    #[arg(long)]
    wait: bool,
    /// Placement: `least-load` picks the least loaded eligible node
    #[arg(long)]
    pick: Option<String>,
    /// Run inside a detached tmux session of this name on the executor
    #[arg(long)]
    tmux: Option<String>,
    /// Only run after these jobs succeeded (id or prefix, repeatable)
    #[arg(long = "after")]
    after: Vec<String>,
    /// Batch label for grouping
    #[arg(long)]
    batch: Option<String>,
    /// Automatic retries on failure
    #[arg(long, default_value_t = 0)]
    retries: u32,
    #[arg(required = true, last = true)]
    cmd: Vec<String>,
}

fn find_id(prefix: &str, ids: &[String]) -> Result<String> {
    let matches: Vec<&String> = ids.iter().filter(|i| i.starts_with(prefix)).collect();
    match matches.len() {
        1 => Ok(matches[0].clone()),
        0 => bail!("no job matching {prefix:?}"),
        _ => bail!("ambiguous job prefix {prefix:?}"),
    }
}

async fn resolve_job(c: &Client, prefix: &str) -> Result<String> {
    let ids: Vec<String> = c
        .list(keys::JOB, false)
        .await?
        .iter()
        .filter_map(|r| keys::id_of(keys::JOB, &r.key).map(String::from))
        .collect();
    find_id(prefix, &ids)
}

struct JobView {
    spec: JobSpec,
    claim: Option<JobClaim>,
    result: Option<JobResult>,
    state: JobState,
}

async fn load_jobs(c: &Client) -> Result<Vec<JobView>> {
    let specs = c.list(keys::JOB, false).await?;
    let claims: BTreeMap<String, JobClaim> = c
        .list(keys::CLAIM, false)
        .await?
        .into_iter()
        .filter_map(|r| r.parse::<JobClaim>().ok().map(|c| (c.job_id.clone(), c)))
        .collect();
    let results: BTreeMap<String, JobResult> = c
        .list(keys::RESULT, false)
        .await?
        .into_iter()
        .filter_map(|r| r.parse::<JobResult>().ok().map(|c| (c.job_id.clone(), c)))
        .collect();
    let mut out = Vec::new();
    for r in specs {
        let Ok(spec) = r.parse::<JobSpec>() else {
            continue;
        };
        let claim = claims.get(&spec.id).cloned();
        let result = results.get(&spec.id).cloned();
        let state = JobState::derive_at(
            &spec,
            claim.as_ref(),
            result.as_ref(),
            flotilla_core::now_ms(),
        );
        out.push(JobView {
            spec,
            claim,
            result,
            state,
        });
    }
    out.sort_by_key(|j| std::cmp::Reverse(j.spec.submitted_at_ms));
    Ok(out)
}

async fn node_names(c: &Client) -> Result<BTreeMap<String, String>> {
    Ok(c.status()
        .await?
        .nodes
        .into_iter()
        .map(|n| (n.facts.node_id, n.facts.name))
        .collect())
}

pub async fn job(c: &Client, cmd: JobCmd, json: bool) -> Result<()> {
    match cmd {
        JobCmd::Submit(a) => {
            let a = *a;
            let me = c.me().await?;
            let node = match &a.node {
                Some(n) => {
                    let st = c.status().await?;
                    Some(
                        st.nodes
                            .iter()
                            .find(|x| &x.facts.name == n || &x.facts.node_id == n)
                            .map(|x| x.facts.node_id.clone())
                            .ok_or_else(|| anyhow!("unknown node {n:?}"))?,
                    )
                }
                None => None,
            };
            let spec = JobSpec {
                id: uuid::Uuid::new_v4().to_string(),
                cmd: a.cmd,
                cwd: a.cwd,
                env: parse_env(&a.env)?,
                selector: a.selector.unwrap_or_default(),
                node,
                submitted_by: me.name,
                submitted_at_ms: flotilla_core::now_ms(),
                timeout_secs: a.timeout,
                cancelled: false,
                pick: a.pick,
                tmux: a.tmux,
                kind: None,
                after: resolve_many(c, &a.after).await?,
                batch: a.batch,
                retries: a.retries,
                retry: 0,
                cancel_reason: None,
            };
            c.put_json(&keys::job(&spec.id), &spec).await?;
            if json && !a.wait {
                return print_json(&spec);
            }
            println!("{}", spec.id);
            if a.wait {
                return wait(c, &spec.id, json).await;
            }
            Ok(())
        }
        JobCmd::Ls { hours, watch } => {
            if watch {
                return watch_loop(c, "", Duration::from_secs(5), || {
                    job_ls_once(c, hours, json)
                })
                .await;
            }
            job_ls_once(c, hours, json).await
        }
        JobCmd::Show { id } => {
            let id = resolve_job(c, &id).await?;
            let j = load_jobs(c)
                .await?
                .into_iter()
                .find(|j| j.spec.id == id)
                .ok_or_else(|| anyhow!("job vanished"))?;
            if json {
                return print_json(
                    &serde_json::json!({"spec": j.spec, "claim": j.claim, "result": j.result, "state": j.state}),
                );
            }
            let names = node_names(c).await?;
            println!("id:        {}", j.spec.id);
            println!("state:     {:?}", j.state);
            println!("cmd:       {}", shell_words(&j.spec.cmd));
            println!(
                "submitted: {} ago by {}",
                ms_ago(j.spec.submitted_at_ms),
                j.spec.submitted_by
            );
            if let Some(n) = &j.spec.node {
                println!("pinned:    {}", names.get(n).unwrap_or(n));
            } else if !j.spec.selector.is_empty() {
                println!("selector:  {}", j.spec.selector);
            }
            if let Some(pick) = &j.spec.pick {
                println!("placement: {pick}");
            }
            if !j.spec.after.is_empty() {
                println!(
                    "after:     {}",
                    j.spec
                        .after
                        .iter()
                        .map(|a| a[..a.len().min(8)].to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            if let Some(b) = &j.spec.batch {
                println!("batch:     {b}");
            }
            if j.spec.retries > 0 {
                println!("retries:   {} of {} used", j.spec.retry, j.spec.retries);
            }
            if let Some(r) = &j.spec.cancel_reason {
                println!("reason:    {r}");
            }
            if let Some(t) = &j.spec.tmux {
                println!("tmux:      {t}");
            }
            if let Some(cl) = &j.claim {
                let now = flotilla_core::now_ms();
                let lease = if cl.lease_until_ms == 0 {
                    "no lease".to_string()
                } else if cl.lease_until_ms > now {
                    format!("lease {}s left", (cl.lease_until_ms - now) / 1000)
                } else {
                    format!("lease expired {} ago", ms_ago(cl.lease_until_ms))
                };
                println!(
                    "claimed:   {} ({} ago, attempt {}, {})",
                    names.get(&cl.node).unwrap_or(&cl.node),
                    ms_ago(cl.claimed_at_ms),
                    cl.attempt,
                    lease
                );
            }
            println!("--- timeline ---");
            let ts = |ms: u64| {
                chrono::DateTime::<chrono::Local>::from(
                    std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms),
                )
                .format("%H:%M:%S%.3f")
                .to_string()
            };
            println!(
                "created   {}  by {}",
                ts(j.spec.submitted_at_ms),
                j.spec.submitted_by
            );
            if let Some(cl) = &j.claim {
                println!(
                    "claimed   {}  by {} (attempt {})",
                    ts(cl.claimed_at_ms),
                    names.get(&cl.node).unwrap_or(&cl.node),
                    cl.attempt
                );
                if let Some(st) = cl.started_at_ms {
                    println!("started   {}", ts(st));
                }
            }
            if let Some(r) = &j.result {
                println!(
                    "finished  {}  exit {:?}{}",
                    ts(r.finished_at_ms),
                    r.exit_code,
                    r.error
                        .as_ref()
                        .map(|e| format!(" ({e})"))
                        .unwrap_or_default()
                );
            }
            if j.spec.cancelled {
                println!("cancelled (request replicated; see result for outcome)");
            }
            if let Some(r) = &j.result {
                println!(
                    "exit:      {:?}{}",
                    r.exit_code,
                    r.error
                        .as_ref()
                        .map(|e| format!(" ({e})"))
                        .unwrap_or_default()
                );
                println!(
                    "duration:  {}s",
                    (r.finished_at_ms.saturating_sub(r.started_at_ms)) / 1000
                );
                if !r.output_tail.is_empty() {
                    println!("--- output tail ---");
                    print!("{}", r.output_tail);
                    if !r.output_tail.ends_with('\n') {
                        println!();
                    }
                }
            }
            Ok(())
        }
        JobCmd::Logs { id } => {
            let id = resolve_job(c, &id).await?;
            let st = c.status().await?;
            let claim: JobClaim = c
                .get_record(&keys::claim(&id))
                .await?
                .ok_or_else(|| anyhow!("job not claimed yet"))?
                .parse()?;
            let node = st
                .nodes
                .iter()
                .find(|n| n.facts.node_id == claim.node)
                .ok_or_else(|| anyhow!("executor {} unknown", claim.node))?;
            if !node.online {
                bail!(
                    "executor {} is offline; try `job show` for the output tail",
                    node.facts.name
                );
            }
            let client = node_url(c, &st, node)?;
            match client.job_log(&id).await? {
                Some(text) => {
                    print!("{text}");
                    Ok(())
                }
                None => bail!("{} has no log for this job", node.facts.name),
            }
        }
        JobCmd::Wait { id } => {
            let id = resolve_job(c, &id).await?;
            wait(c, &id, json).await
        }
        JobCmd::Cancel { id } => {
            let id = resolve_job(c, &id).await?;
            let mut spec: JobSpec = c
                .get_record(&keys::job(&id))
                .await?
                .ok_or_else(|| anyhow!("no such job"))?
                .parse()?;
            spec.cancelled = true;
            c.put_json(&keys::job(&id), &spec).await?;
            println!("cancelled {id}");
            Ok(())
        }
        JobCmd::Rm { id } => {
            let id = resolve_job(c, &id).await?;
            for k in [keys::job(&id), keys::claim(&id), keys::result(&id)] {
                c.delete_record(&k).await?;
            }
            println!("removed {id}");
            Ok(())
        }
    }
}

async fn job_ls_once(c: &Client, hours: u64, json: bool) -> Result<()> {
    let names = node_names(c).await?;
    let cutoff = flotilla_core::now_ms().saturating_sub(hours * 3_600_000);
    let jobs: Vec<JobView> = load_jobs(c)
        .await?
        .into_iter()
        .filter(|j| j.result.is_none() || j.result.as_ref().unwrap().finished_at_ms >= cutoff)
        .collect();
    if json {
        let v: Vec<serde_json::Value> = jobs.iter().map(|j| serde_json::json!({"spec": j.spec, "claim": j.claim, "result": j.result, "state": j.state})).collect();
        return print_json(&v);
    }
    let mut t = Table::new();
    t.load_preset(UTF8_FULL_CONDENSED);
    t.set_header([
        "id",
        "state",
        "node",
        "exit",
        "submitted",
        "by",
        "target",
        "cmd",
    ]);
    for j in jobs {
        let state = match j.state {
            JobState::Succeeded => Cell::new("succeeded").fg(Color::Green),
            JobState::Failed => Cell::new("failed").fg(Color::Red),
            JobState::Running => Cell::new("running").fg(Color::Yellow),
            JobState::Claimed => Cell::new("claimed").fg(Color::Yellow),
            JobState::Lost => Cell::new("lost").fg(Color::Red),
            JobState::Queued => Cell::new("queued"),
            JobState::Cancelled => Cell::new("cancelled").fg(Color::DarkGrey),
        };
        let node = j
            .claim
            .as_ref()
            .map(|c| {
                names
                    .get(&c.node)
                    .cloned()
                    .unwrap_or_else(|| c.node.clone())
            })
            .unwrap_or_default();
        let exit = j
            .result
            .as_ref()
            .map(|r| {
                r.exit_code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| r.error.clone().unwrap_or("?".into()))
            })
            .unwrap_or_default();
        let target = j
            .spec
            .node
            .as_ref()
            .map(|n| names.get(n).cloned().unwrap_or_else(|| n.clone()))
            .unwrap_or_else(|| j.spec.selector.to_string());
        t.add_row(vec![
            Cell::new(&j.spec.id[..8]),
            state,
            Cell::new(node),
            Cell::new(exit),
            Cell::new(ms_ago(j.spec.submitted_at_ms)),
            Cell::new(&j.spec.submitted_by),
            Cell::new(target),
            Cell::new(shell_words(&j.spec.cmd)),
        ]);
    }
    println!("{t}");
    Ok(())
}

async fn wait(c: &Client, id: &str, json: bool) -> Result<()> {
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
        if last_state != Some(state) && !json {
            eprintln!("{id}: {state:?}");
            last_state = Some(state);
        }
        if let Some(r) = result {
            if json {
                print_json(&r)?;
            } else {
                print!("{}", r.output_tail);
                if !r.output_tail.ends_with('\n') && !r.output_tail.is_empty() {
                    println!();
                }
                if let Some(e) = &r.error {
                    eprintln!("error: {e}");
                }
            }
            // process::exit skips buffered-stdout flushing
            let _ = std::io::stdout().flush();
            std::process::exit(r.exit_code.unwrap_or(1));
        }
        if state == JobState::Cancelled && result.is_none() {
            if claim.is_some() && cancelled_polls < 15 {
                // the executor will write a result once it has killed the process
                cancelled_polls += 1;
            } else {
                bail!("job cancelled before it ran");
            }
        }
        pause(Duration::from_secs(2)).await;
    }
}

fn shell_words(cmd: &[String]) -> String {
    cmd.iter()
        .map(|w| {
            if w.contains(' ') {
                format!("{w:?}")
            } else {
                w.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum DesiredCmd {
    /// Set desired state for a node from a TOML or JSON file
    Set {
        node: String,
        #[arg(short, long)]
        file: std::path::PathBuf,
    },
    /// Print desired state for a node
    Get { node: String },
    /// Remove desired state for a node
    Rm { node: String },
    /// Show the last reconcile report for every node
    Status,
}

async fn resolve_node(c: &Client, name: &str) -> Result<(String, String)> {
    let st = c.status().await?;
    st.nodes
        .iter()
        .find(|n| n.facts.name == name || n.facts.node_id == name)
        .map(|n| (n.facts.node_id.clone(), n.facts.name.clone()))
        .ok_or_else(|| anyhow!("unknown node {name:?}"))
}

pub async fn desired(c: &Client, cmd: DesiredCmd, json: bool) -> Result<()> {
    match cmd {
        DesiredCmd::Set { node, file } => {
            let (id, name) = resolve_node(c, &node).await?;
            let text = std::fs::read_to_string(&file)
                .with_context(|| format!("reading {}", file.display()))?;
            let ds: DesiredState = if file.extension().map(|e| e == "json").unwrap_or(false) {
                serde_json::from_str(&text)?
            } else {
                toml::from_str(&text)?
            };
            let rec = c.put_json(&keys::desired(&id), &ds).await?;
            println!(
                "desired state for {name} set ({} files, {} ensures) at {}",
                ds.files.len(),
                ds.ensure.len(),
                rec.hlc
            );
            Ok(())
        }
        DesiredCmd::Get { node } => {
            let (id, _) = resolve_node(c, &node).await?;
            match c.get_record(&keys::desired(&id)).await? {
                Some(r) => {
                    let ds: DesiredState = r.parse()?;
                    if json {
                        print_json(&ds)
                    } else {
                        print!("{}", toml::to_string_pretty(&ds)?);
                        Ok(())
                    }
                }
                None => bail!("no desired state for {node}"),
            }
        }
        DesiredCmd::Rm { node } => {
            let (id, name) = resolve_node(c, &node).await?;
            c.delete_record(&keys::desired(&id)).await?;
            println!("removed desired state for {name}");
            Ok(())
        }
        DesiredCmd::Status => {
            let names = node_names(c).await?;
            let reports: Vec<ReconcileReport> = c
                .list(keys::RECONCILE, false)
                .await?
                .into_iter()
                .filter_map(|r| r.parse().ok())
                .collect();
            if json {
                return print_json(&reports);
            }
            let mut t = Table::new();
            t.load_preset(UTF8_FULL_CONDENSED);
            t.set_header(["node", "converged", "last run", "changes", "errors"]);
            for r in reports {
                t.add_row([
                    names.get(&r.node).cloned().unwrap_or(r.node.clone()),
                    if r.converged {
                        "yes".into()
                    } else {
                        "NO".into()
                    },
                    ms_ago(r.at_ms) + " ago",
                    r.changes.join("; "),
                    r.errors.join("; "),
                ]);
            }
            println!("{t}");
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum RecordsCmd {
    /// List records under a prefix
    Ls {
        #[arg(default_value = "")]
        prefix: String,
        /// Include tombstones
        #[arg(long)]
        raw: bool,
    },
    Get {
        key: String,
    },
    /// Write a record with a JSON value
    Put {
        key: String,
        json: String,
    },
    Rm {
        key: String,
    },
}

pub async fn records(c: &Client, cmd: RecordsCmd) -> Result<()> {
    match cmd {
        RecordsCmd::Ls { prefix, raw } => {
            for r in c.list(&prefix, raw).await? {
                let v = serde_json::to_string(&r.value)?;
                let v = if v.len() > 100 {
                    format!(
                        "{}…",
                        &v[..v.char_indices().nth(99).map(|(i, _)| i).unwrap_or(v.len())]
                    )
                } else {
                    v
                };
                println!(
                    "{}{}\t{}@{}\t{}",
                    if r.deleted { "(deleted) " } else { "" },
                    r.key,
                    r.author,
                    r.hlc,
                    v
                );
            }
            Ok(())
        }
        RecordsCmd::Get { key } => match c.get_record(&key).await? {
            Some(r) => print_json(&r),
            None => bail!("no record {key}"),
        },
        RecordsCmd::Put { key, json } => {
            let v: serde_json::Value = serde_json::from_str(&json).context("value must be JSON")?;
            print_json(&c.put_record(&key, v).await?)
        }
        RecordsCmd::Rm { key } => match c.delete_record(&key).await? {
            Some(_) => {
                println!("deleted {key}");
                Ok(())
            }
            None => bail!("no record {key}"),
        },
    }
}

// ---------------------------------------------------------------------------

#[derive(Args, Debug)]
pub struct PushArgs {
    #[command(flatten)]
    target: TargetArgs,
    /// Octal mode on the destination, e.g. 0755
    #[arg(long)]
    mode: Option<String>,
    /// Local file
    src: std::path::PathBuf,
    /// Destination path on each node (absolute or ~/...)
    dst: String,
}

fn sha256_file(p: &std::path::Path) -> Result<String> {
    use sha2::Digest;
    let mut f = std::fs::File::open(p).with_context(|| format!("opening {}", p.display()))?;
    let mut h = sha2::Sha256::new();
    std::io::copy(&mut f, &mut h)?;
    Ok(hex::encode(h.finalize()))
}

pub async fn push(c: &Client, args: PushArgs) -> Result<()> {
    let st = c.status().await?;
    let targets: Vec<NodeStatus> = args
        .target
        .pick(&st)?
        .into_iter()
        .filter(|n| n.online)
        .collect();
    if targets.is_empty() {
        bail!("no online targets");
    }
    let local_sha = sha256_file(&args.src)?;
    let mut failed = false;
    for n in targets {
        let client = node_url(c, &st, &n)?;
        match client
            .put_file(&args.src, &args.dst, args.mode.as_deref())
            .await
        {
            Ok(r) if r.sha256 == local_sha => {
                println!("[{}] {} ({} bytes)", n.facts.name, r.path, r.bytes)
            }
            Ok(r) => {
                failed = true;
                eprintln!(
                    "[{}] checksum mismatch after upload: {} != {}",
                    n.facts.name, r.sha256, local_sha
                );
            }
            Err(e) => {
                failed = true;
                eprintln!("[{}] error: {e:#}", n.facts.name);
            }
        }
    }
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

#[derive(Args, Debug)]
pub struct PullArgs {
    /// Node name or id
    node: String,
    /// Path on the node
    src: String,
    /// Local destination (default: the basename in the current directory)
    dst: Option<std::path::PathBuf>,
}

pub async fn pull(c: &Client, args: PullArgs) -> Result<()> {
    let st = c.status().await?;
    let n = st
        .nodes
        .iter()
        .find(|n| n.facts.name == args.node || n.facts.node_id == args.node)
        .ok_or_else(|| anyhow!("unknown node {:?}", args.node))?;
    if !n.online {
        bail!("{} is offline", n.facts.name);
    }
    let dst = args.dst.unwrap_or_else(|| {
        std::path::PathBuf::from(
            std::path::Path::new(&args.src)
                .file_name()
                .map(|s| s.to_os_string())
                .unwrap_or_else(|| "download".into()),
        )
    });
    let client = node_url(c, &st, n)?;
    let bytes = client.get_file(&args.src, &dst).await?;
    println!("{} bytes -> {}", bytes, dst.display());
    Ok(())
}

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    #[command(flatten)]
    target: TargetArgs,
    /// Push this machine's flotilla/flotillad binaries instead of fetching a release.
    /// Only nodes with the same OS and architecture are upgraded.
    #[arg(long)]
    local: bool,
    /// Directory holding flotilla and flotillad for --local (default: this binary's directory)
    #[arg(long)]
    from_dir: Option<std::path::PathBuf>,
    /// Release tag to install (default: latest)
    #[arg(long)]
    version: Option<String>,
    /// Also upgrade this node, last
    #[arg(long)]
    include_self: bool,
}

const INSTALL_SCRIPT_URL: &str =
    "https://raw.githubusercontent.com/haasonsaas/flotilla/main/scripts/install.sh";

pub async fn upgrade(c: &Client, args: UpgradeArgs) -> Result<()> {
    let st = c.status().await?;
    let mut targets: Vec<NodeStatus> = args
        .target
        .pick(&st)?
        .into_iter()
        .filter(|n| n.online)
        .collect();
    let me_idx = targets.iter().position(|n| n.facts.node_id == st.me);
    let me_node = me_idx.map(|i| targets.remove(i));
    if targets.is_empty() && (me_node.is_none() || !args.include_self) {
        bail!("no online targets");
    }
    let from_dir = match &args.from_dir {
        Some(d) => d.clone(),
        None => std::env::current_exe()?
            .parent()
            .ok_or_else(|| anyhow!("no parent dir"))?
            .to_path_buf(),
    };
    let local_os = std::env::consts::OS;
    let local_arch = std::env::consts::ARCH;
    let mut failed = false;

    for n in targets {
        let name = n.facts.name.clone();
        let old_version = n.facts.version.clone();
        let client = node_url(c, &st, &n)?;
        let outcome: Result<()> = async {
            if args.local {
                let node_os = n.facts.labels.get("os").cloned().unwrap_or_default();
                if node_os != local_os || n.facts.arch != local_arch {
                    bail!("skipped: {node_os}/{} does not match this machine's {local_os}/{local_arch}; use a release instead", n.facts.arch);
                }
                if n.facts.exe_path.is_empty() {
                    bail!("node has not reported its executable path yet (old version?)");
                }
                let dir = std::path::Path::new(&n.facts.exe_path).parent().ok_or_else(|| anyhow!("bad exe_path"))?.to_string_lossy().into_owned();
                for b in ["flotillad", "flotilla"] {
                    let src = from_dir.join(b);
                    let sha = sha256_file(&src)?;
                    let r = client.put_file(&src, &format!("{dir}/{b}.new"), Some("0755")).await?;
                    if r.sha256 != sha {
                        bail!("checksum mismatch uploading {b}");
                    }
                }
                let script = format!("mv -f '{dir}/flotillad.new' '{dir}/flotillad' && mv -f '{dir}/flotilla.new' '{dir}/flotilla' && exec '{dir}/flotilla' install");
                run_remote_install(&client, &name, script).await
            } else {
                let mut env = BTreeMap::new();
                if let Some(v) = &args.version {
                    env.insert("FLOTILLA_VERSION".to_string(), v.clone());
                }
                if !n.facts.exe_path.is_empty() {
                    if let Some(dir) = std::path::Path::new(&n.facts.exe_path).parent() {
                        env.insert("FLOTILLA_BIN_DIR".to_string(), dir.to_string_lossy().into_owned());
                    }
                }
                let script = format!("curl -fsSL {INSTALL_SCRIPT_URL} | sh");
                run_remote_install_env(&client, &name, script, env).await
            }
        }
        .await;
        match outcome {
            Ok(()) => {
                let new_version = wait_for_version(&client, &old_version).await;
                println!(
                    "[{name}] {old_version} -> {}",
                    new_version.unwrap_or_else(|| "unknown (daemon not back yet)".into())
                );
            }
            Err(e) => {
                failed = true;
                eprintln!("[{name}] {e:#}");
            }
        }
    }

    if let (Some(me), true) = (me_node, args.include_self) {
        let name = me.facts.name.clone();
        if args.local {
            let dir = std::env::current_exe()?.parent().unwrap().to_path_buf();
            if from_dir != dir {
                for b in ["flotillad", "flotilla"] {
                    let tmp = dir.join(format!("{b}.new"));
                    std::fs::copy(from_dir.join(b), &tmp)?;
                    std::fs::rename(&tmp, dir.join(b))?;
                }
            }
            crate::install::install(crate::install::InstallArgs { daemon_path: None }).await?;
            println!("[{name}] reinstalled from {}", dir.display());
        } else {
            let mut cmd = std::process::Command::new("sh");
            cmd.arg("-c")
                .arg(format!("curl -fsSL {INSTALL_SCRIPT_URL} | sh"));
            if let Some(v) = &args.version {
                cmd.env("FLOTILLA_VERSION", v);
            }
            let status = cmd.status()?;
            if !status.success() {
                failed = true;
                eprintln!("[{name}] install script failed: {status}");
            }
        }
    }
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_remote_install(client: &Client, name: &str, script: String) -> Result<()> {
    run_remote_install_env(client, name, script, BTreeMap::new()).await
}

/// Run the install step over exec. The daemon restarts itself part-way
/// through, which drops our stream; that is expected, so a broken stream
/// after output started counts as success and `wait_for_version` verifies.
async fn run_remote_install_env(
    client: &Client,
    name: &str,
    script: String,
    env: BTreeMap<String, String>,
) -> Result<()> {
    let req = ExecRequest {
        cmd: vec!["sh".into(), "-c".into(), script],
        cwd: None,
        env,
        timeout_secs: Some(600),
    };
    let mut frames = client.exec(&req).await?;
    let mut saw_output = false;
    while let Some(f) = frames.next().await {
        match f {
            Ok(ExecFrame::Stdout { data }) | Ok(ExecFrame::Stderr { data }) => {
                saw_output = true;
                eprint!("[{name}] {data}");
            }
            Ok(ExecFrame::Exit { code: Some(0) }) => return Ok(()),
            // The daemon restarts itself during install, which ends the
            // exec without a status; `wait_for_version` verifies the result.
            Ok(ExecFrame::Exit { code: None }) if saw_output => return Ok(()),
            Ok(ExecFrame::Exit { code }) => bail!("install exited with {code:?}"),
            Ok(ExecFrame::Error { message }) => bail!("install error: {message}"),
            Ok(ExecFrame::Keepalive) => {}
            Err(_) if saw_output => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

async fn wait_for_version(client: &Client, old: &str) -> Option<String> {
    for _ in 0..30 {
        pause(Duration::from_secs(1)).await;
        if let Ok(me) = client.me().await {
            if me.version != old {
                return Some(me.version);
            }
        }
    }
    client.me().await.ok().map(|m| m.version)
}

// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum SessionCmd {
    /// List tmux sessions across the fleet (from each node's facts)
    Ls {
        /// Only this node
        #[arg(short = 'n', long = "node")]
        node: Option<String>,
    },
    /// Start a detached tmux session on a node, optionally running a command
    Start {
        #[arg(short = 'n', long = "node")]
        node: String,
        /// Session name
        #[arg(long)]
        name: String,
        /// Working directory on the node
        #[arg(long)]
        cwd: Option<String>,
        /// Environment KEY=VALUE for the session (repeatable)
        #[arg(short = 'e', long = "env")]
        env: Vec<String>,
        /// Command to run in the session (default: the login shell)
        #[arg(last = true)]
        cmd: Vec<String>,
    },
    /// Type text into a session (followed by Enter unless --no-enter)
    Send {
        #[arg(short = 'n', long = "node")]
        node: String,
        name: String,
        #[arg(long)]
        no_enter: bool,
        #[arg(required = true, last = true)]
        text: Vec<String>,
    },
    /// Print the last lines of a session's active pane
    Tail {
        #[arg(short = 'n', long = "node")]
        node: String,
        name: String,
        #[arg(long, default_value_t = 50)]
        lines: u32,
    },
    /// Kill a session
    Kill {
        #[arg(short = 'n', long = "node")]
        node: String,
        name: String,
    },
    /// Attach interactively over SSH (prints the command if ssh fails)
    Attach {
        #[arg(short = 'n', long = "node")]
        node: String,
        name: String,
        /// SSH user on the node (default: same as here)
        #[arg(long)]
        user: Option<String>,
    },
}

async fn find_node(c: &Client, name: &str) -> Result<(StatusResponse, NodeStatus)> {
    let st = c.status().await?;
    let n = st
        .nodes
        .iter()
        .find(|n| n.facts.name == name || n.facts.node_id == name)
        .cloned()
        .ok_or_else(|| anyhow!("unknown node {name:?}"))?;
    if !n.online {
        bail!("{} is offline", n.facts.name);
    }
    Ok((st, n))
}

/// Run a command on a node and collect its output.
async fn exec_collect(
    client: &Client,
    cmd: Vec<String>,
    env: BTreeMap<String, String>,
) -> Result<(Option<i32>, String, String)> {
    let req = ExecRequest {
        cmd,
        cwd: None,
        env,
        timeout_secs: Some(60),
    };
    let mut frames = client.exec(&req).await?;
    let (mut out, mut err, mut code) = (String::new(), String::new(), None);
    while let Some(f) = frames.next().await {
        match f? {
            ExecFrame::Stdout { data } => out.push_str(&data),
            ExecFrame::Stderr { data } => err.push_str(&data),
            ExecFrame::Error { message } => err.push_str(&message),
            ExecFrame::Exit { code: c } => code = c,
            ExecFrame::Keepalive => {}
        }
    }
    Ok((code, out, err))
}

async fn tmux(client: &Client, args: &[&str]) -> Result<String> {
    let mut cmd = vec!["tmux".to_string()];
    cmd.extend(args.iter().map(|s| s.to_string()));
    let (code, out, err) = exec_collect(client, cmd, BTreeMap::new()).await?;
    if code != Some(0) {
        bail!(
            "tmux {} failed: {}",
            args.first().unwrap_or(&""),
            err.trim()
        );
    }
    Ok(out)
}

pub async fn session(c: &Client, cmd: SessionCmd, json: bool) -> Result<()> {
    match cmd {
        SessionCmd::Ls { node } => {
            let st = c.status().await?;
            let mut rows = Vec::new();
            for n in &st.nodes {
                if let Some(want) = &node {
                    if &n.facts.name != want && &n.facts.node_id != want {
                        continue;
                    }
                }
                for s in &n.facts.sessions {
                    rows.push((n.facts.name.clone(), n.online, s.clone()));
                }
            }
            if json {
                let v: Vec<serde_json::Value> = rows.iter().map(|(node, online, s)| serde_json::json!({"node": node, "online": online, "session": s})).collect();
                return print_json(&v);
            }
            let mut t = Table::new();
            t.load_preset(UTF8_FULL_CONDENSED);
            t.set_header([
                "node", "session", "running", "windows", "attached", "cwd", "age",
            ]);
            for (node, online, s) in rows {
                t.add_row(vec![
                    if online {
                        Cell::new(node)
                    } else {
                        Cell::new(node).fg(Color::DarkGrey)
                    },
                    Cell::new(&s.name),
                    Cell::new(&s.command),
                    Cell::new(s.windows),
                    Cell::new(if s.attached { "yes" } else { "" }),
                    Cell::new(&s.cwd),
                    Cell::new(ms_ago(s.created_ms)),
                ]);
            }
            println!("{t}");
            Ok(())
        }
        SessionCmd::Start {
            node,
            name,
            cwd,
            env,
            cmd,
        } => {
            let (st, n) = find_node(c, &node).await?;
            let client = node_url(c, &st, &n)?;
            let mut args: Vec<String> =
                vec!["new-session".into(), "-d".into(), "-s".into(), name.clone()];
            if let Some(d) = &cwd {
                args.push("-c".into());
                args.push(d.clone());
            }
            for pair in parse_env(&env)? {
                args.push("-e".into());
                args.push(format!("{}={}", pair.0, pair.1));
            }
            if !cmd.is_empty() {
                args.push("--".into());
                args.extend(cmd);
            }
            let mut full = vec!["tmux".to_string()];
            full.extend(args);
            let (code, _, err) = exec_collect(&client, full, BTreeMap::new()).await?;
            if code != Some(0) {
                bail!(
                    "tmux new-session failed on {}: {}",
                    n.facts.name,
                    err.trim()
                );
            }
            println!("started session {name} on {}", n.facts.name);
            Ok(())
        }
        SessionCmd::Send {
            node,
            name,
            no_enter,
            text,
        } => {
            let (st, n) = find_node(c, &node).await?;
            let client = node_url(c, &st, &n)?;
            let line = text.join(" ");
            let mut args = vec!["send-keys", "-t", name.as_str(), "--", line.as_str()];
            if !no_enter {
                args.push("Enter");
            }
            tmux(&client, &args).await?;
            Ok(())
        }
        SessionCmd::Tail { node, name, lines } => {
            let (st, n) = find_node(c, &node).await?;
            let client = node_url(c, &st, &n)?;
            let start = format!("-{lines}");
            let out = tmux(
                &client,
                &[
                    "capture-pane",
                    "-p",
                    "-t",
                    name.as_str(),
                    "-S",
                    start.as_str(),
                ],
            )
            .await?;
            let trimmed = out.trim_end_matches('\n');
            println!("{trimmed}");
            Ok(())
        }
        SessionCmd::Kill { node, name } => {
            let (st, n) = find_node(c, &node).await?;
            let client = node_url(c, &st, &n)?;
            tmux(&client, &["kill-session", "-t", name.as_str()]).await?;
            println!("killed session {name} on {}", n.facts.name);
            Ok(())
        }
        SessionCmd::Attach { node, name, user } => {
            let (_, n) = find_node(c, &node).await?;
            let ip = n
                .facts
                .tailscale_ips
                .iter()
                .find(|s| !s.contains(':'))
                .or(n.facts.tailscale_ips.first())
                .ok_or_else(|| anyhow!("node has no address"))?;
            let target = match user {
                Some(u) => format!("{u}@{ip}"),
                None => ip.clone(),
            };
            let status = std::process::Command::new("ssh")
                .args(["-t", &target, "tmux", "attach", "-t", &name])
                .status();
            match status {
                Ok(s) if s.success() => Ok(()),
                _ => bail!("could not attach; try: ssh -t {target} tmux attach -t {name}"),
            }
        }
    }
}

// ---------------------------------------------------------------------------

/// Wake a node by MAC. The magic packet has to originate on the target's LAN,
/// so it is sent from this daemon and from every online node whose last
/// known LAN subnet matches the target's.
pub async fn wake(c: &Client, node: &str) -> Result<()> {
    let st = c.status().await?;
    let target = st
        .nodes
        .iter()
        .find(|n| n.facts.name == node || n.facts.node_id == node)
        .ok_or_else(|| anyhow!("unknown node {node:?}"))?;
    if target.facts.lan.is_empty() {
        bail!("{} never reported a LAN interface (needs a build with wake support to have run there once)", target.facts.name);
    }
    let same_subnet = |a: &str, ap: u8, b: &str| -> bool {
        match (
            a.parse::<std::net::Ipv4Addr>(),
            b.parse::<std::net::Ipv4Addr>(),
        ) {
            (Ok(a), Ok(b)) => {
                let mask = if ap == 0 {
                    0
                } else {
                    u32::MAX << (32 - ap as u32)
                };
                u32::from(a) & mask == u32::from(b) & mask
            }
            _ => false,
        }
    };
    let mut senders: Vec<(String, Client)> = vec![("local".into(), c.clone())];
    for n in st
        .nodes
        .iter()
        .filter(|n| n.online && n.facts.node_id != st.me && n.facts.node_id != target.facts.node_id)
    {
        let on_lan = target.facts.lan.iter().any(|t| {
            n.facts
                .lan
                .iter()
                .any(|h| same_subnet(&t.ip, t.prefix, &h.ip))
        });
        if on_lan {
            if let Ok(client) = node_url(c, &st, n) {
                senders.push((n.facts.name.clone(), client));
            }
        }
    }
    let mut any = false;
    for iface in &target.facts.lan {
        let req = WakeRequest {
            mac: iface.mac.clone(),
            targets: vec![iface.ip.clone()],
        };
        for (name, client) in &senders {
            match client.wake(&req).await {
                Ok(r) => {
                    any = true;
                    println!(
                        "[{name}] sent for {} ({}) via {}",
                        iface.mac,
                        iface.name,
                        r.sent_to.join(", ")
                    );
                }
                Err(e) => eprintln!("[{name}] {e:#}"),
            }
        }
    }
    if !any {
        bail!("no packet was sent");
    }
    println!("sent; {} should show online in `flotilla status` within a minute if wake-on-LAN is enabled on it", target.facts.name);
    Ok(())
}

// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum AgentCmd {
    /// Run an agent command as a job inside a tmux session on the fleet
    Run(AgentRunArgs),
    /// List agent runs across the fleet
    Ls,
    /// Full log of an agent run (from the node that ran it)
    Logs { id: String },
    /// Attach to a running agent's tmux session over SSH
    Attach {
        id: String,
        #[arg(long)]
        user: Option<String>,
    },
    /// Stop an agent run
    Stop { id: String },
}

#[derive(Args, Debug)]
pub struct AgentRunArgs {
    /// Pin to one node by name or id
    #[arg(short = 'n', long = "node")]
    node: Option<String>,
    /// Only nodes whose labels match may claim it, e.g. os=linux
    #[arg(short = 'l', long = "selector")]
    selector: Option<Selector>,
    /// Placement (default least-load): least-load | any
    #[arg(long, default_value = "least-load")]
    pick: String,
    /// tmux session name on the executor (default: agent-<id prefix>)
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    cwd: Option<String>,
    #[arg(short = 'e', long = "env")]
    env: Vec<String>,
    #[arg(long)]
    timeout: Option<u64>,
    /// Wait for completion and print the output tail
    #[arg(long)]
    wait: bool,
    #[arg(required = true, last = true)]
    cmd: Vec<String>,
}

pub async fn agent(c: &Client, cmd: AgentCmd, json: bool) -> Result<()> {
    match cmd {
        AgentCmd::Run(a) => {
            let me = c.me().await?;
            let node = match &a.node {
                Some(n) => Some(resolve_node(c, n).await?.0),
                None => None,
            };
            let id = uuid::Uuid::new_v4().to_string();
            let name = a
                .name
                .clone()
                .unwrap_or_else(|| format!("agent-{}", &id[..8]));
            let spec = JobSpec {
                id: id.clone(),
                cmd: a.cmd,
                cwd: a.cwd,
                env: parse_env(&a.env)?,
                selector: a.selector.unwrap_or_default(),
                node,
                submitted_by: me.name,
                submitted_at_ms: flotilla_core::now_ms(),
                timeout_secs: a.timeout,
                cancelled: false,
                pick: if a.pick == "any" { None } else { Some(a.pick) },
                tmux: Some(name.clone()),
                kind: Some("agent".into()),
                after: Vec::new(),
                batch: None,
                retries: 0,
                retry: 0,
                cancel_reason: None,
            };
            c.put_json(&keys::job(&id), &spec).await?;
            if json && !a.wait {
                return print_json(&spec);
            }
            println!(
                "{id}  (tmux session {name}; `flotilla agent attach {}` once claimed)",
                &id[..8]
            );
            if a.wait {
                return wait(c, &id, json).await;
            }
            Ok(())
        }
        AgentCmd::Ls => {
            let names = node_names(c).await?;
            let jobs: Vec<JobView> = load_jobs(c)
                .await?
                .into_iter()
                .filter(|j| j.spec.kind.as_deref() == Some("agent"))
                .collect();
            if json {
                let v: Vec<serde_json::Value> = jobs.iter().map(|j| serde_json::json!({"spec": j.spec, "claim": j.claim, "result": j.result, "state": j.state})).collect();
                return print_json(&v);
            }
            let st = c.status().await?;
            let mut t = Table::new();
            t.load_preset(UTF8_FULL_CONDENSED);
            t.set_header([
                "id",
                "state",
                "node",
                "session",
                "started",
                "elapsed",
                "last line",
                "cmd",
            ]);
            for j in jobs {
                let node_id = j.claim.as_ref().map(|c| c.node.clone());
                let node = node_id
                    .as_ref()
                    .map(|n| names.get(n).cloned().unwrap_or_else(|| n.clone()))
                    .unwrap_or_default();
                let (started, elapsed) = match (&j.claim, &j.result) {
                    (Some(cl), Some(r)) => (
                        ms_ago(cl.claimed_at_ms) + " ago",
                        format!(
                            "{}s",
                            r.finished_at_ms.saturating_sub(r.started_at_ms) / 1000
                        ),
                    ),
                    (Some(cl), None) => {
                        (ms_ago(cl.claimed_at_ms) + " ago", ms_ago(cl.claimed_at_ms))
                    }
                    _ => (String::new(), String::new()),
                };
                let last = match (&j.state, &j.result, &node_id) {
                    (_, Some(r), _) => r.output_tail.lines().last().unwrap_or("").to_string(),
                    (JobState::Running | JobState::Claimed, None, Some(nid)) => {
                        // live: peek at the session's pane on the executor
                        match st
                            .nodes
                            .iter()
                            .find(|n| &n.facts.node_id == nid)
                            .and_then(|n| node_url(c, &st, n).ok())
                        {
                            Some(client) => tmux(
                                &client,
                                &[
                                    "capture-pane",
                                    "-p",
                                    "-t",
                                    j.spec.tmux.as_deref().unwrap_or(""),
                                    "-S",
                                    "-5",
                                ],
                            )
                            .await
                            .ok()
                            .and_then(|o| {
                                o.lines()
                                    .rev()
                                    .find(|l| !l.trim().is_empty())
                                    .map(String::from)
                            })
                            .unwrap_or_default(),
                            None => String::new(),
                        }
                    }
                    _ => String::new(),
                };
                let state_cell = match j.state {
                    JobState::Succeeded => Cell::new("done").fg(Color::Green),
                    JobState::Failed => Cell::new("failed").fg(Color::Red),
                    JobState::Running => Cell::new("running").fg(Color::Yellow),
                    JobState::Claimed => Cell::new("claimed").fg(Color::Yellow),
                    JobState::Lost => Cell::new("lost").fg(Color::Red),
                    JobState::Queued => Cell::new("queued"),
                    JobState::Cancelled => Cell::new("stopped").fg(Color::DarkGrey),
                };
                let last: String = last.chars().take(60).collect();
                t.add_row(vec![
                    Cell::new(&j.spec.id[..8]),
                    state_cell,
                    Cell::new(node),
                    Cell::new(j.spec.tmux.clone().unwrap_or_default()),
                    Cell::new(started),
                    Cell::new(elapsed),
                    Cell::new(last),
                    Cell::new(
                        shell_words(&j.spec.cmd)
                            .chars()
                            .take(50)
                            .collect::<String>(),
                    ),
                ]);
            }
            println!("{t}");
            Ok(())
        }
        AgentCmd::Logs { id } => job(c, JobCmd::Logs { id }, json).await,
        AgentCmd::Attach { id, user } => {
            let id = resolve_job(c, &id).await?;
            let spec: JobSpec = c
                .get_record(&keys::job(&id))
                .await?
                .ok_or_else(|| anyhow!("no such job"))?
                .parse()?;
            let claim: JobClaim = c
                .get_record(&keys::claim(&id))
                .await?
                .ok_or_else(|| anyhow!("not claimed yet"))?
                .parse()?;
            let names = node_names(c).await?;
            let node = names
                .get(&claim.node)
                .cloned()
                .unwrap_or(claim.node.clone());
            let name = spec
                .tmux
                .ok_or_else(|| anyhow!("job has no tmux session"))?;
            session(c, SessionCmd::Attach { node, name, user }, json).await
        }
        AgentCmd::Stop { id } => job(c, JobCmd::Cancel { id }, json).await,
    }
}

async fn resolve_many(c: &Client, prefixes: &[String]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for p in prefixes {
        out.push(resolve_job(c, p).await?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum BatchCmd {
    /// Submit N shards of one command; `{i}` and `{n}` in the command are substituted
    Submit(BatchSubmitArgs),
    /// Summary of every batch: counts per state
    Ls,
    /// Jobs in one batch
    Show { batch: String },
    /// Wait until every job in the batch is terminal; exit 1 if any did not succeed
    Wait { batch: String },
    /// Cancel every non-terminal job in the batch
    Cancel { batch: String },
}

#[derive(Args, Debug)]
pub struct BatchSubmitArgs {
    /// Batch name (default: a short id)
    #[arg(long)]
    batch: Option<String>,
    /// Number of shards
    #[arg(short = 'n', long, default_value_t = 1)]
    shards: u32,
    #[arg(short = 'l', long = "selector")]
    selector: Option<Selector>,
    #[arg(long, default_value = "least-load")]
    pick: String,
    #[arg(long)]
    cwd: Option<String>,
    #[arg(short = 'e', long = "env")]
    env: Vec<String>,
    #[arg(long)]
    timeout: Option<u64>,
    #[arg(long, default_value_t = 0)]
    retries: u32,
    /// A final command to run after every shard succeeded (same cwd/env)
    #[arg(long = "then")]
    then: Option<String>,
    /// Wait for the batch to finish
    #[arg(long)]
    wait: bool,
    #[arg(required = true, last = true)]
    cmd: Vec<String>,
}

fn batch_jobs<'a>(jobs: &'a [JobView], batch: &str) -> Vec<&'a JobView> {
    jobs.iter()
        .filter(|j| j.spec.batch.as_deref() == Some(batch))
        .collect()
}

pub async fn batch(c: &Client, cmd: BatchCmd, _json: bool) -> Result<()> {
    match cmd {
        BatchCmd::Submit(a) => {
            let me = c.me().await?;
            let name = a
                .batch
                .clone()
                .unwrap_or_else(|| format!("b-{}", &uuid::Uuid::new_v4().to_string()[..6]));
            let env = parse_env(&a.env)?;
            let mut ids = Vec::new();
            for i in 0..a.shards {
                let cmdv: Vec<String> = a
                    .cmd
                    .iter()
                    .map(|w| {
                        w.replace("{i}", &i.to_string())
                            .replace("{n}", &a.shards.to_string())
                    })
                    .collect();
                let spec = JobSpec {
                    id: uuid::Uuid::new_v4().to_string(),
                    cmd: cmdv,
                    cwd: a.cwd.clone(),
                    env: env.clone(),
                    selector: a.selector.clone().unwrap_or_default(),
                    node: None,
                    submitted_by: me.name.clone(),
                    submitted_at_ms: flotilla_core::now_ms(),
                    timeout_secs: a.timeout,
                    cancelled: false,
                    pick: if a.pick == "any" {
                        None
                    } else {
                        Some(a.pick.clone())
                    },
                    tmux: None,
                    kind: Some("shard".into()),
                    after: Vec::new(),
                    batch: Some(name.clone()),
                    retries: a.retries,
                    retry: 0,
                    cancel_reason: None,
                };
                c.put_json(&keys::job(&spec.id), &spec).await?;
                ids.push(spec.id);
            }
            if let Some(then) = &a.then {
                let spec = JobSpec {
                    id: uuid::Uuid::new_v4().to_string(),
                    cmd: vec!["sh".into(), "-c".into(), then.clone()],
                    cwd: a.cwd.clone(),
                    env: env.clone(),
                    selector: a.selector.clone().unwrap_or_default(),
                    node: None,
                    submitted_by: me.name.clone(),
                    submitted_at_ms: flotilla_core::now_ms(),
                    timeout_secs: a.timeout,
                    cancelled: false,
                    pick: if a.pick == "any" {
                        None
                    } else {
                        Some(a.pick.clone())
                    },
                    tmux: None,
                    kind: Some("then".into()),
                    after: ids.clone(),
                    batch: Some(name.clone()),
                    retries: 0,
                    retry: 0,
                    cancel_reason: None,
                };
                c.put_json(&keys::job(&spec.id), &spec).await?;
            }
            println!(
                "batch {name}: {} shards{}",
                a.shards,
                if a.then.is_some() {
                    " + 1 final job"
                } else {
                    ""
                }
            );
            if a.wait {
                return batch_wait(c, &name).await;
            }
            Ok(())
        }
        BatchCmd::Ls => {
            let jobs = load_jobs(c).await?;
            let mut names: Vec<String> = jobs.iter().filter_map(|j| j.spec.batch.clone()).collect();
            names.sort();
            names.dedup();
            let mut t = Table::new();
            t.load_preset(UTF8_FULL_CONDENSED);
            t.set_header([
                "batch",
                "jobs",
                "queued",
                "running",
                "succeeded",
                "failed",
                "cancelled",
                "lost",
                "newest",
            ]);
            for b in names {
                let js = batch_jobs(&jobs, &b);
                let count = |st: &[JobState]| js.iter().filter(|j| st.contains(&j.state)).count();
                let newest = js.iter().map(|j| j.spec.submitted_at_ms).max().unwrap_or(0);
                t.add_row(vec![
                    Cell::new(&b),
                    Cell::new(js.len()),
                    Cell::new(count(&[JobState::Queued])),
                    Cell::new(count(&[JobState::Claimed, JobState::Running])),
                    Cell::new(count(&[JobState::Succeeded])).fg(Color::Green),
                    Cell::new(count(&[JobState::Failed])).fg(Color::Red),
                    Cell::new(count(&[JobState::Cancelled])),
                    Cell::new(count(&[JobState::Lost])),
                    Cell::new(ms_ago(newest) + " ago"),
                ]);
            }
            println!("{t}");
            Ok(())
        }
        BatchCmd::Show { batch: b } => {
            let jobs = load_jobs(c).await?;
            let names = node_names(c).await?;
            let mut t = Table::new();
            t.load_preset(UTF8_FULL_CONDENSED);
            t.set_header(["id", "kind", "state", "node", "exit", "retries", "cmd"]);
            for j in batch_jobs(&jobs, &b) {
                let node = j
                    .claim
                    .as_ref()
                    .map(|c| {
                        names
                            .get(&c.node)
                            .cloned()
                            .unwrap_or_else(|| c.node.clone())
                    })
                    .unwrap_or_default();
                let exit = j
                    .result
                    .as_ref()
                    .map(|r| {
                        r.exit_code
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| r.error.clone().unwrap_or("?".into()))
                    })
                    .unwrap_or_default();
                t.add_row(vec![
                    Cell::new(&j.spec.id[..8]),
                    Cell::new(j.spec.kind.clone().unwrap_or_default()),
                    Cell::new(j.state.label()),
                    Cell::new(node),
                    Cell::new(exit),
                    Cell::new(format!("{}/{}", j.spec.retry, j.spec.retries)),
                    Cell::new(
                        shell_words(&j.spec.cmd)
                            .chars()
                            .take(60)
                            .collect::<String>(),
                    ),
                ]);
            }
            println!("{t}");
            Ok(())
        }
        BatchCmd::Wait { batch: b } => batch_wait(c, &b).await,
        BatchCmd::Cancel { batch: b } => {
            let jobs = load_jobs(c).await?;
            let mut n = 0;
            for j in batch_jobs(&jobs, &b) {
                if !j.state.is_terminal() {
                    let mut spec = j.spec.clone();
                    spec.cancelled = true;
                    c.put_json(&keys::job(&spec.id), &spec).await?;
                    n += 1;
                }
            }
            println!("cancelled {n} jobs in {b}");
            Ok(())
        }
    }
}

async fn batch_wait(c: &Client, b: &str) -> Result<()> {
    let mut last = String::new();
    loop {
        let jobs = load_jobs(c).await?;
        let js = batch_jobs(&jobs, b);
        if js.is_empty() {
            bail!("no jobs in batch {b}");
        }
        let summary: String = {
            let mut counts = std::collections::BTreeMap::new();
            for j in &js {
                *counts.entry(j.state.label()).or_insert(0) += 1;
            }
            counts
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(" ")
        };
        if summary != last {
            eprintln!("{b}: {summary}");
            last = summary;
        }
        if js.iter().all(|j| j.state.is_terminal()) {
            let ok = js.iter().all(|j| j.state == JobState::Succeeded);
            if !ok {
                std::process::exit(1);
            }
            return Ok(());
        }
        pause(Duration::from_secs(3)).await;
    }
}
