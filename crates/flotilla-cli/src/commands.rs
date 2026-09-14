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

pub async fn status(c: &Client, json: bool) -> Result<()> {
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
        }
    }
    Ok(code)
}

// ---------------------------------------------------------------------------

#[derive(Subcommand, Debug)]
pub enum JobCmd {
    /// Submit a job into the replicated store
    Submit(SubmitArgs),
    /// List jobs
    Ls {
        /// Include finished jobs older than this many hours (default 24)
        #[arg(long, default_value_t = 24)]
        hours: u64,
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
        let state = JobState::derive(&spec, claim.as_ref(), result.as_ref());
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
        JobCmd::Ls { hours } => {
            let names = node_names(c).await?;
            let cutoff = flotilla_core::now_ms().saturating_sub(hours * 3_600_000);
            let jobs: Vec<JobView> = load_jobs(c)
                .await?
                .into_iter()
                .filter(|j| {
                    j.result.is_none() || j.result.as_ref().unwrap().finished_at_ms >= cutoff
                })
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
                    JobState::Claimed => Cell::new("running").fg(Color::Yellow),
                    JobState::Pending => Cell::new("pending"),
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
            if let Some(cl) = &j.claim {
                println!(
                    "claimed:   {} ({} ago)",
                    names.get(&cl.node).unwrap_or(&cl.node),
                    ms_ago(cl.claimed_at_ms)
                );
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

async fn wait(c: &Client, id: &str, json: bool) -> Result<()> {
    let mut last_state = None;
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
        let state = JobState::derive(&spec, claim.as_ref(), result.as_ref());
        if last_state != Some(state) && !json {
            eprintln!("{id}: {state:?}");
            last_state = Some(state);
        }
        if let Some(r) = result {
            if json {
                print_json(&r)?;
            } else {
                print!("{}", r.output_tail);
                if let Some(e) = &r.error {
                    eprintln!("error: {e}");
                }
            }
            std::process::exit(r.exit_code.unwrap_or(1));
        }
        if state == JobState::Cancelled {
            bail!("job cancelled before it ran");
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
