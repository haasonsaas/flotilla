//! In-process integration tests: two daemons with static identity on
//! loopback, talking to each other exactly as they would over the tailnet.

use crate::config::{Config, StaticIdentityConfig, StaticPeer};
use crate::identity::IdentityProvider;
use crate::server::{pause, router, AppState};
use flotilla_core::api::*;
use flotilla_core::keys;
use flotilla_core::schema::*;
use flotilla_core::Store;
use futures::StreamExt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

struct Node {
    state: AppState,
    base: String,
    http: reqwest::Client,
}

async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn node(name: &str, port: u16, peers: Vec<(&str, u16)>, dir: &std::path::Path) -> Node {
    let cfg = Config {
        port,
        data_dir: dir.join(name),
        identity: "static".into(),
        sync_interval_secs: 1,
        job_lease_secs: 3,
        facts_interval_secs: 1,
        scheduler_interval_secs: 1,
        reconcile_interval_secs: 1,
        static_identity: Some(StaticIdentityConfig {
            node_id: format!("id-{name}"),
            name: name.into(),
            ips: vec![],
            login: "static@local".into(),
            peers: peers
                .into_iter()
                .map(|(n, p)| StaticPeer {
                    node_id: format!("id-{n}"),
                    name: n.into(),
                    ips: vec!["127.0.0.1".into()],
                    port: Some(p),
                })
                .collect(),
        }),
        ..Config::default()
    };
    std::fs::create_dir_all(cfg.jobs_dir()).unwrap();
    let identity = Arc::new(IdentityProvider::from_config(&cfg).unwrap());
    let me = identity.me().await.unwrap();
    let store =
        Arc::new(Store::open(&cfg.data_dir.join("store.redb"), me.node_id.clone()).unwrap());
    let state = AppState::new(Arc::new(cfg), identity, store, me);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let app = router(state.clone());
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    tokio::spawn(crate::facts::run(state.clone()));
    tokio::spawn(crate::sync_loop::run(state.clone()));
    tokio::spawn(crate::scheduler::run(state.clone()));
    tokio::spawn(crate::reconcile::run(state.clone()));
    Node {
        state,
        base: format!("http://127.0.0.1:{port}"),
        http: reqwest::Client::new(),
    }
}

async fn eventually<F: Fn() -> bool>(what: &str, f: F) {
    // 40s: the full suite runs many two-node daemons in parallel on CI.
    for _ in 0..200 {
        if f() {
            return;
        }
        pause(Duration::from_millis(200)).await;
    }
    panic!("timed out waiting for {what}");
}

fn tmp() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("flotilla-it-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_sync_facts_and_records() {
    let dir = tmp();
    let (pa, pb) = (free_port().await, free_port().await);
    let a = node("a", pa, vec![("b", pb)], &dir).await;
    let b = node("b", pb, vec![("a", pa)], &dir).await;

    // Facts from both nodes show up on both nodes.
    eventually("facts replicated", || {
        a.state
            .store
            .get(&keys::node_facts("id-b"))
            .unwrap()
            .is_some()
            && b.state
                .store
                .get(&keys::node_facts("id-a"))
                .unwrap()
                .is_some()
    })
    .await;

    // A record written through A's HTTP API reaches B.
    let r: flotilla_core::Record = a
        .http
        .put(format!("{}/v1/records/test/hello", a.base))
        .json(&PutRecordRequest {
            value: serde_json::json!({"x": 1}),
        })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r.author, "id-a");
    eventually("record replicated", || {
        b.state
            .store
            .get("test/hello")
            .unwrap()
            .map(|r| r.value == serde_json::json!({"x": 1}))
            .unwrap_or(false)
    })
    .await;

    // Status on B lists both nodes as online with labels.
    let st: StatusResponse = b
        .http
        .get(format!("{}/v1/status", b.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(st.nodes.len(), 2);
    assert!(st.nodes.iter().all(|n| n.online));
    assert_eq!(st.nodes[0].facts.labels["node"], "a");

    // Delete on B tombstones on A.
    b.http
        .delete(format!("{}/v1/records/test/hello", b.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    eventually("tombstone replicated", || {
        a.state.store.get("test/hello").unwrap().is_none()
    })
    .await;
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exec_streams_frames() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    let resp = a
        .http
        .post(format!("{}/v1/exec", a.base))
        .json(&ExecRequest {
            cmd: vec![
                "sh".into(),
                "-c".into(),
                "echo one; echo two 1>&2; exit 4".into(),
            ],
            ..Default::default()
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let mut frames = Vec::new();
    let mut buf = Vec::new();
    let mut body = resp.bytes_stream();
    while let Some(chunk) = body.next().await {
        buf.extend_from_slice(&chunk.unwrap());
        while let Some(i) = buf.iter().position(|&c| c == b'\n') {
            let line: Vec<u8> = buf.drain(..=i).collect();
            frames.push(serde_json::from_slice::<ExecFrame>(&line[..line.len() - 1]).unwrap());
        }
    }
    assert!(frames.contains(&ExecFrame::Stdout {
        data: "one\n".into()
    }));
    assert!(frames.contains(&ExecFrame::Stderr {
        data: "two\n".into()
    }));
    assert_eq!(frames.last(), Some(&ExecFrame::Exit { code: Some(4) }));
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_is_claimed_run_on_selected_node_and_result_replicates() {
    let dir = tmp();
    let (pa, pb) = (free_port().await, free_port().await);
    let a = node("a", pa, vec![("b", pb)], &dir).await;
    let b = node("b", pb, vec![("a", pa)], &dir).await;
    eventually("facts replicated", || {
        a.state
            .store
            .get(&keys::node_facts("id-b"))
            .unwrap()
            .is_some()
    })
    .await;

    let id = uuid::Uuid::new_v4().to_string();
    let spec = JobSpec {
        id: id.clone(),
        cmd: vec![
            "sh".into(),
            "-c".into(),
            "echo hello from $(hostname); echo line2".into(),
        ],
        cwd: None,
        env: Default::default(),
        selector: "node=b".parse().unwrap(),
        node: None,
        submitted_by: "test".into(),
        submitted_at_ms: flotilla_core::now_ms(),
        timeout_secs: Some(30),
        cancelled: false,
    };
    a.state.store.put_json(&keys::job(&id), &spec).unwrap();

    eventually("result replicated to submitter", || {
        a.state.store.get(&keys::result(&id)).unwrap().is_some()
    })
    .await;
    let result: JobResult = a
        .state
        .store
        .get(&keys::result(&id))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(result.node, "id-b", "selector must route to b");
    assert_eq!(result.exit_code, Some(0));
    assert!(result.output_tail.contains("hello from"));
    assert!(result.output_tail.ends_with("line2\n"));
    let claim: JobClaim = a
        .state
        .store
        .get(&keys::claim(&id))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(claim.node, "id-b");
    assert!(a.state.store.get(&keys::result(&id)).unwrap().is_some());

    // Full log is served by the executor, not by the submitter.
    let log = b
        .http
        .get(format!("{}/v1/jobs/{id}/log", b.base))
        .send()
        .await
        .unwrap();
    assert_eq!(log.status(), 200);
    assert!(log.text().await.unwrap().contains("line2"));
    let missing = a
        .http
        .get(format!("{}/v1/jobs/{id}/log", a.base))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_cancel_kills_running_process() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    eventually("own facts", || a.state.my_facts().is_some()).await;
    let id = uuid::Uuid::new_v4().to_string();
    let mut spec = JobSpec {
        id: id.clone(),
        cmd: vec!["tail".into(), "-f".into(), "/dev/null".into()],
        cwd: None,
        env: Default::default(),
        selector: Default::default(),
        node: Some("solo".into()),
        submitted_by: "test".into(),
        submitted_at_ms: flotilla_core::now_ms(),
        timeout_secs: None,
        cancelled: false,
    };
    a.state.store.put_json(&keys::job(&id), &spec).unwrap();
    eventually("job running", || {
        a.state.running.lock().unwrap().contains_key(&id)
    })
    .await;
    // Let it get past the settle window and actually start.
    eventually("claim settled", || {
        a.state.store.get(&keys::claim(&id)).unwrap().is_some()
    })
    .await;
    pause(a.state.cfg.settle_window() + Duration::from_secs(1)).await;
    spec.cancelled = true;
    a.state.store.put_json(&keys::job(&id), &spec).unwrap();
    eventually("result written", || {
        a.state.store.get(&keys::result(&id)).unwrap().is_some()
    })
    .await;
    let result: JobResult = a
        .state
        .store
        .get(&keys::result(&id))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(result.exit_code, None);
    assert_eq!(result.error.as_deref(), Some("cancelled"));
    assert!(!a.state.running.lock().unwrap().contains_key(&id));
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn desired_state_converges_and_reports() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    let target = dir.join("managed.txt");
    let marker = dir.join("marker");
    let desired = DesiredState {
        files: vec![DesiredFile {
            path: target.to_string_lossy().into(),
            content: "hello\n".into(),
            mode: Some("0600".into()),
        }],
        ensure: vec![Ensure {
            name: "marker".into(),
            check: vec!["test".into(), "-e".into(), marker.to_string_lossy().into()],
            apply: vec!["touch".into(), marker.to_string_lossy().into()],
        }],
    };
    a.state
        .store
        .put_json(&keys::desired("id-solo"), &desired)
        .unwrap();
    eventually("reconciled", || {
        a.state
            .store
            .get(&keys::reconcile("id-solo"))
            .unwrap()
            .is_some()
    })
    .await;
    eventually("file written", || {
        std::fs::read_to_string(&target).ok().as_deref() == Some("hello\n")
    })
    .await;
    eventually("marker applied", || marker.exists()).await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    eventually("converged", || {
        a.state
            .store
            .get(&keys::reconcile("id-solo"))
            .unwrap()
            .and_then(|r| r.parse::<ReconcileReport>().ok())
            .map(|r| r.converged)
            .unwrap_or(false)
    })
    .await;
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_lease_is_taken_over() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    eventually("own facts", || a.state.my_facts().is_some()).await;
    let id = uuid::Uuid::new_v4().to_string();
    let spec = JobSpec {
        id: id.clone(),
        cmd: vec!["sh".into(), "-c".into(), "echo recovered".into()],
        cwd: None,
        env: Default::default(),
        selector: Default::default(),
        node: None,
        submitted_by: "test".into(),
        submitted_at_ms: flotilla_core::now_ms(),
        timeout_secs: Some(30),
        cancelled: false,
    };
    a.state.store.put_json(&keys::job(&id), &spec).unwrap();
    // A claim from a node that died: lease already in the past.
    let dead = JobClaim {
        job_id: id.clone(),
        node: "id-dead".into(),
        claimed_at_ms: 1,
        lease_until_ms: 1,
        attempt: 1,
    };
    a.state
        .store
        .merge(&flotilla_core::Record {
            key: keys::claim(&id),
            value: serde_json::to_value(&dead).unwrap(),
            author: "id-dead".into(),
            hlc: flotilla_core::Hlc::from_parts(1, 0),
            deleted: false,
        })
        .unwrap();
    eventually("taken over and finished", || {
        a.state.store.get(&keys::result(&id)).unwrap().is_some()
    })
    .await;
    let claim: JobClaim = a
        .state
        .store
        .get(&keys::claim(&id))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(claim.node, "id-solo");
    assert_eq!(claim.attempt, 2);
    let result: JobResult = a
        .state
        .store
        .get(&keys::result(&id))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert!(result.output_tail.contains("recovered"));
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_job_renews_lease_and_stops_when_claim_is_lost() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    eventually("own facts", || a.state.my_facts().is_some()).await;
    let id = uuid::Uuid::new_v4().to_string();
    let spec = JobSpec {
        id: id.clone(),
        cmd: vec!["tail".into(), "-f".into(), "/dev/null".into()],
        cwd: None,
        env: Default::default(),
        selector: Default::default(),
        node: Some("solo".into()),
        submitted_by: "test".into(),
        submitted_at_ms: flotilla_core::now_ms(),
        timeout_secs: None,
        cancelled: false,
    };
    a.state.store.put_json(&keys::job(&id), &spec).unwrap();
    eventually("claimed", || {
        a.state.store.get(&keys::claim(&id)).unwrap().is_some()
    })
    .await;
    let first: JobClaim = a
        .state
        .store
        .get(&keys::claim(&id))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    // Lease is 3s and renewal happens every 1s once running (after the settle window).
    eventually("lease renewed", || {
        a.state
            .store
            .get(&keys::claim(&id))
            .unwrap()
            .and_then(|r| r.parse::<JobClaim>().ok())
            .map(|c| c.lease_until_ms > first.lease_until_ms)
            .unwrap_or(false)
    })
    .await;
    assert!(a.state.running.lock().unwrap().contains_key(&id));
    // Another node steals the claim with a newer record. The executor must
    // stop and must not write a result.
    let now = flotilla_core::now_ms();
    let thief = JobClaim {
        job_id: id.clone(),
        node: "id-other".into(),
        claimed_at_ms: now,
        lease_until_ms: now + 600_000,
        attempt: 2,
    };
    a.state
        .store
        .merge(&flotilla_core::Record {
            key: keys::claim(&id),
            value: serde_json::to_value(&thief).unwrap(),
            author: "zzzz-other".into(),
            hlc: flotilla_core::Hlc::from_parts(now + 5_000, 0),
            deleted: false,
        })
        .unwrap();
    eventually("executor stopped", || {
        !a.state.running.lock().unwrap().contains_key(&id)
    })
    .await;
    assert!(
        a.state.store.get(&keys::result(&id)).unwrap().is_none(),
        "a stopped executor writes no result"
    );
    let claim: JobClaim = a
        .state
        .store
        .get(&keys::claim(&id))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(claim.node, "id-other");
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_retires_old_jobs_and_keeps_recent() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    let now = flotilla_core::now_ms();
    let mk = |id: &str, finished: u64| {
        let spec = JobSpec {
            id: id.into(),
            cmd: vec!["true".into()],
            cwd: None,
            env: Default::default(),
            selector: Default::default(),
            node: Some("elsewhere".into()),
            submitted_by: "test".into(),
            submitted_at_ms: finished - 1000,
            timeout_secs: None,
            cancelled: false,
        };
        let result = JobResult {
            job_id: id.into(),
            node: "id-elsewhere".into(),
            exit_code: Some(0),
            started_at_ms: finished - 500,
            finished_at_ms: finished,
            output_tail: String::new(),
            error: None,
        };
        a.state.store.put_json(&keys::job(id), &spec).unwrap();
        a.state.store.put_json(&keys::result(id), &result).unwrap();
    };
    let retention_ms = a.state.cfg.job_retention_hours * 3600 * 1000;
    mk("old", now - retention_ms - 60_000);
    mk("recent", now - 60_000);
    std::fs::write(a.state.cfg.job_log_path("old"), "log").unwrap();
    crate::gc::pass(&a.state).unwrap();
    assert!(a.state.store.get(&keys::job("old")).unwrap().is_none());
    assert!(a.state.store.get(&keys::result("old")).unwrap().is_none());
    assert!(!a.state.cfg.job_log_path("old").exists());
    assert!(a.state.store.get(&keys::job("recent")).unwrap().is_some());
    assert!(a
        .state
        .store
        .get(&keys::result("recent"))
        .unwrap()
        .is_some());
    // horizon moved to now - gc_horizon_days
    assert!(a.state.store.horizon().unwrap().wall_ms() > 0);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_round_trip_and_reject_bad_paths() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    let target = dir.join("sub").join("payload.bin");
    let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let resp = a
        .http
        .put(format!("{}/v1/files", a.base))
        .query(&[
            ("path", target.to_string_lossy().as_ref()),
            ("mode", "0755"),
        ])
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let info: FileWriteResponse = resp.json().await.unwrap();
    assert_eq!(info.bytes, payload.len() as u64);
    assert_eq!(std::fs::read(&target).unwrap(), payload);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
    assert!(!target.with_file_name("payload.bin.flotilla-tmp").exists());

    let back = a
        .http
        .get(format!("{}/v1/files", a.base))
        .query(&[("path", target.to_string_lossy().as_ref())])
        .send()
        .await
        .unwrap();
    assert_eq!(back.status(), 200);
    assert_eq!(back.bytes().await.unwrap().to_vec(), payload);

    let missing = a
        .http
        .get(format!("{}/v1/files", a.base))
        .query(&[("path", "/definitely/not/here")])
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    for bad in ["relative/path", "/tmp/../etc/passwd"] {
        let r = a
            .http
            .put(format!("{}/v1/files", a.base))
            .query(&[("path", bad)])
            .body("x")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{bad}");
    }
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_stream_reports_changes_with_prefix_filter() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    let resp = a
        .http
        .get(format!("{}/v1/events", a.base))
        .query(&[("prefix", "test/")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let mut body = resp.bytes_stream();
    a.state
        .store
        .put("other/ignored", serde_json::json!(0))
        .unwrap();
    a.state
        .store
        .put("test/one", serde_json::json!({"n": 1}))
        .unwrap();
    a.state.store.delete("test/one").unwrap();
    let mut buf = String::new();
    let mut got: Vec<RecordEvent> = Vec::new();
    while got.len() < 2 {
        let chunk = tokio::time::timeout(Duration::from_secs(10), body.next())
            .await
            .expect("event within 10s")
            .unwrap()
            .unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
        for line in buf.lines() {
            if let Some(json) = line.strip_prefix("data:") {
                if let Ok(ev) = serde_json::from_str::<RecordEvent>(json.trim()) {
                    if !got.iter().any(|g| g.hlc == ev.hlc) {
                        got.push(ev);
                    }
                }
            }
        }
    }
    assert_eq!(got[0].key, "test/one");
    assert!(!got[0].deleted);
    assert_eq!(got[1].key, "test/one");
    assert!(got[1].deleted);
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_keeps_cancelled_job_while_its_lease_is_live() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    let now = flotilla_core::now_ms();
    let retention_ms = a.state.cfg.job_retention_hours * 3600 * 1000;
    let spec = JobSpec {
        id: "long".into(),
        cmd: vec!["true".into()],
        cwd: None,
        env: Default::default(),
        selector: Default::default(),
        node: Some("elsewhere".into()),
        submitted_by: "test".into(),
        submitted_at_ms: now - retention_ms - 60_000,
        timeout_secs: None,
        cancelled: true,
    };
    a.state.store.put_json(&keys::job("long"), &spec).unwrap();
    let claim = JobClaim {
        job_id: "long".into(),
        node: "id-elsewhere".into(),
        claimed_at_ms: now - 1000,
        lease_until_ms: now + 60_000,
        attempt: 1,
    };
    a.state
        .store
        .put_json(&keys::claim("long"), &claim)
        .unwrap();
    crate::gc::pass(&a.state).unwrap();
    assert!(
        a.state.store.get(&keys::job("long")).unwrap().is_some(),
        "still claimed: must not retire"
    );
    // once the lease lapsed long ago, it goes
    let dead = JobClaim {
        lease_until_ms: now - retention_ms - 60_000,
        ..claim
    };
    a.state.store.put_json(&keys::claim("long"), &dead).unwrap();
    crate::gc::pass(&a.state).unwrap();
    assert!(a.state.store.get(&keys::job("long")).unwrap().is_none());
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_leaves_claim_and_writes_no_result() {
    let dir = tmp();
    let pa = free_port().await;
    let a = node("solo", pa, vec![], &dir).await;
    eventually("own facts", || a.state.my_facts().is_some()).await;
    let id = uuid::Uuid::new_v4().to_string();
    let spec = JobSpec {
        id: id.clone(),
        cmd: vec!["tail".into(), "-f".into(), "/dev/null".into()],
        cwd: None,
        env: Default::default(),
        selector: Default::default(),
        node: Some("solo".into()),
        submitted_by: "test".into(),
        submitted_at_ms: flotilla_core::now_ms(),
        timeout_secs: None,
        cancelled: false,
    };
    a.state.store.put_json(&keys::job(&id), &spec).unwrap();
    eventually("claimed", || {
        a.state.store.get(&keys::claim(&id)).unwrap().is_some()
    })
    .await;
    pause(a.state.cfg.settle_window() + Duration::from_secs(1)).await;
    assert!(a.state.running.lock().unwrap().contains_key(&id));
    // simulate SIGTERM handling
    a.state
        .shutting_down
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let tokens: Vec<_> = a.state.running.lock().unwrap().values().cloned().collect();
    for t in tokens {
        t.cancel();
    }
    eventually("runner exited", || {
        !a.state.running.lock().unwrap().contains_key(&id)
    })
    .await;
    assert!(
        a.state.store.get(&keys::result(&id)).unwrap().is_none(),
        "no result on shutdown"
    );
    let claim: JobClaim = a
        .state
        .store
        .get(&keys::claim(&id))
        .unwrap()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(claim.node, "id-solo", "claim kept for resume/takeover");
    std::fs::remove_dir_all(dir).ok();
}
