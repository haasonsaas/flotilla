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
    for _ in 0..100 {
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
