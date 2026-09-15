//! HTTP API. Every route except /v1/health sits behind the peer auth
//! middleware.

use crate::auth;
use crate::auth::Caller;
use crate::config::Config;
use crate::exec;
use crate::identity::{IdentityProvider, NodeInfo};
use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use flotilla_core::api::*;
use flotilla_core::schema::NodeFacts;
use flotilla_core::sync::SyncMessage;
use flotilla_core::{keys, Record, Store};
use futures::StreamExt;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub identity: Arc<IdentityProvider>,
    pub store: Arc<Store>,
    pub me: NodeInfo,
    pub http: reqwest::Client,
    /// Jobs currently executing on this node, with their cancel tokens.
    pub running: Arc<Mutex<HashMap<String, CancellationToken>>>,
    /// Per-peer sync bookkeeping, keyed like `sync_loop::Candidate::key`.
    pub sync_state: Arc<Mutex<HashMap<String, PeerSyncState>>>,
    /// Every applied store change, for `/v1/events` subscribers.
    pub events: tokio::sync::broadcast::Sender<RecordEvent>,
}

impl AppState {
    pub fn new(
        cfg: Arc<Config>,
        identity: Arc<IdentityProvider>,
        store: Arc<Store>,
        me: NodeInfo,
    ) -> AppState {
        let (events, _) = tokio::sync::broadcast::channel::<RecordEvent>(1024);
        let tx = events.clone();
        store.on_change(move |r| {
            let _ = tx.send(RecordEvent::from(r));
        });
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .connect_timeout(std::time::Duration::from_secs(3))
            .build()
            .expect("reqwest client");
        AppState {
            cfg,
            identity,
            store,
            me,
            http,
            running: Arc::new(Mutex::new(HashMap::new())),
            sync_state: Arc::new(Mutex::new(HashMap::new())),
            events,
        }
    }

    pub fn running_jobs(&self) -> Vec<String> {
        let mut v: Vec<String> = self.running.lock().unwrap().keys().cloned().collect();
        v.sort();
        v
    }

    /// Facts for this node as last written (labels are needed by the scheduler).
    pub fn my_facts(&self) -> Option<NodeFacts> {
        self.store
            .get(&keys::node_facts(&self.me.node_id))
            .ok()
            .flatten()
            .and_then(|r| r.parse().ok())
    }
}

pub struct AppError(anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        AppError(e.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::warn!(error = %self.0, "request failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                error: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

type ApiResult<T> = std::result::Result<T, AppError>;

pub fn router(state: AppState) -> Router {
    let authed = Router::new()
        .route("/v1/self", get(get_self))
        .route("/v1/peers", get(get_peers))
        .route("/v1/status", get(get_status))
        .route("/v1/records", get(list_records))
        .route(
            "/v1/records/{*key}",
            get(get_record).put(put_record).delete(delete_record),
        )
        .route("/v1/sync", post(post_sync))
        .route("/v1/syncstate", get(get_sync_state))
        .route("/v1/syncnow", post(post_sync_now))
        .route("/v1/exec", post(post_exec))
        .route("/v1/jobs/{id}/log", get(get_job_log))
        .route("/v1/files", get(get_file).put(put_file))
        .route("/v1/events", get(get_events))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_peer,
        ));
    Router::new()
        .route(
            "/v1/health",
            get(|| async { Json(serde_json::json!({"ok": true})) }),
        )
        .merge(authed)
        .with_state(state)
}

pub async fn serve(state: AppState) -> Result<()> {
    let mut addrs: Vec<SocketAddr> = vec![SocketAddr::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        state.cfg.port,
    )];
    for ip in &state.me.ips {
        addrs.push(SocketAddr::new(*ip, state.cfg.port));
    }
    for extra in &state.cfg.listen {
        addrs.push(
            extra
                .parse()
                .with_context(|| format!("bad listen address {extra:?}"))?,
        );
    }
    addrs.dedup();
    let app = router(state);
    let mut tasks = Vec::new();
    for addr in addrs {
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(%addr, error = %e, "bind failed, skipping");
                continue;
            }
        };
        tracing::info!(%addr, "listening");
        let app = app.clone();
        tasks.push(tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
        }));
    }
    anyhow::ensure!(!tasks.is_empty(), "could not bind any address");
    for t in tasks {
        t.await??;
    }
    Ok(())
}

fn self_info(state: &AppState) -> SelfInfo {
    SelfInfo {
        node_id: state.me.node_id.clone(),
        name: state.me.name.clone(),
        version: crate::VERSION.to_string(),
        tailscale_ips: state.me.ips.iter().map(ToString::to_string).collect(),
        port: state.cfg.port,
    }
}

async fn get_self(State(state): State<AppState>) -> Json<SelfInfo> {
    Json(self_info(&state))
}

async fn get_peers(State(state): State<AppState>) -> ApiResult<Json<PeersResponse>> {
    let peers = state.identity.peers().await?;
    Ok(Json(PeersResponse {
        me: self_info(&state),
        peers,
    }))
}

async fn get_status(State(state): State<AppState>) -> ApiResult<Json<StatusResponse>> {
    let online: HashSet<String> = state
        .identity
        .peers()
        .await?
        .into_iter()
        .filter(|p| p.online)
        .map(|p| p.node_id)
        .collect();
    let now = flotilla_core::now_ms();
    let mut nodes = Vec::new();
    for rec in state.store.list(keys::NODE)? {
        let facts: NodeFacts = match rec.parse() {
            Ok(f) => f,
            Err(_) => continue,
        };
        let is_me = facts.node_id == state.me.node_id;
        nodes.push(NodeStatus {
            online: is_me || online.contains(&facts.node_id),
            facts_age_secs: now.saturating_sub(facts.reported_at_ms) / 1000,
            facts,
        });
    }
    nodes.sort_by(|a, b| a.facts.name.cmp(&b.facts.name));
    Ok(Json(StatusResponse {
        me: state.me.node_id.clone(),
        nodes,
    }))
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    raw: bool,
}

async fn list_records(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<RecordsResponse>> {
    let records = if q.raw {
        state.store.list_raw(&q.prefix)?
    } else {
        state.store.list(&q.prefix)?
    };
    Ok(Json(RecordsResponse { records }))
}

async fn get_record(State(state): State<AppState>, Path(key): Path<String>) -> ApiResult<Response> {
    Ok(match state.store.get(&key)? {
        Some(r) => Json(r).into_response(),
        None => not_found(&key),
    })
}

async fn put_record(
    State(state): State<AppState>,
    Extension(caller): Extension<Caller>,
    Path(key): Path<String>,
    Json(body): Json<PutRecordRequest>,
) -> ApiResult<Json<Record>> {
    tracing::debug!(key, by = %caller.login, from = %caller.name, "put");
    Ok(Json(state.store.put(&key, body.value)?))
}

async fn delete_record(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> ApiResult<Response> {
    Ok(match state.store.delete(&key)? {
        Some(r) => Json(r).into_response(),
        None => not_found(&key),
    })
}

fn not_found(key: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorBody {
            error: format!("no record {key}"),
        }),
    )
        .into_response()
}

async fn post_sync(
    State(state): State<AppState>,
    Json(msg): Json<SyncMessage>,
) -> ApiResult<Json<SyncMessage>> {
    let store = state.store.clone();
    let reply =
        tokio::task::spawn_blocking(move || flotilla_core::sync::respond(&store, &msg)).await??;
    Ok(Json(reply))
}

async fn get_sync_state(State(state): State<AppState>) -> Json<SyncStateResponse> {
    let mut peers: Vec<PeerSyncState> =
        state.sync_state.lock().unwrap().values().cloned().collect();
    peers.sort_by(|a, b| a.name.cmp(&b.name));
    Json(SyncStateResponse { peers })
}

async fn post_sync_now(
    State(state): State<AppState>,
    Json(req): Json<SyncNowRequest>,
) -> ApiResult<Json<Vec<SyncNowResult>>> {
    let all = crate::sync_loop::candidates(&state).await?;
    let selected: Vec<_> = match &req.peer {
        Some(p) => {
            let as_url = flotilla_core::api::base_url(p, state.cfg.port);
            all.into_iter()
                .filter(|c| {
                    &c.key == p
                        || &c.name == p
                        || &c.url == p
                        || as_url.as_deref() == Some(c.url.as_str())
                })
                .collect()
        }
        None => all,
    };
    let mut out = Vec::new();
    for c in &selected {
        out.push(crate::sync_loop::sync_candidate(&state, c).await);
    }
    Ok(Json(out))
}

async fn post_exec(
    State(_state): State<AppState>,
    Extension(caller): Extension<Caller>,
    Json(req): Json<ExecRequest>,
) -> Response {
    tracing::info!(cmd = ?req.cmd, by = %caller.login, from = %caller.name, node = %caller.node_id, "exec");
    let cancel = CancellationToken::new();
    let rx = exec::spawn(req, cancel.clone());
    // Dropping the response body (client went away) cancels the child.
    let guard = cancel.drop_guard();
    let stream = ReceiverStream::new(rx).map(move |frame| {
        let _keep = &guard;
        let mut line = serde_json::to_vec(&frame).unwrap_or_default();
        line.push(b'\n');
        Ok::<_, std::convert::Infallible>(Bytes::from(line))
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn get_job_log(State(state): State<AppState>, Path(id): Path<String>) -> ApiResult<Response> {
    if id.is_empty() || id.contains('/') || id.contains("..") {
        return Ok((StatusCode::BAD_REQUEST, "bad job id").into_response());
    }
    let path = state.cfg.job_log_path(&id);
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            Ok(([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], bytes).into_response())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok((StatusCode::NOT_FOUND, "no log for that job on this node").into_response())
        }
        Err(e) => Err(e.into()),
    }
}

/// Resolve a client-supplied path: expand `~`, require absolute, refuse `..`.
fn resolve_path(p: &str) -> std::result::Result<std::path::PathBuf, String> {
    let expanded = exec::expand_home(p);
    let path = std::path::PathBuf::from(&expanded);
    if !path.is_absolute() {
        return Err(format!("path must be absolute or start with ~/: {p}"));
    }
    if path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(format!("path must not contain ..: {p}"));
    }
    Ok(path)
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    prefix: String,
}

/// Server-sent events of store changes, optionally filtered by key prefix.
/// A subscriber that falls behind gets a `lagged` event and continues.
async fn get_events(State(state): State<AppState>, Query(q): Query<EventsQuery>) -> Response {
    use axum::response::sse::{Event, KeepAlive, Sse};
    let rx = state.events.subscribe();
    let prefix = q.prefix;
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(move |item| {
        let prefix = prefix.clone();
        async move {
            match item {
                Ok(ev) if ev.key.starts_with(&prefix) => {
                    Some(Ok::<Event, std::convert::Infallible>(
                        Event::default()
                            .event("record")
                            .json_data(&ev)
                            .unwrap_or_else(|_| Event::default()),
                    ))
                }
                Ok(_) => None,
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    Some(Ok(Event::default().event("lagged").data(n.to_string())))
                }
            }
        }
    });
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response()
}

async fn get_file(
    State(_state): State<AppState>,
    Query(q): Query<FileQuery>,
) -> ApiResult<Response> {
    let path = match resolve_path(&q.path) {
        Ok(p) => p,
        Err(e) => {
            return Ok((StatusCode::BAD_REQUEST, Json(ErrorBody { error: e })).into_response())
        }
    };
    match tokio::fs::File::open(&path).await {
        Ok(f) => {
            let len = f.metadata().await.map(|m| m.len()).ok();
            let stream = tokio_util::io::ReaderStream::new(f);
            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream");
            if let Some(len) = len {
                resp = resp.header(header::CONTENT_LENGTH, len);
            }
            Ok(resp.body(Body::from_stream(stream)).unwrap())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("no such file: {}", path.display()),
            }),
        )
            .into_response()),
        Err(e) => Err(e.into()),
    }
}

/// Stream the body into `<path>.flotilla-tmp`, then rename over `path`, so a
/// running binary keeps its old inode and readers never see a partial file.
async fn put_file(
    State(_state): State<AppState>,
    Extension(caller): Extension<Caller>,
    Query(q): Query<FileQuery>,
    body: Body,
) -> ApiResult<Response> {
    use sha2::Digest;
    use tokio::io::AsyncWriteExt;
    let path = match resolve_path(&q.path) {
        Ok(p) => p,
        Err(e) => {
            return Ok((StatusCode::BAD_REQUEST, Json(ErrorBody { error: e })).into_response())
        }
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_file_name(format!(
        "{}.flotilla-tmp",
        path.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    ));
    let mut f = tokio::fs::File::create(&tmp).await?;
    let mut hasher = sha2::Sha256::new();
    let mut bytes: u64 = 0;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorBody {
                        error: format!("upload aborted: {e}"),
                    }),
                )
                    .into_response());
            }
        };
        hasher.update(&chunk);
        bytes += chunk.len() as u64;
        f.write_all(&chunk).await?;
    }
    f.flush().await?;
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = q
            .mode
            .as_deref()
            .and_then(|m| u32::from_str_radix(m.trim_start_matches("0o"), 8).ok())
            .unwrap_or(0o644);
        tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode)).await?;
    }
    tokio::fs::rename(&tmp, &path).await?;
    let sha256 = hex::encode(hasher.finalize());
    tracing::info!(path = %path.display(), bytes, by = %caller.login, from = %caller.name, "file written");
    Ok(Json(FileWriteResponse {
        path: path.to_string_lossy().into_owned(),
        bytes,
        sha256,
    })
    .into_response())
}

/// Wait for a duration without a timer primitive that can be mistaken for
/// a busy loop: a timeout around a future that never resolves.
pub async fn pause(d: std::time::Duration) {
    let _ = tokio::time::timeout(d, std::future::pending::<()>()).await;
}
