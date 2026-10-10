//! Inbound authorization.
//!
//! Every non-loopback request is resolved through the identity provider.
//! The caller's roles come from two places, unioned:
//! - the config allow-lists (`allowed_users`, `allowed_tags`): every role;
//! - the tailnet policy, via the application capability named by
//!   `grant_cap` in the caller's `whois` CapMap, whose values look like
//!   `{"roles": ["read", "exec"]}`.
//!
//! Loopback is not an identity: any local process, including untrusted CI job
//! code, can connect to 127.0.0.1. Local callers (loopback, or this node's own
//! tailnet addresses) therefore get every role only by presenting the local
//! operator token (`<data_dir>/local.token`, mode 0600, sent by the CLI in the
//! `x-flotilla-local-token` header), or when `trust_loopback = true`. Without
//! either they get [`Role::View`] only: status, peers, record reads, the
//! dashboard. Connections through `proxy_listen` that claim a local source
//! address are refused, since a local process can forge a PROXY header.
//! Route groups then require a specific role.

use crate::server::AppState;
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use flotilla_core::api::ErrorBody;
use std::collections::BTreeSet;
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// Status, peers, records, events, the dashboard. Implied by `Read`.
    /// This is all an unauthenticated local caller gets.
    View,
    /// Everything in `View`, plus job logs, artifacts and file downloads.
    Read,
    /// The peer sync protocol.
    Sync,
    /// Writing and deleting records (jobs, desired state).
    Write,
    /// Running processes and writing files.
    Exec,
    /// Everything.
    Admin,
}

impl Role {
    fn parse(s: &str) -> Option<Role> {
        match s {
            "view" => Some(Role::View),
            "read" => Some(Role::Read),
            "sync" => Some(Role::Sync),
            "write" => Some(Role::Write),
            "exec" => Some(Role::Exec),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::View => "view",
            Role::Read => "read",
            Role::Sync => "sync",
            Role::Write => "write",
            Role::Exec => "exec",
            Role::Admin => "admin",
        }
    }
}

/// Who is calling, attached as a request extension.
#[derive(Clone, Debug)]
pub struct Caller {
    pub node_id: String,
    pub name: String,
    pub login: String,
    pub roles: BTreeSet<Role>,
}

impl Caller {
    pub fn has(&self, role: Role) -> bool {
        self.roles.contains(&Role::Admin)
            || self.roles.contains(&role)
            || (role == Role::View && self.roles.contains(&Role::Read))
    }
}

fn all_roles() -> BTreeSet<Role> {
    [
        Role::View,
        Role::Read,
        Role::Sync,
        Role::Write,
        Role::Exec,
        Role::Admin,
    ]
    .into_iter()
    .collect()
}

/// The caller's address as the listener saw it: the TCP peer for plain
/// listeners, the PROXY header's source for userspace-tailnet listeners.
#[derive(Clone, Copy, Debug)]
pub struct ClientAddr(pub SocketAddr);

impl
    axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, tokio::net::TcpListener>>
    for ClientAddr
{
    fn connect_info(s: axum::serve::IncomingStream<'_, tokio::net::TcpListener>) -> Self {
        ClientAddr(*s.remote_addr())
    }
}

impl
    axum::extract::connect_info::Connected<
        axum::serve::IncomingStream<'_, crate::proxyproto::ProxyListener>,
    > for ClientAddr
{
    fn connect_info(s: axum::serve::IncomingStream<'_, crate::proxyproto::ProxyListener>) -> Self {
        ClientAddr(*s.remote_addr())
    }
}

/// Marks routes served by a PROXY-protocol listener, where the source
/// address comes from a header any local process can write.
#[derive(Clone, Copy, Debug)]
pub struct ViaProxy;

/// Which tailnet (index into the identity provider) a connection arrived
/// on, attached to each listener's routes. Tailnets can hand out the same
/// address to different machines, so `whois` must ask the right one.
#[derive(Clone, Copy, Debug)]
pub struct TailnetCtx(pub usize);

/// Roles for a resolved caller on the primary tailnet. Empty means "not
/// allowed at all".
#[cfg(test)]
pub fn roles_for(cfg: &crate::config::Config, w: &crate::identity::WhoIs) -> BTreeSet<Role> {
    roles_for_in(cfg, 0, w)
}

/// Roles for a caller that arrived on tailnet `idx`, using that tailnet's
/// allow-lists if it has its own.
pub fn roles_for_in(
    cfg: &crate::config::Config,
    idx: usize,
    w: &crate::identity::WhoIs,
) -> BTreeSet<Role> {
    let (users, tags) = cfg.allow_lists(idx);
    let user_ok = users.iter().any(|u| u == &w.login);
    let tag_ok = w.tags.iter().any(|t| tags.contains(t));
    if user_ok || tag_ok {
        return all_roles();
    }
    let mut roles = BTreeSet::new();
    if let Some(grants) = w.caps.get(&cfg.grant_cap) {
        for g in grants {
            if let Some(list) = g.get("roles").and_then(|r| r.as_array()) {
                roles.extend(
                    list.iter()
                        .filter_map(|v| v.as_str())
                        .filter_map(Role::parse),
                );
            }
        }
    }
    roles
}

/// Create (or reuse) the local operator token in `data_dir`. The file is
/// created with mode 0600 so only the daemon's user (and root) can read it.
pub fn load_or_create_token(data_dir: &std::path::Path) -> std::io::Result<String> {
    use flotilla_core::api::LOCAL_TOKEN_FILE;
    use std::io::Write;
    let path = data_dir.join(LOCAL_TOKEN_FILE);
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let t = existing.trim();
        if t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()) && private_file(&path) {
            return Ok(t.to_string());
        }
    }
    let token = hex::encode(rand::random::<[u8; 32]>());
    // Write a private temp file, then rename over the old one, so the token
    // is never readable under looser permissions.
    let tmp = data_dir.join(format!("{LOCAL_TOKEN_FILE}.tmp"));
    let _ = std::fs::remove_file(&tmp);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&tmp)?;
    f.write_all(token.as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, &path)?;
    Ok(token)
}

#[cfg(unix)]
fn private_file(path: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o077 == 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn private_file(_: &std::path::Path) -> bool {
    true
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn operator(state: &AppState, roles: BTreeSet<Role>) -> Caller {
    Caller {
        node_id: state.me.node_id.clone(),
        name: "local".into(),
        login: "local".into(),
        roles,
    }
}

pub async fn require_peer(
    State(state): State<AppState>,
    ConnectInfo(ClientAddr(addr)): ConnectInfo<ClientAddr>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let ip = addr.ip();
    // Loopback, or this machine dialing its own tailnet address: the source
    // is a process on this host, not a tailnet peer.
    let local = ip.is_loopback() || state.me.ips.contains(&ip);
    let caller = if local {
        if req.extensions().get::<ViaProxy>().is_some() {
            tracing::warn!(%ip, "rejected: local source address on a PROXY listener");
            return reject(
                StatusCode::FORBIDDEN,
                "local source address not accepted on the proxy listener".into(),
            );
        }
        let presented = req
            .headers()
            .get(flotilla_core::api::LOCAL_TOKEN_HEADER)
            .and_then(|v| v.to_str().ok());
        let proven = match (&state.local_token, presented) {
            (Some(want), Some(got)) => ct_eq(want.as_bytes(), got.trim().as_bytes()),
            _ => false,
        };
        if proven || state.cfg.trust_loopback {
            operator(&state, all_roles())
        } else {
            operator(&state, [Role::View].into_iter().collect())
        }
    } else {
        let tailnet = req.extensions().get::<TailnetCtx>().map_or(0, |c| c.0);
        match state.identity.whois_in(tailnet, ip).await {
            Ok(Some(w)) => {
                let roles = roles_for_in(&state.cfg, tailnet, &w);
                if roles.is_empty() {
                    tracing::warn!(%ip, login = %w.login, tags = ?w.tags, "rejected: no roles");
                    return reject(
                        StatusCode::FORBIDDEN,
                        format!(
                            "{} is not an allowed user or tag and holds no {} grant",
                            w.login, state.cfg.grant_cap
                        ),
                    );
                }
                Caller {
                    node_id: w.node_id,
                    name: w.name,
                    login: w.login,
                    roles,
                }
            }
            Ok(None) => {
                tracing::warn!(%ip, "rejected: unknown peer");
                return reject(StatusCode::UNAUTHORIZED, "unknown peer".into());
            }
            Err(e) => {
                tracing::error!(%ip, error = %e, "identity lookup failed");
                return reject(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "identity lookup failed".into(),
                );
            }
        }
    };
    req.extensions_mut().insert(caller);
    next.run(req).await
}

/// Middleware factory: the caller (set by `require_peer`) must hold `role`.
pub fn require_role(
    role: Role,
) -> impl Fn(
    Request<Body>,
    Next,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>>
       + Clone {
    move |req: Request<Body>, next: Next| {
        Box::pin(async move {
            match req.extensions().get::<Caller>() {
                Some(c) if c.has(role) => next.run(req).await,
                Some(c) => {
                    tracing::warn!(login = %c.login, needed = role.as_str(), roles = ?c.roles, "rejected: missing role");
                    reject(
                        StatusCode::FORBIDDEN,
                        format!("{} lacks the {} role", c.login, role.as_str()),
                    )
                }
                None => reject(StatusCode::UNAUTHORIZED, "unauthenticated".into()),
            }
        })
    }
}

fn reject(code: StatusCode, msg: String) -> Response {
    (code, Json(ErrorBody { error: msg })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::identity::WhoIs;
    use serde_json::json;

    fn who(login: &str, tags: &[&str]) -> WhoIs {
        WhoIs {
            node_id: "n".into(),
            name: "n".into(),
            login: login.into(),
            tags: tags.iter().map(|s| s.to_string()).collect(),
            caps: Default::default(),
        }
    }

    fn cfg() -> Config {
        Config {
            allowed_users: vec!["me@example.com".into()],
            allowed_tags: vec!["tag:fleet".into()],
            ..Config::default()
        }
    }

    #[test]
    fn allow_lists_grant_every_role() {
        assert_eq!(roles_for(&cfg(), &who("me@example.com", &[])), all_roles());
        assert_eq!(
            roles_for(&cfg(), &who("tagged-devices", &["tag:other", "tag:fleet"])),
            all_roles()
        );
        assert!(roles_for(&cfg(), &who("stranger@example.com", &["tag:other"])).is_empty());
        assert!(
            roles_for(&Config::default(), &who("me@example.com", &[])).is_empty(),
            "empty policy denies"
        );
    }

    #[test]
    fn grants_give_listed_roles_only() {
        let mut w = who("ci@example.com", &["tag:ci"]);
        w.caps.insert(
            "haasonsaas.com/cap/flotilla".into(),
            vec![
                json!({"roles": ["read", "bogus"]}),
                json!({"roles": ["exec"]}),
                json!("not an object"),
            ],
        );
        let roles = roles_for(&cfg(), &w);
        assert_eq!(roles, [Role::Read, Role::Exec].into_iter().collect());
        let c = Caller {
            node_id: "n".into(),
            name: "n".into(),
            login: "x".into(),
            roles,
        };
        assert!(c.has(Role::Read));
        assert!(c.has(Role::Exec));
        assert!(!c.has(Role::Write));
        assert!(!c.has(Role::Admin));
        // a different capability name is ignored
        let other = Config {
            grant_cap: "example.com/cap/other".into(),
            ..Config::default()
        };
        assert!(roles_for(&other, &w).is_empty());
    }

    #[test]
    fn per_tailnet_allow_lists_replace_global_ones() {
        use crate::config::TailnetConfig;
        let cfg = Config {
            allowed_users: vec!["me@example.com".into()],
            tailnet: vec![
                TailnetConfig {
                    name: "evalops".into(),
                    ..Default::default()
                },
                TailnetConfig {
                    name: "homelab".into(),
                    allowed_users: Some(vec!["home@example.com".into()]),
                    ..Default::default()
                },
            ],
            ..Config::default()
        };
        // tailnet 0 has no lists of its own: the global list applies
        assert_eq!(
            roles_for_in(&cfg, 0, &who("me@example.com", &[])),
            all_roles()
        );
        assert!(roles_for_in(&cfg, 0, &who("home@example.com", &[])).is_empty());
        // tailnet 1 has its own list, which replaces the global one
        assert_eq!(
            roles_for_in(&cfg, 1, &who("home@example.com", &[])),
            all_roles()
        );
        assert!(roles_for_in(&cfg, 1, &who("me@example.com", &[])).is_empty());
    }

    #[test]
    fn view_is_implied_by_read_but_not_the_reverse() {
        let only = |r: Role| Caller {
            node_id: "n".into(),
            name: "n".into(),
            login: "x".into(),
            roles: [r].into_iter().collect(),
        };
        assert!(only(Role::Read).has(Role::View));
        assert!(only(Role::View).has(Role::View));
        assert!(!only(Role::View).has(Role::Read));
        assert!(!only(Role::View).has(Role::Exec));
    }

    #[test]
    fn token_file_is_private_stable_and_repaired() {
        let dir = std::env::temp_dir().join(format!("flotilla-token-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let t = load_or_create_token(&dir).unwrap();
        assert_eq!(t.len(), 64);
        assert_eq!(load_or_create_token(&dir).unwrap(), t, "reused");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let path = dir.join(flotilla_core::api::LOCAL_TOKEN_FILE);
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
            // a token left world-readable is not trusted: rotate it
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            let t2 = load_or_create_token(&dir).unwrap();
            assert_ne!(t2, t);
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn constant_time_compare() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"abcd"));
    }

    #[test]
    fn admin_role_implies_everything() {
        let c = Caller {
            node_id: "n".into(),
            name: "n".into(),
            login: "x".into(),
            roles: [Role::Admin].into_iter().collect(),
        };
        assert!(c.has(Role::Write) && c.has(Role::Exec) && c.has(Role::Sync) && c.has(Role::Read));
    }
}
