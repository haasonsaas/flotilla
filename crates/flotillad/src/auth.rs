//! Inbound authorization.
//!
//! Every non-loopback request is resolved through the identity provider.
//! The caller's roles come from two places, unioned:
//! - the config allow-lists (`allowed_users`, `allowed_tags`): every role;
//! - the tailnet policy, via the application capability named by
//!   `grant_cap` in the caller's `whois` CapMap, whose values look like
//!   `{"roles": ["read", "exec"]}`.
//!
//! Loopback callers are the same user on the same machine and get every role.
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
    /// Status, records, peers, events, job logs, file downloads.
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
        self.roles.contains(&Role::Admin) || self.roles.contains(&role)
    }
}

fn all_roles() -> BTreeSet<Role> {
    [Role::Read, Role::Sync, Role::Write, Role::Exec, Role::Admin]
        .into_iter()
        .collect()
}

/// Roles for a resolved caller. Empty means "not allowed at all".
pub fn roles_for(cfg: &crate::config::Config, w: &crate::identity::WhoIs) -> BTreeSet<Role> {
    let user_ok = cfg.allowed_users.iter().any(|u| u == &w.login);
    let tag_ok = w.tags.iter().any(|t| cfg.allowed_tags.contains(t));
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

pub async fn require_peer(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let ip = addr.ip();
    let caller = if ip.is_loopback() {
        Caller {
            node_id: state.me.node_id.clone(),
            name: "local".into(),
            login: "local".into(),
            roles: all_roles(),
        }
    } else {
        match state.identity.whois(ip).await {
            Ok(Some(w)) => {
                let roles = roles_for(&state.cfg, &w);
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
            "haasonsaas.dev/cap/flotilla".into(),
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
