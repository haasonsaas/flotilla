//! Inbound authorization. Loopback is trusted; everything else must resolve
//! through the identity provider to an allowed login or tag.

use crate::server::AppState;
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use flotilla_core::api::ErrorBody;
use std::net::SocketAddr;

/// Who is calling, attached as a request extension.
#[derive(Clone, Debug)]
pub struct Caller {
    pub node_id: String,
    pub name: String,
    pub login: String,
}

pub async fn require_peer(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let ip = addr.ip();
    let caller = if ip.is_loopback() {
        Caller { node_id: state.me.node_id.clone(), name: "local".into(), login: "local".into() }
    } else {
        match state.identity.whois(ip).await {
            Ok(Some(w)) => {
                if !is_allowed(&state.cfg, &w) {
                    tracing::warn!(%ip, login = %w.login, tags = ?w.tags, "rejected: not allowed");
                    return reject(StatusCode::FORBIDDEN, format!("{} is not an allowed user or tag", w.login));
                }
                Caller { node_id: w.node_id, name: w.name, login: w.login }
            }
            Ok(None) => {
                tracing::warn!(%ip, "rejected: unknown peer");
                return reject(StatusCode::UNAUTHORIZED, "unknown peer".into());
            }
            Err(e) => {
                tracing::error!(%ip, error = %e, "identity lookup failed");
                return reject(StatusCode::SERVICE_UNAVAILABLE, "identity lookup failed".into());
            }
        }
    };
    req.extensions_mut().insert(caller);
    next.run(req).await
}

pub fn is_allowed(cfg: &crate::config::Config, w: &crate::identity::WhoIs) -> bool {
    let user_ok = cfg.allowed_users.iter().any(|u| u == &w.login);
    let tag_ok = w.tags.iter().any(|t| cfg.allowed_tags.contains(t));
    user_ok || tag_ok
}

fn reject(code: StatusCode, msg: String) -> Response {
    (code, Json(ErrorBody { error: msg })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::identity::WhoIs;

    fn who(login: &str, tags: &[&str]) -> WhoIs {
        WhoIs { node_id: "n".into(), name: "n".into(), login: login.into(), tags: tags.iter().map(|s| s.to_string()).collect() }
    }

    #[test]
    fn policy() {
        let cfg = Config { allowed_users: vec!["me@example.com".into()], allowed_tags: vec!["tag:fleet".into()], ..Config::default() };
        assert!(is_allowed(&cfg, &who("me@example.com", &[])));
        assert!(is_allowed(&cfg, &who("tagged-devices", &["tag:other", "tag:fleet"])));
        assert!(!is_allowed(&cfg, &who("stranger@example.com", &["tag:other"])));
        assert!(!is_allowed(&Config::default(), &who("me@example.com", &[])), "empty policy denies");
    }
}
