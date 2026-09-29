//! Watchdog: compare this node's facts with the `[alerts]` thresholds, keep
//! `alert/<node>/<id>` records in step, and notify when one starts firing.
//!
//! The record is written before any notification, so a node that cannot
//! reach ntfy (or whose tailscaled is the thing that died) still leaves the
//! alert in its own store, and it replicates as soon as the node syncs.

use crate::config::AlertsConfig;
use crate::server::AppState;
use flotilla_core::schema::{Alert, NodeFacts};
use flotilla_core::{keys, now_ms};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// A firing alert is rewritten at most this often when only its text changed.
const REFRESH_MS: u64 = 5 * 60 * 1000;

#[derive(Clone, Debug, PartialEq)]
pub struct Finding {
    pub id: String,
    pub kind: &'static str,
    pub subject: String,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Notify {
    None,
    Fired,
    Repeat,
    Resolved,
}

fn sanitize(s: &str) -> String {
    let t: String = s
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let t = t.trim_matches('_').to_string();
    if t.is_empty() {
        "root".into()
    } else {
        t
    }
}

/// Thresholds that the facts currently violate.
pub fn evaluate(cfg: &AlertsConfig, f: &NodeFacts) -> Vec<Finding> {
    let mut out = Vec::new();
    if cfg.disk_free_pct_min > 0.0 {
        for d in &f.disks {
            let pct = d.free_pct();
            if pct < cfg.disk_free_pct_min {
                out.push(Finding {
                    id: format!("disk_free_{}", sanitize(&d.mount)),
                    kind: "disk_free",
                    subject: d.mount.clone(),
                    message: format!(
                        "{} has {} GB free of {} GB ({:.1}%), below {}%",
                        d.mount, d.free_gb, d.total_gb, pct, cfg.disk_free_pct_min
                    ),
                });
            }
        }
    }
    if cfg.load_per_core_max > 0.0 && f.load_per_core > cfg.load_per_core_max {
        out.push(Finding {
            id: "load".into(),
            kind: "load",
            subject: "load".into(),
            message: format!(
                "load {:.2} per core ({:.1} on {} cores), above {}",
                f.load_per_core, f.load_1m, f.cpus, cfg.load_per_core_max
            ),
        });
    }
    if cfg.tailscale_logged_out {
        for t in &f.tailscale {
            if t.state != "Running" {
                let what = match t.state.as_str() {
                    "NeedsLogin" => "is logged out (NeedsLogin)".to_string(),
                    "unreachable" => "tailscaled is not answering".to_string(),
                    other => format!("is {other}, not Running"),
                };
                out.push(Finding {
                    id: format!("tailscale_{}", sanitize(&t.name)),
                    kind: "tailscale",
                    subject: t.name.clone(),
                    message: format!("tailnet {} {}", t.name, what),
                });
            }
        }
    }
    out
}

/// Decide which alert records to write. `pending` remembers when each
/// finding was first seen (in memory only) so `for_secs` can hold it back.
pub fn plan(
    cfg: &AlertsConfig,
    facts: &NodeFacts,
    existing: &BTreeMap<String, Alert>,
    pending: &mut HashMap<String, u64>,
    now: u64,
) -> Vec<(String, Alert, Notify)> {
    let findings = evaluate(cfg, facts);
    let mut out = Vec::new();
    pending.retain(|id, _| findings.iter().any(|f| &f.id == id));
    for f in &findings {
        let first_seen = *pending.entry(f.id.clone()).or_insert(now);
        match existing.get(&f.id) {
            Some(a) if a.firing => {
                let repeat = cfg.renotify_hours > 0
                    && now.saturating_sub(a.notified_ms) >= cfg.renotify_hours * 3_600_000;
                let stale_text = a.message != f.message && now - a.updated_ms >= REFRESH_MS;
                if repeat || stale_text {
                    let mut n = a.clone();
                    n.message = f.message.clone();
                    n.updated_ms = now;
                    if repeat {
                        n.notified_ms = now;
                    }
                    out.push((
                        f.id.clone(),
                        n,
                        if repeat { Notify::Repeat } else { Notify::None },
                    ));
                }
            }
            _ if now.saturating_sub(first_seen) >= cfg.for_secs * 1000 => {
                out.push((
                    f.id.clone(),
                    Alert {
                        node_id: facts.node_id.clone(),
                        node: facts.name.clone(),
                        kind: f.kind.into(),
                        subject: f.subject.clone(),
                        message: f.message.clone(),
                        firing: true,
                        since_ms: now,
                        updated_ms: now,
                        notified_ms: now,
                        resolved_ms: None,
                    },
                    Notify::Fired,
                ));
            }
            _ => {}
        }
    }
    for (id, a) in existing {
        if a.firing && !findings.iter().any(|f| &f.id == id) {
            let mut n = a.clone();
            n.firing = false;
            n.updated_ms = now;
            n.resolved_ms = Some(now);
            let notify = if cfg.notify_resolved {
                Notify::Resolved
            } else {
                Notify::None
            };
            out.push((id.clone(), n, notify));
        }
    }
    out
}

static PENDING: Mutex<Option<HashMap<String, HashMap<String, u64>>>> = Mutex::new(None);

/// Run after each facts write.
pub async fn check(state: &AppState, facts: &NodeFacts) {
    let existing: BTreeMap<String, Alert> = state
        .store
        .list(&keys::alerts_of(&state.me.node_id))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|r| {
            let id = r.key.rsplit('/').next()?.to_string();
            r.parse::<Alert>().ok().map(|a| (id, a))
        })
        .collect();
    let actions = {
        let mut g = PENDING.lock().unwrap();
        let pending = g
            .get_or_insert_with(HashMap::new)
            .entry(state.me.node_id.clone())
            .or_default();
        plan(&state.cfg.alerts, facts, &existing, pending, now_ms())
    };
    for (id, alert, notify) in actions {
        if let Err(e) = state
            .store
            .put_json(&keys::alert(&state.me.node_id, &id), &alert)
        {
            tracing::error!(error = %e, alert = %id, "writing alert");
            continue;
        }
        if alert.firing {
            tracing::warn!(node = %alert.node, kind = %alert.kind, "ALERT {}", alert.message);
        } else {
            tracing::info!(node = %alert.node, kind = %alert.kind, "alert cleared: {}", alert.message);
        }
        if notify != Notify::None {
            deliver(state, alert, notify);
        }
    }
}

fn deliver(state: &AppState, alert: Alert, notify: Notify) {
    let cfg = &state.cfg.alerts;
    let ntfy = cfg
        .ntfy_url
        .clone()
        .or_else(|| state.cfg.notify.as_ref().map(|n| n.ntfy_url.clone()))
        .zip(
            cfg.topic
                .clone()
                .or_else(|| state.cfg.notify.as_ref().map(|n| n.topic.clone())),
        );
    let token = cfg
        .token
        .clone()
        .or_else(|| state.cfg.notify.as_ref().and_then(|n| n.token.clone()));
    let webhook = cfg.webhook_url.clone();
    if ntfy.is_none() && webhook.is_none() {
        return;
    }
    let http = state.http.clone();
    let event = match notify {
        Notify::Resolved => "resolved",
        Notify::Repeat => "repeat",
        _ => "firing",
    };
    tokio::spawn(async move {
        if let Some((url, topic)) = ntfy {
            let url = format!("{}/{}", url.trim_end_matches('/'), topic);
            let (title, tag) = if event == "resolved" {
                (
                    format!("flotilla · {}: cleared", alert.node),
                    "white_check_mark",
                )
            } else {
                (
                    format!("flotilla · {}: alert", alert.node),
                    "rotating_light",
                )
            };
            let mut req = http
                .post(&url)
                .header("Title", title)
                .header("Tags", tag)
                .header(
                    "Priority",
                    if event == "resolved" {
                        "default"
                    } else {
                        "high"
                    },
                )
                .body(format!("{}: {}", alert.node, alert.message));
            if let Some(t) = &token {
                req = req.header("Authorization", format!("Bearer {t}"));
            }
            match req.send().await {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => tracing::warn!(status = %r.status(), "ntfy rejected alert"),
                Err(e) => tracing::warn!(error = %e, "ntfy unreachable for alert"),
            }
        }
        if let Some(url) = webhook {
            match http
                .post(&url)
                .json(&json!({"event": event, "alert": alert}))
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => tracing::warn!(status = %r.status(), "alert webhook rejected"),
                Err(e) => tracing::warn!(error = %e, "alert webhook unreachable"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotilla_core::schema::{DiskInfo, TailnetHealth};

    fn facts() -> NodeFacts {
        NodeFacts {
            node_id: "n1".into(),
            name: "olympus".into(),
            cpus: 8,
            disks: vec![
                DiskInfo {
                    mount: "/".into(),
                    total_gb: 500,
                    free_gb: 200,
                },
                DiskInfo {
                    mount: "/mnt/build".into(),
                    total_gb: 590,
                    free_gb: 0,
                },
            ],
            tailscale: vec![
                TailnetHealth {
                    name: "evalops".into(),
                    state: "Running".into(),
                },
                TailnetHealth {
                    name: "home".into(),
                    state: "NeedsLogin".into(),
                },
            ],
            ..Default::default()
        }
    }

    fn cfg() -> AlertsConfig {
        AlertsConfig {
            for_secs: 0,
            ..Default::default()
        }
    }

    #[test]
    fn the_full_build_volume_and_the_logged_out_tailnet_trip() {
        let f = evaluate(&cfg(), &facts());
        let ids: Vec<&str> = f.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, ["disk_free_mnt_build", "tailscale_home"]);
        assert!(f[0].message.contains("0 GB free of 590 GB"));
        assert!(f[1].message.contains("logged out"));
    }

    #[test]
    fn switches_and_thresholds() {
        let mut c = cfg();
        c.disk_free_pct_min = 0.0;
        c.tailscale_logged_out = false;
        assert!(evaluate(&c, &facts()).is_empty());
        // 40% free trips a 50% threshold on both volumes' worth of data
        c.disk_free_pct_min = 50.0;
        let ids: Vec<String> = evaluate(&c, &facts()).into_iter().map(|f| f.id).collect();
        assert_eq!(ids, ["disk_free_root", "disk_free_mnt_build"]);
        let mut hot = facts();
        hot.load_per_core = 3.0;
        hot.load_1m = 24.0;
        c.disk_free_pct_min = 0.0;
        assert!(
            evaluate(&c, &hot).is_empty(),
            "load alert is off by default"
        );
        c.load_per_core_max = 2.0;
        assert_eq!(evaluate(&c, &hot)[0].kind, "load");
    }

    #[test]
    fn unreachable_and_stopped_tailscaled_trip() {
        let mut f = facts();
        f.tailscale = vec![
            TailnetHealth {
                name: "a".into(),
                state: "unreachable".into(),
            },
            TailnetHealth {
                name: "b".into(),
                state: "Stopped".into(),
            },
        ];
        let m: Vec<String> = evaluate(&cfg(), &f)
            .into_iter()
            .filter(|f| f.kind == "tailscale")
            .map(|f| f.message)
            .collect();
        assert_eq!(m.len(), 2);
        assert!(m[0].contains("not answering"));
        assert!(m[1].contains("Stopped"));
    }

    #[test]
    fn fires_once_then_resolves() {
        let c = cfg();
        let mut pending = HashMap::new();
        let mut existing = BTreeMap::new();
        let t0 = 1_000_000;
        let a = plan(&c, &facts(), &existing, &mut pending, t0);
        assert_eq!(a.len(), 2);
        assert!(a.iter().all(|(_, al, n)| al.firing && *n == Notify::Fired));
        for (id, al, _) in a {
            existing.insert(id, al);
        }
        // still tripped: nothing to write, nothing to send
        assert!(plan(&c, &facts(), &existing, &mut pending, t0 + 15_000).is_empty());
        // the disk is fixed
        let mut healed = facts();
        healed.disks[1].free_gb = 300;
        let a = plan(&c, &healed, &existing, &mut pending, t0 + 30_000);
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].0, "disk_free_mnt_build");
        assert!(!a[0].1.firing);
        assert_eq!(a[0].1.resolved_ms, Some(t0 + 30_000));
        assert_eq!(a[0].2, Notify::None, "resolved is quiet unless asked for");
        let mut c2 = c.clone();
        c2.notify_resolved = true;
        let a = plan(&c2, &healed, &existing, &mut pending, t0 + 30_000);
        assert_eq!(a[0].2, Notify::Resolved);
    }

    #[test]
    fn for_secs_holds_back_a_blip() {
        let mut c = cfg();
        c.for_secs = 60;
        let mut pending = HashMap::new();
        let existing = BTreeMap::new();
        assert!(plan(&c, &facts(), &existing, &mut pending, 1_000).is_empty());
        assert!(plan(&c, &facts(), &existing, &mut pending, 31_000).is_empty());
        // it clears before 60s, so the clock restarts
        let ok = NodeFacts {
            node_id: "n1".into(),
            ..Default::default()
        };
        assert!(plan(&c, &ok, &existing, &mut pending, 45_000).is_empty());
        assert!(pending.is_empty());
        assert!(plan(&c, &facts(), &existing, &mut pending, 50_000).is_empty());
        let a = plan(&c, &facts(), &existing, &mut pending, 111_000);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn renotify_repeats_while_firing() {
        let mut c = cfg();
        c.renotify_hours = 1;
        let mut pending = HashMap::new();
        let mut existing = BTreeMap::new();
        for (id, al, _) in plan(&c, &facts(), &existing, &mut pending, 0) {
            existing.insert(id, al);
        }
        assert!(plan(&c, &facts(), &existing, &mut pending, 3_599_000).is_empty());
        let a = plan(&c, &facts(), &existing, &mut pending, 3_600_000);
        assert_eq!(a.len(), 2);
        assert!(a.iter().all(|(_, _, n)| *n == Notify::Repeat));
    }

    async fn state_with(alerts: AlertsConfig) -> AppState {
        use crate::config::{Config, StaticIdentityConfig};
        use crate::identity::IdentityProvider;
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("flotilla-al-{}", uuid::Uuid::new_v4()));
        let cfg = Config {
            data_dir: dir.clone(),
            identity: "static".into(),
            alerts,
            static_identity: Some(StaticIdentityConfig {
                node_id: "n1".into(),
                name: "olympus".into(),
                ips: vec![],
                login: "static@local".into(),
                peers: vec![],
            }),
            ..Config::default()
        };
        std::fs::create_dir_all(&dir).unwrap();
        let identity = Arc::new(IdentityProvider::from_config(&cfg).unwrap());
        let me = identity.me().await.unwrap();
        let store = Arc::new(flotilla_core::Store::open(&dir.join("store.redb"), "n1").unwrap());
        AppState::new(Arc::new(cfg), identity, store, me)
    }

    #[tokio::test]
    async fn trip_writes_a_record_and_posts_webhook_and_ntfy_once() {
        use axum::routing::post;
        let hits: std::sync::Arc<Mutex<Vec<(String, String)>>> = Default::default();
        let h = hits.clone();
        let app = axum::Router::new().route(
            "/{*path}",
            post(
                move |axum::extract::Path(p): axum::extract::Path<String>, body: String| {
                    let h = h.clone();
                    async move {
                        h.lock().unwrap().push((p, body));
                        "ok"
                    }
                },
            ),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });

        let state = state_with(AlertsConfig {
            for_secs: 0,
            webhook_url: Some(format!("{base}/hook")),
            ntfy_url: Some(base.clone()),
            topic: Some("fleet".into()),
            notify_resolved: true,
            ..Default::default()
        })
        .await;
        let mut f = facts();
        f.node_id = "n1".into();
        check(&state, &f).await;
        // the record is in the store immediately
        let recs = state.store.list(&keys::alerts_of("n1")).unwrap();
        assert_eq!(recs.len(), 2);
        let a: Alert = state
            .store
            .get(&keys::alert("n1", "disk_free_mnt_build"))
            .unwrap()
            .unwrap()
            .parse()
            .unwrap();
        assert!(a.firing);
        assert_eq!(a.kind, "disk_free");
        assert_eq!(a.subject, "/mnt/build");
        // a second pass with the same facts sends nothing new
        check(&state, &f).await;
        for _ in 0..50 {
            if hits.lock().unwrap().len() >= 4 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        {
            let got = hits.lock().unwrap();
            assert_eq!(got.len(), 4, "2 alerts x (ntfy + webhook): {got:?}");
            assert!(got
                .iter()
                .any(|(p, b)| p == "fleet" && b.contains("590 GB")));
            assert!(got
                .iter()
                .any(|(p, b)| p == "hook" && b.contains("\"event\":\"firing\"")));
        }
        // fixing both clears the records and sends the resolved notices
        f.disks[1].free_gb = 400;
        f.tailscale[1].state = "Running".into();
        check(&state, &f).await;
        let a: Alert = state
            .store
            .get(&keys::alert("n1", "tailscale_home"))
            .unwrap()
            .unwrap()
            .parse()
            .unwrap();
        assert!(!a.firing);
        assert!(a.resolved_ms.is_some());
        for _ in 0..50 {
            if hits.lock().unwrap().len() >= 8 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(hits.lock().unwrap().len(), 8);
    }
}
