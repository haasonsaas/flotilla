//! Push notifications via ntfy for job outcomes.

use crate::server::AppState;

/// Fire-and-forget. `event` is one of succeeded, failed, cancelled, lost.
pub fn send(state: &AppState, event: &str, title: String, body: String) {
    let Some(cfg) = state.cfg.notify.clone() else {
        return;
    };
    if !cfg.on.iter().any(|e| e == event || e == "all") {
        return;
    }
    let http = state.http.clone();
    let node = state.me.name.clone();
    let event = event.to_string();
    tokio::spawn(async move {
        let url = format!("{}/{}", cfg.ntfy_url.trim_end_matches('/'), cfg.topic);
        let mut req = http
            .post(&url)
            .header("Title", format!("flotilla · {node}: {title}"))
            .header(
                "Tags",
                match event.as_str() {
                    "succeeded" => "white_check_mark",
                    "failed" => "x",
                    "cancelled" => "no_entry_sign",
                    _ => "warning",
                },
            )
            .body(body);
        if let Some(t) = &cfg.token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        match req.send().await {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => tracing::warn!(status = %r.status(), "ntfy rejected notification"),
            Err(e) => tracing::warn!(error = %e, "ntfy unreachable"),
        }
    });
}
