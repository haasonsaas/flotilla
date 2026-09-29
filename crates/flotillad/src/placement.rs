//! Affinity placement. Every node evaluates the same rule over the facts
//! everyone replicates, so the winner is agreed without a coordinator; the
//! claim record's last-writer-wins still settles any disagreement.

use flotilla_core::schema::{JobSpec, NodeFacts};
use std::cmp::Reverse;

/// The default when a node does not advertise `max_jobs` (matches
/// `Config::max_concurrent_jobs`' default).
pub const DEFAULT_MAX_JOBS: usize = 2;

/// Lower sorts first (better).
pub type Rank = (u8, Reverse<u64>, u64, String);

/// A node's advertised per-node concurrency cap.
pub fn max_jobs(f: &NodeFacts) -> usize {
    f.labels
        .get("max_jobs")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_JOBS)
}

pub fn at_cap(f: &NodeFacts) -> bool {
    f.running_jobs.len() >= max_jobs(f)
}

fn keys_close(a: &str, b: &str) -> bool {
    !a.is_empty() && !b.is_empty() && (a.starts_with(b) || b.starts_with(a))
}

/// Tier 0: warm and the key matches. Tier 1: warm. Tier 2: cold. Within
/// tier 0 and 1 the most recently used cache wins; then least load per cpu
/// (in thousandths); then node id.
pub fn rank(spec: &JobSpec, f: &NodeFacts) -> Rank {
    let load = (f.load_1m / f.cpus.max(1) as f64 * 1000.0).max(0.0) as u64;
    let warm = spec.prefer_warm.as_ref().and_then(|n| f.warm.get(n));
    let (tier, used) = match warm {
        Some(w)
            if spec
                .warm_key
                .as_deref()
                .is_some_and(|k| keys_close(k, &w.key)) =>
        {
            (0, w.last_used_ms)
        }
        Some(w) => (1, w.last_used_ms),
        None => (2, 0),
    };
    (tier, Reverse(used), load, f.node_id.clone())
}

/// True if `mine` is the best node for `spec` among `mine` and the other
/// fresh, selector-matching nodes that are below their cap.
pub fn should_claim(spec: &JobSpec, mine: &NodeFacts, others: &[NodeFacts]) -> bool {
    let my_rank = rank(spec, mine);
    !others.iter().any(|o| {
        o.node_id != mine.node_id
            && spec.selector.matches(&o.labels)
            && !at_cap(o)
            && rank(spec, o) < my_rank
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotilla_core::schema::WarmCache;

    fn node(id: &str, load: f64, warm: Option<(&str, u64)>) -> NodeFacts {
        let mut f = NodeFacts {
            node_id: id.into(),
            cpus: 4,
            load_1m: load,
            ..Default::default()
        };
        if let Some((key, used)) = warm {
            f.warm.insert(
                "mono-rust".into(),
                WarmCache {
                    key: key.into(),
                    last_used_ms: used,
                    ..Default::default()
                },
            );
        }
        f
    }

    fn spec(key: Option<&str>) -> JobSpec {
        JobSpec {
            id: "j".into(),
            cmd: vec![],
            cwd: None,
            env: Default::default(),
            selector: Default::default(),
            node: None,
            submitted_by: "t".into(),
            submitted_at_ms: 0,
            timeout_secs: None,
            cancelled: false,
            pick: None,
            tmux: None,
            kind: None,
            after: vec![],
            batch: None,
            retries: 0,
            retry: 0,
            cancel_reason: None,
            prefer_warm: Some("mono-rust".into()),
            warm_key: key.map(Into::into),
            lock: None,
        }
    }

    #[test]
    fn warm_beats_less_loaded_cold() {
        let s = spec(None);
        let warm = node("a", 3.0, Some(("k", 100)));
        let cold = node("b", 0.0, None);
        assert!(should_claim(&s, &warm, std::slice::from_ref(&cold)));
        assert!(!should_claim(&s, &cold, std::slice::from_ref(&warm)));
    }

    #[test]
    fn freshest_warm_wins_then_load() {
        let s = spec(None);
        let old = node("a", 0.0, Some(("k", 100)));
        let fresh = node("b", 3.0, Some(("k", 200)));
        assert!(should_claim(&s, &fresh, std::slice::from_ref(&old)));
        assert!(!should_claim(&s, &old, std::slice::from_ref(&fresh)));
        let same_but_busy = node("c", 3.0, Some(("k", 200)));
        let same_idle = node("d", 1.0, Some(("k", 200)));
        assert!(should_claim(
            &s,
            &same_idle,
            std::slice::from_ref(&same_but_busy)
        ));
    }

    #[test]
    fn matching_key_beats_fresher_cache() {
        let s = spec(Some("abcdef0123"));
        let stale_match = node("a", 0.0, Some(("abcdef0", 100)));
        let fresh_other = node("b", 0.0, Some(("9999999", 900)));
        assert!(should_claim(
            &s,
            &stale_match,
            std::slice::from_ref(&fresh_other)
        ));
        assert!(!should_claim(
            &s,
            &fresh_other,
            std::slice::from_ref(&stale_match)
        ));
    }

    #[test]
    fn peer_at_cap_is_passed_over() {
        let s = spec(None);
        let cold = node("b", 0.0, None);
        let mut warm = node("a", 0.0, Some(("k", 100)));
        warm.running_jobs = vec!["x".into(), "y".into()];
        assert!(at_cap(&warm));
        assert!(should_claim(&s, &cold, std::slice::from_ref(&warm)));
        warm.labels.insert("max_jobs".into(), "3".into());
        assert!(!at_cap(&warm));
        assert!(!should_claim(&s, &cold, std::slice::from_ref(&warm)));
    }

    #[test]
    fn nobody_warm_falls_back_to_least_load_then_id() {
        let s = spec(None);
        let a = node("a", 2.0, None);
        let b = node("b", 1.0, None);
        assert!(should_claim(&s, &b, std::slice::from_ref(&a)));
        assert!(!should_claim(&s, &a, std::slice::from_ref(&b)));
        let a2 = node("a", 1.0, None);
        assert!(
            should_claim(&s, &a2, std::slice::from_ref(&b)),
            "id breaks ties"
        );
    }

    #[test]
    fn selector_excludes_ineligible_peers() {
        let mut s = spec(None);
        s.selector = "os=linux".parse().unwrap();
        let mine = node("b", 0.0, None);
        let warm_mac = node("a", 0.0, Some(("k", 100))); // no os label
        assert!(should_claim(&s, &mine, std::slice::from_ref(&warm_mac)));
    }
}
