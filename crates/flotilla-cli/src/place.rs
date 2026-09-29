//! Choosing a node for a new tmux session, and finding a session by name.
//! Pure functions over the status response so they can be tested without a
//! fleet.

use anyhow::{bail, Result};
use flotilla_core::api::NodeStatus;
use flotilla_core::selector::Selector;

/// Load a new session should be expected to add before it shows up in the
/// node's load average (facts are up to 15s old, and an agent that has just
/// started is still idle).
const SESSION_LOAD: f64 = 0.5;

/// Lower is better: load plus the sessions already there, per core.
pub fn score(n: &NodeStatus) -> f64 {
    let f = &n.facts;
    (f.load_1m + f.sessions.len() as f64 * SESSION_LOAD) / f.cpus.max(1) as f64
}

/// Why a node cannot take a new session, or None if it can.
fn ineligible(n: &NodeStatus, sel: &Selector) -> Option<String> {
    let f = &n.facts;
    if !n.online {
        return Some("offline".into());
    }
    if f.labels.get("tmux").map(String::as_str) != Some("yes") {
        return Some("no tmux".into());
    }
    if !sel.matches(&f.labels) {
        return Some("selector".into());
    }
    if let Some(max) = f.max_sessions {
        if f.sessions.len() >= max {
            return Some(format!("{}/{} sessions", f.sessions.len(), max));
        }
    }
    None
}

/// The least-loaded eligible node. Ties go to fewer sessions, then name.
pub fn pick_node<'a>(nodes: &'a [NodeStatus], sel: &Selector) -> Result<&'a NodeStatus> {
    let best = nodes
        .iter()
        .filter(|n| ineligible(n, sel).is_none())
        .min_by(|a, b| {
            score(a)
                .total_cmp(&score(b))
                .then(a.facts.sessions.len().cmp(&b.facts.sessions.len()))
                .then(a.facts.name.cmp(&b.facts.name))
        });
    match best {
        Some(n) => Ok(n),
        None => {
            let why: Vec<String> = nodes
                .iter()
                .filter_map(|n| ineligible(n, sel).map(|w| format!("{}: {w}", n.facts.name)))
                .collect();
            bail!(
                "no eligible node for a new session{}{}",
                if sel.is_empty() {
                    String::new()
                } else {
                    format!(" matching {sel}")
                },
                if why.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", why.join(", "))
                }
            )
        }
    }
}

/// Names of the nodes that have a session called `name`.
pub fn nodes_with_session<'a>(nodes: &'a [NodeStatus], name: &str) -> Vec<&'a NodeStatus> {
    nodes
        .iter()
        .filter(|n| n.facts.sessions.iter().any(|s| s.name == name))
        .collect()
}

/// The node running session `name`. Errors when no node or several do.
pub fn locate_session<'a>(nodes: &'a [NodeStatus], name: &str) -> Result<&'a NodeStatus> {
    let found = nodes_with_session(nodes, name);
    match found.as_slice() {
        [one] => Ok(one),
        [] => bail!("no session named {name} on any node (see `flotilla session ls`)"),
        many => bail!(
            "session {name} exists on {}; pass -n to choose",
            many.iter()
                .map(|n| n.facts.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotilla_core::schema::{NodeFacts, SessionInfo};

    fn node(name: &str, online: bool, load: f64, cpus: usize, sessions: &[&str]) -> NodeStatus {
        let mut f = NodeFacts {
            name: name.into(),
            node_id: format!("id-{name}"),
            load_1m: load,
            cpus,
            sessions: sessions
                .iter()
                .map(|s| SessionInfo {
                    name: s.to_string(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        f.labels.insert("tmux".into(), "yes".into());
        NodeStatus {
            facts: f,
            online,
            facts_age_secs: 1,
        }
    }

    fn sel(s: &str) -> Selector {
        s.parse().unwrap()
    }

    #[test]
    fn picks_lowest_load_per_core() {
        let nodes = vec![
            node("small", true, 1.0, 2, &[]),
            node("big", true, 2.0, 16, &[]),
            node("busy", true, 9.0, 8, &[]),
        ];
        assert_eq!(pick_node(&nodes, &sel("")).unwrap().facts.name, "big");
    }

    #[test]
    fn existing_sessions_count_against_a_node() {
        let nodes = vec![
            node("a", true, 0.0, 4, &["x", "y", "z", "w"]),
            node("b", true, 1.0, 4, &[]),
        ];
        // a: (0 + 4*0.5)/4 = 0.5; b: 0.25
        assert_eq!(pick_node(&nodes, &sel("")).unwrap().facts.name, "b");
    }

    #[test]
    fn skips_offline_untmuxed_full_and_unlabelled() {
        let mut nodes = vec![
            node("offline", false, 0.0, 8, &[]),
            node("full", true, 0.0, 8, &["a", "b"]),
            node("idle-no-role", true, 0.0, 8, &[]),
            node("ok", true, 3.0, 4, &[]),
        ];
        nodes[1].facts.max_sessions = Some(2);
        nodes[2].facts.labels.insert("role".into(), "dev".into());
        nodes[3].facts.labels.insert("role".into(), "build".into());
        let picked = pick_node(&nodes, &sel("role=build")).unwrap();
        assert_eq!(picked.facts.name, "ok");
        nodes[0].online = true;
        nodes[0].facts.labels.remove("tmux");
        let err = pick_node(&nodes[..1], &sel("")).unwrap_err().to_string();
        assert!(err.contains("offline") || err.contains("no tmux"), "{err}");
    }

    #[test]
    fn cap_is_honoured_and_reported() {
        let mut n = node("only", true, 0.0, 4, &["a"]);
        n.facts.max_sessions = Some(1);
        let err = pick_node(&[n], &sel("")).unwrap_err().to_string();
        assert!(err.contains("only: 1/1 sessions"), "{err}");
    }

    #[test]
    fn ties_go_to_fewer_sessions_then_name() {
        let nodes = vec![node("b", true, 0.0, 4, &[]), node("a", true, 0.0, 4, &[])];
        assert_eq!(pick_node(&nodes, &sel("")).unwrap().facts.name, "a");
    }

    #[test]
    fn locates_a_session_by_name_across_the_fleet() {
        let nodes = vec![
            node("one", true, 0.0, 4, &["codex"]),
            node("two", true, 0.0, 4, &["grok", "work"]),
            node("three", true, 0.0, 4, &["work"]),
        ];
        assert_eq!(locate_session(&nodes, "grok").unwrap().facts.name, "two");
        let dup = locate_session(&nodes, "work").unwrap_err().to_string();
        assert!(dup.contains("two, three") && dup.contains("-n"), "{dup}");
        assert!(locate_session(&nodes, "nope")
            .unwrap_err()
            .to_string()
            .contains("no session named nope"));
    }
}
