//! Label selectors: `key=value` pairs that must all match a node's labels.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

pub type Labels = BTreeMap<String, String>;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Selector(pub Labels);

impl Selector {
    pub fn matches(&self, labels: &Labels) -> bool {
        self.0.iter().all(|(k, v)| labels.get(k) == Some(v))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromStr for Selector {
    type Err = String;

    /// Parse `a=b,c=d`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut out = Labels::new();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (k, v) = part
                .split_once('=')
                .ok_or_else(|| format!("expected key=value, got {part:?}"))?;
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
        Ok(Selector(out))
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.0.iter().map(|(k, v)| format!("{k}={v}")).collect();
        write!(f, "{}", parts.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parses_and_matches() {
        let sel: Selector = "os=macos, arch=arm64".parse().unwrap();
        assert!(sel.matches(&labels(&[
            ("os", "macos"),
            ("arch", "arm64"),
            ("extra", "x")
        ])));
        assert!(!sel.matches(&labels(&[("os", "macos")])));
        assert!(!sel.matches(&labels(&[("os", "linux"), ("arch", "arm64")])));
        assert_eq!(sel.to_string(), "arch=arm64,os=macos");
    }

    #[test]
    fn empty_matches_everything() {
        let sel: Selector = "".parse().unwrap();
        assert!(sel.is_empty());
        assert!(sel.matches(&Labels::new()));
    }

    #[test]
    fn rejects_bad_input() {
        assert!("os".parse::<Selector>().is_err());
    }
}
