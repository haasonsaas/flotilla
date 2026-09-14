use crate::hlc::Hlc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type NodeId = String;

/// One replicated key/value entry. Last writer wins, ordered by
/// `(hlc, author)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub key: String,
    pub value: Value,
    pub author: NodeId,
    pub hlc: Hlc,
    #[serde(default)]
    pub deleted: bool,
}

impl Record {
    pub fn version(&self) -> (Hlc, &str) {
        (self.hlc, self.author.as_str())
    }

    /// True if `self` should replace `other` under LWW.
    pub fn wins_over(&self, other: &Record) -> bool {
        self.version() > other.version()
    }

    pub fn parse<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(author: &str, hlc: u64) -> Record {
        Record {
            key: "k".into(),
            value: Value::Null,
            author: author.into(),
            hlc: Hlc(hlc),
            deleted: false,
        }
    }

    #[test]
    fn higher_hlc_wins() {
        assert!(rec("a", 2).wins_over(&rec("b", 1)));
        assert!(!rec("a", 1).wins_over(&rec("b", 2)));
    }

    #[test]
    fn equal_hlc_breaks_on_author() {
        assert!(rec("b", 1).wins_over(&rec("a", 1)));
        assert!(!rec("a", 1).wins_over(&rec("b", 1)));
        assert!(!rec("a", 1).wins_over(&rec("a", 1)));
    }
}
