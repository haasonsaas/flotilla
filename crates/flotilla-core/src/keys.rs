//! Key layout for the replicated store. Every layer owns a prefix.

pub const NODE: &str = "node/";
pub const JOB: &str = "job/";
pub const CLAIM: &str = "claim/";
pub const RESULT: &str = "result/";
pub const DESIRED: &str = "desired/";
pub const RECONCILE: &str = "reconcile/";

pub fn node_facts(node: &str) -> String {
    format!("{NODE}{node}/facts")
}
pub fn job(id: &str) -> String {
    format!("{JOB}{id}")
}
pub fn claim(id: &str) -> String {
    format!("{CLAIM}{id}")
}
pub fn result(id: &str) -> String {
    format!("{RESULT}{id}")
}
pub fn desired(node: &str) -> String {
    format!("{DESIRED}{node}")
}
pub fn reconcile(node: &str) -> String {
    format!("{RECONCILE}{node}")
}

/// Extract the id from a prefixed key, e.g. `job/abc` -> `abc`.
pub fn id_of<'a>(prefix: &str, key: &'a str) -> Option<&'a str> {
    key.strip_prefix(prefix)
}
