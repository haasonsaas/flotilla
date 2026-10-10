use anyhow::{anyhow, Context, Result};
use flotilla_core::api::*;
use flotilla_core::Record;
use futures::{Stream, StreamExt};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use std::path::PathBuf;
use std::time::Duration;

const EXEC_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// A reqwest client that adds the local operator token, read fresh from the
/// token file on each request, so a daemon restart or first start after the
/// CLI began does not leave it unauthenticated. Only the client for the
/// local daemon carries a token path; `Client::at` never does, so the token
/// is not sent to peers.
#[derive(Clone)]
struct Http {
    inner: reqwest::Client,
    token_path: Option<PathBuf>,
}

impl Http {
    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let token = self
            .token_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok());
        match token.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
            Some(t) => match reqwest::header::HeaderValue::from_str(t) {
                Ok(mut v) => {
                    v.set_sensitive(true);
                    rb.header(LOCAL_TOKEN_HEADER, v)
                }
                Err(_) => rb,
            },
            None => rb,
        }
    }
    fn get(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.auth(self.inner.get(url))
    }
    fn post(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.auth(self.inner.post(url))
    }
    fn put(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.auth(self.inner.put(url))
    }
    fn delete(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.auth(self.inner.delete(url))
    }
}

fn build_http() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("client")
}

/// True when `base` points at this machine (so the token may be sent).
fn is_loopback_base(base: &str) -> bool {
    reqwest::Url::parse(base)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .is_some_and(|h| {
            h == "localhost"
                || h.trim_matches(|c| c == '[' || c == ']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        })
}

/// Where the daemon keeps the local operator token: `$FLOTILLA_DATA_DIR`,
/// else `data_dir` from the config file, else the default data directory.
pub fn token_path() -> Option<PathBuf> {
    let dir = std::env::var_os("FLOTILLA_DATA_DIR")
        .map(PathBuf::from)
        .or_else(crate::configured_data_dir)
        .or_else(|| {
            std::env::var_os("XDG_DATA_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    directories::BaseDirs::new().map(|b| b.home_dir().join(".local").join("share"))
                })
                .map(|d| d.join("flotilla"))
        })?;
    Some(dir.join(LOCAL_TOKEN_FILE))
}

#[derive(Clone)]
pub struct Client {
    pub base: String,
    http: Http,
}

impl Client {
    pub fn new(base: &str) -> Client {
        Client {
            base: base.trim_end_matches('/').to_string(),
            http: Http {
                inner: build_http(),
                token_path: if is_loopback_base(base) {
                    token_path()
                } else {
                    None
                },
            },
        }
    }

    /// A client pointed at another node. Peers authenticate us by tailnet
    /// identity, so the local token is deliberately not carried over.
    pub fn at(&self, base: &str) -> Client {
        Client {
            base: base.trim_end_matches('/').to_string(),
            http: Http {
                inner: self.http.inner.clone(),
                token_path: None,
            },
        }
    }

    async fn check(resp: reqwest::Response) -> Result<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let text = resp.text().await.unwrap_or_default();
        let msg = serde_json::from_str::<ErrorBody>(&text)
            .map(|e| e.error)
            .unwrap_or(text);
        Err(anyhow!("{status}: {msg}"))
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let resp = self
            .http
            .get(format!("{}{path}", self.base))
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .with_context(|| format!("connecting to {}", self.base))?;
        Ok(Self::check(resp).await?.json().await?)
    }

    pub async fn status(&self) -> Result<StatusResponse> {
        self.get("/v1/status").await
    }
    pub async fn me(&self) -> Result<SelfInfo> {
        self.get("/v1/self").await
    }
    pub async fn peers(&self) -> Result<PeersResponse> {
        self.get("/v1/peers").await
    }

    /// Subscribe to store changes under `prefix` as they happen.
    pub async fn events(&self, prefix: &str) -> Result<impl Stream<Item = Result<RecordEvent>>> {
        let resp = self
            .http
            .get(format!("{}/v1/events", self.base))
            .query(&[("prefix", prefix)])
            .send()
            .await
            .with_context(|| format!("connecting to {}", self.base))?;
        let resp = Self::check(resp).await?;
        let mut buf = String::new();
        Ok(resp.bytes_stream().flat_map(move |chunk| {
            let mut out = Vec::new();
            match chunk {
                Ok(bytes) => {
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    while let Some(i) = buf.find("\n\n") {
                        let frame: String = buf.drain(..i + 2).collect();
                        let mut is_record = false;
                        let mut data = String::new();
                        for line in frame.lines() {
                            if let Some(v) = line.strip_prefix("event:") {
                                is_record = v.trim() == "record";
                            } else if let Some(v) = line.strip_prefix("data:") {
                                data.push_str(v.trim());
                            }
                        }
                        if is_record && !data.is_empty() {
                            out.push(
                                serde_json::from_str::<RecordEvent>(&data).map_err(Into::into),
                            );
                        }
                    }
                }
                Err(e) => out.push(Err(e.into())),
            }
            futures::stream::iter(out)
        }))
    }

    pub async fn wake(&self, req: &WakeRequest) -> Result<WakeResponse> {
        let resp = self
            .http
            .post(format!("{}/v1/wake", self.base))
            .json(req)
            .timeout(Duration::from_secs(15))
            .send()
            .await?;
        Ok(Self::check(resp).await?.json().await?)
    }

    pub async fn sync_state(&self) -> Result<SyncStateResponse> {
        self.get("/v1/syncstate").await
    }

    pub async fn sync_now(&self, peer: Option<String>) -> Result<Vec<SyncNowResult>> {
        let resp = self
            .http
            .post(format!("{}/v1/syncnow", self.base))
            .json(&SyncNowRequest { peer })
            .timeout(Duration::from_secs(120))
            .send()
            .await?;
        Ok(Self::check(resp).await?.json().await?)
    }

    pub async fn list(&self, prefix: &str, raw: bool) -> Result<Vec<Record>> {
        let r: RecordsResponse = self
            .get(&format!(
                "/v1/records?prefix={}&raw={raw}",
                urlencode(prefix)
            ))
            .await?;
        Ok(r.records)
    }

    pub async fn get_record(&self, key: &str) -> Result<Option<Record>> {
        let resp = self
            .http
            .get(format!("{}/v1/records/{key}", self.base))
            .timeout(Duration::from_secs(15))
            .send()
            .await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::check(resp).await?.json().await?))
    }

    pub async fn put_record(&self, key: &str, value: serde_json::Value) -> Result<Record> {
        let resp = self
            .http
            .put(format!("{}/v1/records/{key}", self.base))
            .json(&PutRecordRequest { value })
            .timeout(Duration::from_secs(15))
            .send()
            .await?;
        Ok(Self::check(resp).await?.json().await?)
    }

    pub async fn put_json<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<Record> {
        self.put_record(key, serde_json::to_value(value)?).await
    }

    pub async fn delete_record(&self, key: &str) -> Result<Option<Record>> {
        let resp = self
            .http
            .delete(format!("{}/v1/records/{key}", self.base))
            .timeout(Duration::from_secs(15))
            .send()
            .await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::check(resp).await?.json().await?))
    }

    /// Stream exec frames from a node. No overall timeout: the daemon
    /// enforces `timeout_secs` on the process.
    pub async fn exec(&self, req: &ExecRequest) -> Result<impl Stream<Item = Result<ExecFrame>>> {
        let resp = self
            .http
            .post(format!("{}/v1/exec", self.base))
            .json(req)
            .send()
            .await
            .with_context(|| format!("connecting to {}", self.base))?;
        let resp = Self::check(resp).await?;
        let mut buf: Vec<u8> = Vec::new();
        // The daemon sends a keepalive frame every 10s while the process is
        // silent, so a minute without bytes means the peer is gone.
        let chunks = tokio_stream::StreamExt::timeout(resp.bytes_stream(), EXEC_IDLE_TIMEOUT);
        Ok(chunks
            .flat_map(move |chunk| {
                let mut frames = Vec::new();
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(_) => {
                        frames.push(Err(anyhow!(
                            "no data from peer for {:?}; connection presumed dead",
                            EXEC_IDLE_TIMEOUT
                        )));
                        return futures::stream::iter(frames);
                    }
                };
                match chunk {
                    Ok(bytes) => {
                        buf.extend_from_slice(&bytes);
                        while let Some(i) = buf.iter().position(|&c| c == b'\n') {
                            let line: Vec<u8> = buf.drain(..=i).collect();
                            match serde_json::from_slice::<ExecFrame>(&line[..line.len() - 1]) {
                                Ok(ExecFrame::Keepalive) => {}
                                other => frames.push(other.map_err(Into::into)),
                            }
                        }
                    }
                    Err(e) => frames.push(Err(e.into())),
                }
                futures::stream::iter(frames)
            })
            .boxed())
    }

    pub async fn artifacts(&self, id: &str) -> Result<ArtifactsResponse> {
        self.get(&format!("/v1/jobs/{id}/artifacts")).await
    }

    pub async fn artifact(&self, id: &str, path: &str, local: &std::path::Path) -> Result<u64> {
        use tokio::io::AsyncWriteExt;
        let resp = self
            .http
            .get(format!("{}/v1/jobs/{id}/artifacts/{path}", self.base))
            .timeout(Duration::from_secs(600))
            .send()
            .await?;
        let resp = Self::check(resp).await?;
        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        let mut f = tokio::fs::File::create(local)
            .await
            .with_context(|| format!("creating {}", local.display()))?;
        let mut n = 0u64;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            n += chunk.len() as u64;
            f.write_all(&chunk).await?;
        }
        f.flush().await?;
        Ok(n)
    }

    pub async fn import(&self, records: Vec<Record>) -> Result<ImportResponse> {
        let resp = self
            .http
            .post(format!("{}/v1/records/import", self.base))
            .json(&ImportRequest { records })
            .timeout(Duration::from_secs(300))
            .send()
            .await?;
        Ok(Self::check(resp).await?.json().await?)
    }

    pub async fn job_log(&self, id: &str) -> Result<Option<String>> {
        let resp = self
            .http
            .get(format!("{}/v1/jobs/{id}/log", self.base))
            .timeout(Duration::from_secs(30))
            .send()
            .await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::check(resp).await?.text().await?))
    }

    /// The job log from byte `from` on, or None while no log exists yet.
    pub async fn job_log_from(&self, id: &str, from: u64) -> Result<Option<Vec<u8>>> {
        let resp = self
            .http
            .get(format!("{}/v1/jobs/{id}/log", self.base))
            .query(&[("from", from)])
            .timeout(Duration::from_secs(30))
            .send()
            .await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::check(resp).await?.bytes().await?.to_vec()))
    }

    /// Upload a local file to a path on the node.
    pub async fn put_file(
        &self,
        local: &std::path::Path,
        remote: &str,
        mode: Option<&str>,
    ) -> Result<FileWriteResponse> {
        let f = tokio::fs::File::open(local)
            .await
            .with_context(|| format!("opening {}", local.display()))?;
        let len = f.metadata().await?.len();
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(f));
        let mut q = vec![("path", remote.to_string())];
        if let Some(m) = mode {
            q.push(("mode", m.to_string()));
        }
        let resp = self
            .http
            .put(format!("{}/v1/files", self.base))
            .query(&q)
            .header(reqwest::header::CONTENT_LENGTH, len)
            .body(body)
            .timeout(Duration::from_secs(600))
            .send()
            .await
            .with_context(|| format!("connecting to {}", self.base))?;
        Ok(Self::check(resp).await?.json().await?)
    }

    /// Download a path on the node into a local file. Returns bytes written.
    pub async fn get_file(&self, remote: &str, local: &std::path::Path) -> Result<u64> {
        use tokio::io::AsyncWriteExt;
        let resp = self
            .http
            .get(format!("{}/v1/files", self.base))
            .query(&[("path", remote)])
            .timeout(Duration::from_secs(600))
            .send()
            .await?;
        let resp = Self::check(resp).await?;
        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        let mut f = tokio::fs::File::create(local)
            .await
            .with_context(|| format!("creating {}", local.display()))?;
        let mut n = 0u64;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            n += chunk.len() as u64;
            f.write_all(&chunk).await?;
        }
        f.flush().await?;
        Ok(n)
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Wait without a timer primitive.
pub async fn pause(d: Duration) {
    let _ = tokio::time::timeout(d, std::future::pending::<()>()).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_only_for_loopback_bases() {
        assert!(is_loopback_base("http://127.0.0.1:7400"));
        assert!(is_loopback_base("http://localhost:7400"));
        assert!(is_loopback_base("http://[::1]:7400"));
        assert!(!is_loopback_base("http://100.64.0.1:7400"));
        assert!(!is_loopback_base("http://host.example.com:7400"));
    }

    #[test]
    fn peers_never_get_the_token() {
        let c = Client {
            base: "http://127.0.0.1:7400".into(),
            http: Http {
                inner: build_http(),
                token_path: Some(PathBuf::from("/nonexistent/local.token")),
            },
        };
        assert!(c.http.token_path.is_some());
        assert!(c.at("http://100.64.0.2:7400").http.token_path.is_none());
    }

    #[test]
    fn token_is_read_from_file_per_request() {
        let dir = std::env::temp_dir().join(format!("flotilla-cli-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(LOCAL_TOKEN_FILE);
        let http = Http {
            inner: build_http(),
            token_path: Some(path.clone()),
        };
        let hdr = |h: &Http| {
            h.get("http://127.0.0.1:1/x")
                .build()
                .unwrap()
                .headers()
                .get(LOCAL_TOKEN_HEADER)
                .map(|v| v.to_str().unwrap().to_string())
        };
        assert_eq!(hdr(&http), None, "no file yet");
        std::fs::write(&path, "abc\n").unwrap();
        assert_eq!(hdr(&http).as_deref(), Some("abc"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
