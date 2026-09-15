use anyhow::{anyhow, Context, Result};
use flotilla_core::api::*;
use flotilla_core::Record;
use futures::{Stream, StreamExt};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use std::time::Duration;

#[derive(Clone)]
pub struct Client {
    pub base: String,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: &str) -> Client {
        Client {
            base: base.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .build()
                .expect("client"),
        }
    }

    /// A client pointed at another node.
    pub fn at(&self, base: &str) -> Client {
        Client {
            base: base.trim_end_matches('/').to_string(),
            http: self.http.clone(),
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
        Ok(resp.bytes_stream().flat_map(move |chunk| {
            let mut frames = Vec::new();
            match chunk {
                Ok(bytes) => {
                    buf.extend_from_slice(&bytes);
                    while let Some(i) = buf.iter().position(|&c| c == b'\n') {
                        let line: Vec<u8> = buf.drain(..=i).collect();
                        frames.push(
                            serde_json::from_slice::<ExecFrame>(&line[..line.len() - 1])
                                .map_err(Into::into),
                        );
                    }
                }
                Err(e) => frames.push(Err(e.into())),
            }
            futures::stream::iter(frames)
        }))
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
