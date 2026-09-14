//! Run a process and stream its output as `ExecFrame`s. Shared by the HTTP
//! exec endpoint and the job runner.

use flotilla_core::api::{ExecFrame, ExecRequest};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Spawn the command and return a receiver of frames. The final frame is
/// always `Exit` (or `Error` followed by `Exit { code: None }`). Cancelling
/// the token kills the child.
pub fn spawn(req: ExecRequest, cancel: CancellationToken) -> mpsc::Receiver<ExecFrame> {
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(async move {
        run(req, cancel, tx).await;
    });
    rx
}

async fn run(req: ExecRequest, cancel: CancellationToken, tx: mpsc::Sender<ExecFrame>) {
    if req.cmd.is_empty() {
        let _ = tx.send(ExecFrame::Error { message: "empty command".into() }).await;
        let _ = tx.send(ExecFrame::Exit { code: None }).await;
        return;
    }
    let mut cmd = Command::new(&req.cmd[0]);
    cmd.args(&req.cmd[1..]).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    if let Some(cwd) = &req.cwd {
        cmd.current_dir(expand_home(cwd));
    }
    for (k, v) in &req.env {
        cmd.env(k, v);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(ExecFrame::Error { message: format!("spawn {:?}: {e}", req.cmd[0]) }).await;
            let _ = tx.send(ExecFrame::Exit { code: None }).await;
            return;
        }
    };
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    let out_task = pump(stdout, tx.clone(), |d| ExecFrame::Stdout { data: d });
    let err_task = pump(stderr, tx.clone(), |d| ExecFrame::Stderr { data: d });

    let timeout = req.timeout_secs.map(Duration::from_secs);
    let wait = async {
        match timeout {
            Some(t) => tokio::time::timeout(t, child.wait()).await.ok(),
            None => Some(child.wait().await),
        }
    };
    let status = tokio::select! {
        s = wait => s,
        _ = cancel.cancelled() => None,
    };
    let code = match status {
        Some(Ok(st)) => st.code(),
        Some(Err(e)) => {
            let _ = tx.send(ExecFrame::Error { message: format!("wait: {e}") }).await;
            None
        }
        None => {
            let _ = child.kill().await;
            let why = if cancel.is_cancelled() { "cancelled" } else { "timed out" };
            let _ = tx.send(ExecFrame::Error { message: why.into() }).await;
            None
        }
    };
    let _ = tokio::join!(out_task, err_task);
    let _ = tx.send(ExecFrame::Exit { code }).await;
}

async fn pump<R: tokio::io::AsyncRead + Unpin>(reader: R, tx: mpsc::Sender<ExecFrame>, mk: impl Fn(String) -> ExecFrame) {
    let mut lines = BufReader::new(reader);
    let mut buf = Vec::with_capacity(4096);
    loop {
        buf.clear();
        // read_until keeps the newline so consumers can reassemble exactly
        match lines.read_until(b'\n', &mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                if tx.send(mk(String::from_utf8_lossy(&buf).into_owned())).await.is_err() {
                    // consumer went away; drain to avoid blocking the child
                    let mut sink = Vec::new();
                    let _ = lines.read_to_end(&mut sink).await;
                    break;
                }
            }
        }
    }
}

pub fn expand_home(p: &str) -> String {
    if let Some(rest) = p.strip_prefix("~/") {
        return crate::config::home().join(rest).to_string_lossy().into_owned();
    }
    if p == "~" {
        return crate::config::home().to_string_lossy().into_owned();
    }
    p.to_string()
}

/// A bounded tail of output, for job results.
pub struct Tail {
    buf: String,
    max: usize,
}

impl Tail {
    pub fn new(max: usize) -> Tail {
        Tail { buf: String::new(), max }
    }
    pub fn push(&mut self, s: &str) {
        self.buf.push_str(s);
        if self.buf.len() > self.max {
            let cut = self.buf.len() - self.max;
            let cut = self.buf.char_indices().map(|(i, _)| i).find(|&i| i >= cut).unwrap_or(cut);
            self.buf.drain(..cut);
        }
    }
    pub fn into_string(self) -> String {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A process that blocks until killed, without any timed wait.
    fn block_forever() -> Vec<String> {
        vec!["tail".into(), "-f".into(), "/dev/null".into()]
    }

    async fn collect(req: ExecRequest) -> Vec<ExecFrame> {
        let mut rx = spawn(req, CancellationToken::new());
        let mut out = Vec::new();
        while let Some(f) = rx.recv().await {
            out.push(f);
        }
        out
    }

    #[tokio::test]
    async fn streams_stdout_stderr_and_exit() {
        let frames = collect(ExecRequest {
            cmd: vec!["sh".into(), "-c".into(), "echo out; echo err 1>&2; exit 3".into()],
            ..Default::default()
        })
        .await;
        assert!(frames.contains(&ExecFrame::Stdout { data: "out\n".into() }));
        assert!(frames.contains(&ExecFrame::Stderr { data: "err\n".into() }));
        assert_eq!(frames.last(), Some(&ExecFrame::Exit { code: Some(3) }));
    }

    #[tokio::test]
    async fn missing_binary_reports_error() {
        let frames = collect(ExecRequest { cmd: vec!["/definitely/not/here".into()], ..Default::default() }).await;
        assert!(matches!(frames[0], ExecFrame::Error { .. }));
        assert_eq!(frames.last(), Some(&ExecFrame::Exit { code: None }));
    }

    #[tokio::test]
    async fn timeout_kills_child() {
        let start = std::time::Instant::now();
        let frames = collect(ExecRequest { cmd: block_forever(), timeout_secs: Some(1), ..Default::default() }).await;
        assert!(start.elapsed() < Duration::from_secs(10));
        assert!(frames.iter().any(|f| matches!(f, ExecFrame::Error { message } if message == "timed out")));
        assert_eq!(frames.last(), Some(&ExecFrame::Exit { code: None }));
    }

    #[tokio::test]
    async fn cancel_kills_child() {
        let cancel = CancellationToken::new();
        let mut rx = spawn(ExecRequest { cmd: block_forever(), ..Default::default() }, cancel.clone());
        cancel.cancel();
        let mut frames = Vec::new();
        while let Some(f) = rx.recv().await {
            frames.push(f);
        }
        assert!(frames.iter().any(|f| matches!(f, ExecFrame::Error { message } if message == "cancelled")));
    }

    #[tokio::test]
    async fn cwd_and_env_apply() {
        let frames = collect(ExecRequest {
            cmd: vec!["sh".into(), "-c".into(), "pwd; echo $FLOT".into()],
            cwd: Some("/".into()),
            env: [("FLOT".to_string(), "yes".to_string())].into_iter().collect(),
            ..Default::default()
        })
        .await;
        assert!(frames.contains(&ExecFrame::Stdout { data: "/\n".into() }));
        assert!(frames.contains(&ExecFrame::Stdout { data: "yes\n".into() }));
    }

    #[test]
    fn tail_keeps_last_bytes() {
        let mut t = Tail::new(5);
        t.push("abcdefgh");
        assert_eq!(t.into_string(), "defgh");
    }
}
