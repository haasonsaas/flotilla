//! PROXY protocol (v1 and v2) listener for tailscaled instances running in
//! userspace-networking mode. Their Tailscale addresses are not local
//! interfaces, so the daemon cannot bind them; instead
//! `tailscale serve --tcp=7400 --proxy-protocol=2 tcp://127.0.0.1:<port>`
//! forwards tailnet connections to a loopback port, prefixed with a header
//! that carries the caller's real tailnet address. That address is what
//! `whois` needs to identify the caller.

use anyhow::{bail, Context, Result};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

const V2_SIG: [u8; 12] = *b"\r\n\r\n\0\r\nQUIT\n";
const HEADER_TIMEOUT: Duration = Duration::from_secs(3);

/// Read a PROXY header from `r`, consuming exactly its bytes. Returns the
/// original source address, or None for a header that carries none (v2
/// LOCAL, v1 UNKNOWN); the caller then keeps the TCP peer address.
pub async fn read_header<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<SocketAddr>> {
    let mut first = [0u8; 12];
    r.read_exact(&mut first[..5]).await?;
    if &first[..5] == b"PROXY" {
        return read_v1(r).await;
    }
    r.read_exact(&mut first[5..]).await?;
    if first != V2_SIG {
        bail!("not a PROXY protocol header");
    }
    let mut rest = [0u8; 4];
    r.read_exact(&mut rest).await?;
    let (ver_cmd, fam, len) = (
        rest[0],
        rest[1],
        u16::from_be_bytes([rest[2], rest[3]]) as usize,
    );
    if ver_cmd >> 4 != 2 {
        bail!("unsupported PROXY version {}", ver_cmd >> 4);
    }
    if len > 536 {
        bail!("PROXY header too long");
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    if ver_cmd & 0x0f == 0 {
        return Ok(None); // LOCAL: health check from the proxy itself
    }
    parse_v2_addr(fam, &body)
}

fn parse_v2_addr(fam: u8, b: &[u8]) -> Result<Option<SocketAddr>> {
    match fam >> 4 {
        1 if b.len() >= 12 => {
            let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
            let port = u16::from_be_bytes([b[8], b[9]]);
            Ok(Some(SocketAddr::new(IpAddr::V4(ip), port)))
        }
        2 if b.len() >= 36 => {
            let mut o = [0u8; 16];
            o.copy_from_slice(&b[..16]);
            let port = u16::from_be_bytes([b[32], b[33]]);
            Ok(Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(o)), port)))
        }
        0 => Ok(None),
        _ => bail!("unsupported PROXY address family {fam:#x}"),
    }
}

async fn read_v1<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<SocketAddr>> {
    // "PROXY" already consumed; the rest of the line is at most 102 bytes.
    let mut line = Vec::new();
    loop {
        let mut b = [0u8; 1];
        r.read_exact(&mut b).await?;
        line.push(b[0]);
        if line.ends_with(b"\r\n") {
            break;
        }
        if line.len() > 107 {
            bail!("PROXY v1 line too long");
        }
    }
    let text = String::from_utf8_lossy(&line[..line.len() - 2]).into_owned();
    let parts: Vec<&str> = text.split_whitespace().collect();
    match parts.as_slice() {
        ["UNKNOWN", ..] => Ok(None),
        [proto, src, _dst, sport, _dport] if *proto == "TCP4" || *proto == "TCP6" => {
            let ip: IpAddr = src.parse().context("PROXY v1 source address")?;
            let port: u16 = sport.parse().context("PROXY v1 source port")?;
            Ok(Some(SocketAddr::new(ip, port)))
        }
        _ => bail!("bad PROXY v1 line {text:?}"),
    }
}

/// An axum listener that accepts on a loopback port and yields each
/// connection with the source address from its PROXY header. Header reads
/// happen in their own tasks so a slow client cannot stall accepts.
pub struct ProxyListener {
    rx: mpsc::Receiver<(TcpStream, SocketAddr)>,
    local: SocketAddr,
}

impl ProxyListener {
    pub fn new(inner: TcpListener) -> std::io::Result<ProxyListener> {
        let local = inner.local_addr()?;
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let (mut stream, peer) = match inner.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::warn!(error = %e, "proxy listener accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    match tokio::time::timeout(HEADER_TIMEOUT, read_header(&mut stream)).await {
                        Ok(Ok(src)) => {
                            let _ = tx.send((stream, src.unwrap_or(peer))).await;
                        }
                        Ok(Err(e)) => tracing::debug!(%peer, error = %e, "bad PROXY header"),
                        Err(_) => tracing::debug!(%peer, "PROXY header timed out"),
                    }
                });
            }
        });
        Ok(ProxyListener { rx, local })
    }
}

impl axum::serve::Listener for ProxyListener {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(x) => x,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn v1_tcp4() {
        let mut b: &[u8] = b"PROXY TCP4 100.64.1.2 100.64.9.9 51234 7400\r\nGET / HTTP/1.1\r\n";
        let a = read_header(&mut b).await.unwrap().unwrap();
        assert_eq!(a, "100.64.1.2:51234".parse().unwrap());
        assert!(b.starts_with(b"GET /"), "only the header is consumed");
    }

    #[tokio::test]
    async fn v1_unknown_and_garbage() {
        let mut b: &[u8] = b"PROXY UNKNOWN\r\n";
        assert!(read_header(&mut b).await.unwrap().is_none());
        let mut g: &[u8] = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n";
        assert!(read_header(&mut g).await.is_err());
    }

    #[tokio::test]
    async fn v2_ipv4_and_ipv6() {
        let mut h = V2_SIG.to_vec();
        h.extend([
            0x21, 0x11, 0, 12, 100, 64, 1, 2, 100, 64, 9, 9, 0xc8, 0x1e, 0x1c, 0xe8,
        ]);
        h.extend(b"HTTP");
        let mut b: &[u8] = &h;
        let a = read_header(&mut b).await.unwrap().unwrap();
        assert_eq!(a, "100.64.1.2:51230".parse().unwrap());
        assert_eq!(b, b"HTTP");

        let mut h = V2_SIG.to_vec();
        h.extend([0x21, 0x21, 0, 36]);
        let src: Ipv6Addr = "fd7a:115c:a1e0::1".parse().unwrap();
        h.extend(src.octets());
        h.extend(Ipv6Addr::LOCALHOST.octets());
        h.extend([0x00, 0x50, 0x1c, 0xe8]);
        let mut b: &[u8] = &h;
        let a = read_header(&mut b).await.unwrap().unwrap();
        assert_eq!(a, SocketAddr::new(IpAddr::V6(src), 80));
    }

    #[tokio::test]
    async fn v2_local_has_no_source() {
        let mut h = V2_SIG.to_vec();
        h.extend([0x20, 0x00, 0, 0]);
        let mut b: &[u8] = &h;
        assert!(read_header(&mut b).await.unwrap().is_none());
    }
}
