//! Streamed discovery, `GET /peers?ih=…` (docs/discovery-api.md): every
//! tracker and the DHT are queried in parallel, and each peer address is
//! written as one NDJSON line the moment a source returns it, deduplicated
//! and filtered (public addresses, ports >= 1024). The response closes when
//! every source has finished or at the stream deadline (~30 s), whichever
//! comes first. No probing, no tokens.
//!
//! A finished lookup is cached for 5 minutes (30 s if empty): a second
//! request for the same info-hash gets the cached list at once.
//!
//! Abuse limits (OQ-14), checked before any lookup starts: requests per
//! minute and concurrent streams per client IP, and concurrent streams in
//! total. Logs carry no info-hash, magnet or IP.

use crate::cache::PeerCache;
use crate::config::Config;
use crate::lookup::{Lookup, LookupResult};
use crate::netguard;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::JoinHandle;
use tokio::time::Sleep;

/// The sources stop this long before the stream's deadline, so a lookup
/// that runs to the deadline still completes (and is cached).
const SOURCE_SLACK: Duration = Duration::from_millis(500);
const WINDOW: Duration = Duration::from_secs(60);
const MAX_TRACKED_IPS: usize = 10_000;

#[derive(Debug, Clone, Copy)]
pub struct StreamLimits {
    pub per_ip_per_minute: u32,
    pub per_ip_streams: u32,
    pub total_streams: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    TotalStreams,
    StreamsFromIp,
    RequestsFromIp,
}

impl Refusal {
    pub fn code(self) -> &'static str {
        match self {
            Refusal::TotalStreams => "too_many_streams",
            Refusal::StreamsFromIp => "too_many_streams_from_client",
            Refusal::RequestsFromIp => "rate_limited",
        }
    }
}

#[derive(Default)]
struct LimiterState {
    windows: HashMap<IpAddr, (Instant, u32)>,
    active: HashMap<IpAddr, u32>,
    total: u32,
}

pub struct Limiter {
    limits: StreamLimits,
    state: Mutex<LimiterState>,
}

/// One admitted stream; dropping it gives its slots back.
pub struct Permit {
    limiter: Arc<Limiter>,
    ip: IpAddr,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut st = self.limiter.lock();
        st.total = st.total.saturating_sub(1);
        if let Some(n) = st.active.get_mut(&self.ip) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                st.active.remove(&self.ip);
            }
        }
    }
}

impl Limiter {
    pub fn new(limits: StreamLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(LimiterState::default()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LimiterState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Admit one request from `ip`, or say which limit it hit. A refused
    /// request does not count against the per-minute budget.
    pub fn try_acquire(self: &Arc<Self>, ip: IpAddr, now: Instant) -> Result<Permit, Refusal> {
        let mut st = self.lock();
        if st.total >= self.limits.total_streams {
            return Err(Refusal::TotalStreams);
        }
        if st.active.get(&ip).copied().unwrap_or(0) >= self.limits.per_ip_streams {
            return Err(Refusal::StreamsFromIp);
        }
        if st.windows.len() >= MAX_TRACKED_IPS && !st.windows.contains_key(&ip) {
            st.windows
                .retain(|_, (start, _)| now.duration_since(*start) < WINDOW);
        }
        let window = st.windows.entry(ip).or_insert((now, 0));
        if now.duration_since(window.0) >= WINDOW {
            *window = (now, 0);
        }
        if window.1 >= self.limits.per_ip_per_minute {
            return Err(Refusal::RequestsFromIp);
        }
        window.1 += 1;
        *st.active.entry(ip).or_insert(0) += 1;
        st.total += 1;
        Ok(Permit {
            limiter: self.clone(),
            ip,
        })
    }
}

/// Whether discovery hands out this address: a public IP the gateways may
/// dial (they refuse ports below 1024).
pub fn is_dialable(addr: &SocketAddr) -> bool {
    addr.port() >= 1024 && netguard::is_public(addr.ip())
}

/// `{"peer":"1.2.3.4:6881"}\n`
pub fn peer_line(addr: &SocketAddr) -> String {
    format!("{{\"peer\":\"{addr}\"}}\n")
}

/// The last line: how many peers were sent, whether they came from the
/// cache, and whether every source finished (false: cut at the deadline).
pub fn end_line(count: usize, cached: bool, complete: bool) -> String {
    format!("{{\"done\":true,\"count\":{count},\"cached\":{cached},\"complete\":{complete}}}\n")
}

/// The whole body for a cached result.
pub fn cached_body(result: &LookupResult) -> Bytes {
    let mut seen = HashSet::new();
    let mut out = String::new();
    for addr in result.peers.iter().filter(|a| is_dialable(a)) {
        if seen.insert(*addr) {
            out.push_str(&peer_line(addr));
        }
    }
    out.push_str(&end_line(seen.len(), true, true));
    Bytes::from(out)
}

/// Aborts the lookup task when the response is dropped (client gone).
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct LiveStream {
    rx: UnboundedReceiver<Vec<SocketAddr>>,
    seen: HashSet<SocketAddr>,
    deadline: Pin<Box<Sleep>>,
    finished: bool,
    _task: AbortOnDrop,
    _permit: Permit,
}

/// Start a lookup and return its NDJSON body as a stream of chunks.
pub fn live(
    lookup: Arc<Lookup>,
    cache: Arc<PeerCache>,
    cfg: Arc<Config>,
    info_hash: [u8; 20],
    trackers: Vec<String>,
    permit: Permit,
) -> impl futures_lite::Stream<Item = Result<Bytes, Infallible>> + Send + 'static {
    let (tx, rx) = unbounded_channel();
    let stream_deadline = cfg.peers_stream_deadline;
    let sources_deadline = stream_deadline.saturating_sub(SOURCE_SLACK);
    let task = tokio::spawn(async move {
        let result = lookup
            .run_streaming_for(info_hash, trackers, &cfg, sources_deadline, Some(tx))
            .await;
        cache.insert(info_hash, Arc::new(result)).await;
    });
    let state = LiveStream {
        rx,
        seen: HashSet::new(),
        deadline: Box::pin(tokio::time::sleep(stream_deadline)),
        finished: false,
        _task: AbortOnDrop(task),
        _permit: permit,
    };
    futures_lite::stream::unfold(state, |mut st| async move {
        if st.finished {
            return None;
        }
        loop {
            tokio::select! {
                batch = st.rx.recv() => match batch {
                    Some(batch) => {
                        let mut chunk = String::new();
                        for addr in batch.iter().filter(|a| is_dialable(a)) {
                            if st.seen.insert(*addr) {
                                chunk.push_str(&peer_line(addr));
                            }
                        }
                        if !chunk.is_empty() {
                            return Some((Ok(Bytes::from(chunk)), st));
                        }
                    }
                    None => {
                        st.finished = true;
                        let end = end_line(st.seen.len(), false, true);
                        return Some((Ok(Bytes::from(end)), st));
                    }
                },
                _ = st.deadline.as_mut() => {
                    st.finished = true;
                    let end = end_line(st.seen.len(), false, false);
                    return Some((Ok(Bytes::from(end)), st));
                }
            }
        }
    })
}

/// The client IP for the limits: the trusted proxy's header (nginx's
/// `X-Real-IP`) when the request comes from loopback, else the TCP peer.
pub fn client_ip(peer: Option<SocketAddr>, header: Option<&str>) -> IpAddr {
    let peer_ip = peer.map(|p| p.ip());
    match (peer_ip, header) {
        (Some(ip), Some(value)) if ip.is_loopback() => value.trim().parse::<IpAddr>().unwrap_or(ip),
        (Some(ip), _) => ip,
        (None, _) => IpAddr::from([0, 0, 0, 0]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(per_min: u32, per_ip: u32, total: u32) -> Arc<Limiter> {
        Limiter::new(StreamLimits {
            per_ip_per_minute: per_min,
            per_ip_streams: per_ip,
            total_streams: total,
        })
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn limits_concurrent_streams_per_ip_and_in_total() {
        let l = limiter(100, 2, 3);
        let now = Instant::now();
        let a1 = l.try_acquire(ip("1.1.1.1"), now).unwrap();
        let _a2 = l.try_acquire(ip("1.1.1.1"), now).unwrap();
        assert_eq!(
            l.try_acquire(ip("1.1.1.1"), now).err(),
            Some(Refusal::StreamsFromIp)
        );
        let _b = l.try_acquire(ip("2.2.2.2"), now).unwrap();
        assert_eq!(
            l.try_acquire(ip("3.3.3.3"), now).err(),
            Some(Refusal::TotalStreams)
        );
        drop(a1);
        assert!(l.try_acquire(ip("1.1.1.1"), now).is_ok());
    }

    #[test]
    fn limits_requests_per_minute_per_ip() {
        let l = limiter(3, 10, 10);
        let now = Instant::now();
        for _ in 0..3 {
            drop(l.try_acquire(ip("1.1.1.1"), now).unwrap());
        }
        assert_eq!(
            l.try_acquire(ip("1.1.1.1"), now).err(),
            Some(Refusal::RequestsFromIp)
        );
        assert!(l.try_acquire(ip("2.2.2.2"), now).is_ok());
        assert!(l
            .try_acquire(ip("1.1.1.1"), now + Duration::from_secs(60))
            .is_ok());
    }

    #[test]
    fn filters_private_reserved_and_low_ports() {
        for ok in ["8.8.8.8:6881", "[2001:4860::1]:51413"] {
            assert!(is_dialable(&ok.parse().unwrap()), "{ok}");
        }
        for bad in [
            "10.0.0.1:6881",
            "127.0.0.1:6881",
            "192.168.1.1:6881",
            "100.64.0.1:6881",
            "[::1]:6881",
            "[fe80::1]:6881",
            "8.8.8.8:80",
            "8.8.8.8:0",
        ] {
            assert!(!is_dialable(&bad.parse().unwrap()), "{bad}");
        }
    }

    #[test]
    fn lines_are_ndjson() {
        let v4: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        for line in [peer_line(&v4), peer_line(&v6), end_line(2, true, false)] {
            assert!(line.ends_with('\n'));
            let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
            assert!(v.is_object());
        }
        assert_eq!(peer_line(&v6), "{\"peer\":\"[2001:db8::1]:6881\"}\n");
    }

    #[test]
    fn trusts_the_proxy_header_only_from_loopback() {
        let lo: SocketAddr = "127.0.0.1:40000".parse().unwrap();
        let far: SocketAddr = "203.0.113.5:40000".parse().unwrap();
        assert_eq!(
            client_ip(Some(lo), Some("198.51.100.7")),
            ip("198.51.100.7")
        );
        assert_eq!(client_ip(Some(lo), Some("garbage")), ip("127.0.0.1"));
        assert_eq!(
            client_ip(Some(far), Some("198.51.100.7")),
            ip("203.0.113.5")
        );
        assert_eq!(client_ip(None, None), ip("0.0.0.0"));
    }
}
