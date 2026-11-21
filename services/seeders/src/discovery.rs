//! Streamed discovery, `GET /peers?ih=…` (docs/discovery-api.md): every
//! tracker and the DHT are queried in parallel, and each address they return
//! (deduplicated, public, port >= 1024) is scouted (`scout`): probed, scored
//! and written as one NDJSON line. Addresses the scout knows from the last
//! minutes come first, best first; live addresses are written as their
//! probe ends; addresses left unprobed are written as unknown shortly before
//! the deadline (or at once when the info-hash used its probe budget), and
//! dead ones last. The response closes when every source and probe has
//! finished, or at the stream deadline (~30 s). No tokens.
//!
//! The info dict (`info`): live peers offering `ut_metadata` are handed to
//! the metadata fetcher as they are found, and one line
//! `{"info":"<base64>","size":n}` is written as soon as a verified info dict
//! is in (first of all when it is already cached). The stream stays open
//! for a fetch in the air, up to its deadline.
//!
//! A finished lookup is cached for 5 minutes (30 s if empty): a later
//! request for the same info-hash replays its addresses at once, with their
//! current scores.
//!
//! Abuse limits (OQ-14), checked before any lookup starts: requests per
//! minute and concurrent streams per client IP, and concurrent streams in
//! total. Logs carry no info-hash, magnet or IP.

use crate::cache::PeerCache;
use crate::config::Config;
use crate::info::{self, InfoService, Outcome};
use crate::lookup::{Lookup, LookupResult};
use crate::netguard;
use crate::scout::{Ask, Entry, Scout, UNKNOWN_SCORE};
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, unbounded_channel, Sender};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

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
/// dial (they refuse ports below 1024). `allow_private` (local tests and
/// development only, `ALLOW_PRIVATE_PEERS`) lifts the public-IP rule.
pub fn is_dialable(addr: &SocketAddr, allow_private: bool) -> bool {
    addr.port() >= 1024 && (allow_private || netguard::is_public(addr.ip()))
}

/// A probed address: `{"peer":…,"score":…,"alive":true,"seed":…,…}` for a
/// live one, `{"peer":…,"score":…,"alive":false}` for a dead one.
pub fn scouted_line(addr: &SocketAddr, entry: &Entry, now: Instant, half_life: Duration) -> String {
    let score = entry.score(now, half_life);
    let f = &entry.facts;
    if !f.alive {
        return format!("{{\"peer\":\"{addr}\",\"score\":{score:.2},\"alive\":false}}\n");
    }
    let mut line = format!(
        "{{\"peer\":\"{addr}\",\"score\":{score:.2},\"alive\":true,\"seed\":{},\"md\":{},\"unchoked\":{}",
        f.seed, f.md, f.unchoked
    );
    if let Some(have) = f.have {
        line.push_str(&format!(",\"have\":{have:.2}"));
    }
    if let Some(rtt) = f.rtt_ms {
        line.push_str(&format!(",\"rtt\":{rtt}"));
    }
    line.push_str("}\n");
    line
}

/// An address nobody probed: `{"peer":…,"score":0.2}`.
pub fn unknown_line(addr: &SocketAddr) -> String {
    format!("{{\"peer\":\"{addr}\",\"score\":{UNKNOWN_SCORE:.2}}}\n")
}

/// The last line: how many addresses were sent (and how many of them
/// alive, dead or unknown), whether the lookup came from the cache, and
/// whether every source finished (false: cut at the deadline).
pub fn end_line(counts: &Counts, cached: bool, complete: bool) -> String {
    format!(
        "{{\"done\":true,\"count\":{},\"cached\":{cached},\"complete\":{complete},\"alive\":{},\"dead\":{},\"unknown\":{}}}\n",
        counts.alive + counts.dead + counts.unknown,
        counts.alive,
        counts.dead,
        counts.unknown
    )
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub alive: usize,
    pub dead: usize,
    pub unknown: usize,
}

/// Aborts a task when dropped (the response went away).
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Where a stream's addresses come from.
pub enum Source {
    /// A new lookup (trackers + DHT); its result is cached when it finishes.
    Lookup {
        lookup: Arc<Lookup>,
        cache: Arc<PeerCache>,
        trackers: Vec<String>,
    },
    /// A lookup finished within the cache's TTL.
    Cached(Arc<LookupResult>),
}

/// Unprobed addresses are written as unknown this long before the deadline.
const FLUSH_BEFORE: Duration = Duration::from_secs(1);
/// A finished lookup gets this long to land in the cache once the stream ends.
const CACHE_GRACE: Duration = Duration::from_secs(1);

/// Start a stream and return its NDJSON body as a stream of chunks. The
/// work runs in a task that ends (with its lookup and its waits) when the
/// body is dropped; probes already started finish and are remembered.
pub fn stream(
    scout: Arc<Scout>,
    info: Arc<InfoService>,
    cfg: Arc<Config>,
    info_hash: [u8; 20],
    source: Source,
    permit: Permit,
) -> impl futures_lite::Stream<Item = Result<Bytes, Infallible>> + Send + 'static {
    let (tx, rx) = channel::<Bytes>(64);
    let task = tokio::spawn(async move {
        let _permit = permit;
        run(scout, info, cfg, info_hash, source, tx).await;
    });
    futures_lite::stream::unfold((rx, AbortOnDrop(task)), |(mut rx, guard)| async move {
        let chunk = rx.recv().await?;
        Some((Ok(chunk), (rx, guard)))
    })
}

/// One stream's state: what was written, what waits for a probe.
struct Run {
    tx: Sender<Bytes>,
    half_life: Duration,
    seen: HashSet<SocketAddr>,
    queue: VecDeque<SocketAddr>,
    probing: HashSet<SocketAddr>,
    dead: Vec<(SocketAddr, Entry)>,
    counts: Counts,
    out: String,
    /// Live peers offering ut_metadata, in the order found.
    md_peers: Vec<SocketAddr>,
    /// New ones since the last offer to the metadata fetcher.
    md_new: bool,
}

impl Run {
    fn found(&mut self, entry: Entry, addr: SocketAddr) {
        if entry.facts.alive && entry.facts.md {
            self.md_peers.push(addr);
            self.md_new = true;
        }
        if entry.facts.alive {
            self.counts.alive += 1;
            self.out
                .push_str(&scouted_line(&addr, &entry, Instant::now(), self.half_life));
        } else {
            self.dead.push((addr, entry));
        }
    }

    fn unknown(&mut self, addr: SocketAddr) {
        self.counts.unknown += 1;
        self.out.push_str(&unknown_line(&addr));
    }

    /// Send what was written so far. False: the client went away.
    async fn flush(&mut self) -> bool {
        if self.out.is_empty() {
            return true;
        }
        let chunk = Bytes::from(std::mem::take(&mut self.out));
        self.tx.send(chunk).await.is_ok()
    }
}

/// The metadata side of a stream: written yet, or the fetch to wait for.
struct InfoState {
    written: bool,
    waiting: Option<watch::Receiver<Option<Outcome>>>,
}

impl InfoState {
    /// Hand new md peers to the fetcher and follow its flight (or write
    /// the info dict, if a flight just put it in the cache).
    async fn offer(&mut self, service: &Arc<InfoService>, info_hash: [u8; 20], st: &mut Run) {
        if self.written || !st.md_new {
            return;
        }
        st.md_new = false;
        service.offer(info_hash, &st.md_peers);
        if self.waiting.is_none() {
            self.waiting = service.watch(&info_hash);
        }
        if self.waiting.is_none() {
            if let Some(dict) = service.cached(&info_hash).await {
                st.out.push_str(&info::info_line(&dict));
                self.written = true;
            }
        }
    }

    fn pending(&self) -> bool {
        !self.written && self.waiting.is_some()
    }
}

/// The flight's outcome (None: the flight went away without one).
async fn outcome_of(waiting: &mut Option<watch::Receiver<Option<Outcome>>>) -> Option<Outcome> {
    let rx = waiting.as_mut()?;
    match rx.wait_for(Option::is_some).await {
        Ok(v) => v.clone(),
        Err(_) => None,
    }
}

async fn run(
    scout: Arc<Scout>,
    service: Arc<InfoService>,
    cfg: Arc<Config>,
    info_hash: [u8; 20],
    source: Source,
    tx: Sender<Bytes>,
) {
    let started = tokio::time::Instant::now();
    let deadline = started + cfg.peers_stream_deadline;
    let flush_at = deadline
        .checked_sub(FLUSH_BEFORE)
        .filter(|t| *t > started)
        .unwrap_or(started + cfg.peers_stream_deadline / 2);
    let allow_private = cfg.allow_private_peers;
    let per_stream = cfg.scout_per_stream.max(1);

    let (found_tx, mut found_rx) = unbounded_channel::<Vec<SocketAddr>>();
    let cached = matches!(source, Source::Cached(_));
    let mut lookup_task = match source {
        Source::Lookup {
            lookup,
            cache,
            trackers,
        } => {
            let sources_deadline = cfg.peers_stream_deadline.saturating_sub(SOURCE_SLACK);
            let cfg = cfg.clone();
            Some(AbortOnDrop(tokio::spawn(async move {
                let result = lookup
                    .run_streaming_for(info_hash, trackers, &cfg, sources_deadline, Some(found_tx))
                    .await;
                cache.insert(info_hash, Arc::new(result)).await;
            })))
        }
        Source::Cached(result) => {
            let _ = found_tx.send(result.peers.clone());
            // The only batch: closing the channel ends the sources.
            drop(found_tx);
            None
        }
    };

    let mut st = Run {
        tx,
        half_life: scout.settings().half_life,
        seen: HashSet::new(),
        queue: VecDeque::new(),
        probing: HashSet::new(),
        dead: Vec::new(),
        counts: Counts::default(),
        out: String::new(),
        md_peers: Vec::new(),
        md_new: false,
    };
    let mut meta = InfoState {
        written: false,
        waiting: None,
    };
    // A cached info dict goes first of all.
    if let Some(dict) = service.cached(&info_hash).await {
        st.out.push_str(&info::info_line(&dict));
        meta.written = true;
    }

    // What the scout already knows comes first, best first.
    for (addr, entry) in scout.fresh(info_hash).await {
        if is_dialable(&addr, allow_private) && st.seen.insert(addr) {
            st.found(entry, addr);
        }
    }
    meta.offer(&service, info_hash, &mut st).await;
    if !st.flush().await {
        return;
    }

    let mut waits: JoinSet<(SocketAddr, Option<Entry>)> = JoinSet::new();
    let mut sources_done = false;
    let mut flushing = false;
    let mut complete = true;
    loop {
        // Start probes (or reuse fresh results) up to the per-stream limit.
        while !flushing && waits.len() < per_stream {
            let Some(addr) = st.queue.pop_front() else {
                break;
            };
            match scout.ask(info_hash, addr).await {
                Ask::Fresh(entry) => st.found(entry, addr),
                Ask::OverBudget => st.unknown(addr),
                Ask::Wait(mut rx) => {
                    st.probing.insert(addr);
                    waits.spawn(async move {
                        let entry = rx.wait_for(Option::is_some).await.ok().and_then(|e| *e);
                        (addr, entry)
                    });
                }
            }
        }
        meta.offer(&service, info_hash, &mut st).await;
        if !st.flush().await {
            return;
        }
        if sources_done && st.queue.is_empty() && waits.is_empty() && !meta.pending() {
            break;
        }
        tokio::select! {
            batch = found_rx.recv(), if !sources_done => match batch {
                Some(batch) => {
                    for addr in batch {
                        if !is_dialable(&addr, allow_private) || !st.seen.insert(addr) {
                            continue;
                        }
                        if flushing {
                            st.unknown(addr);
                        } else {
                            st.queue.push_back(addr);
                        }
                    }
                }
                None => sources_done = true,
            },
            Some(joined) = waits.join_next(), if !waits.is_empty() => {
                if let Ok((addr, entry)) = joined {
                    st.probing.remove(&addr);
                    match entry {
                        Some(entry) => st.found(entry, addr),
                        // The probe's task died: nothing learned.
                        None => st.unknown(addr),
                    }
                }
            },
            outcome = outcome_of(&mut meta.waiting), if meta.pending() => {
                match outcome {
                    Some(Ok(dict)) => {
                        st.out.push_str(&info::info_line(&dict));
                        meta.written = true;
                    }
                    // That flight failed (or is gone): a peer found later
                    // may start another.
                    _ => meta.waiting = None,
                }
            },
            _ = tokio::time::sleep_until(flush_at), if !flushing => {
                // Too late to probe: what is left goes out unprobed. Probes
                // already running finish and are remembered for next time.
                flushing = true;
                waits.abort_all();
                waits = JoinSet::new();
                let left: Vec<SocketAddr> = st.queue.drain(..).chain(st.probing.drain()).collect();
                for addr in left {
                    st.unknown(addr);
                }
            },
            _ = tokio::time::sleep_until(deadline) => {
                complete = sources_done;
                break;
            },
        }
    }

    let mut dead = std::mem::take(&mut st.dead);
    dead.sort_by_key(|(addr, _)| *addr);
    let now = Instant::now();
    for (addr, entry) in &dead {
        st.counts.dead += 1;
        st.out
            .push_str(&scouted_line(addr, entry, now, st.half_life));
    }
    st.out.push_str(&end_line(&st.counts, cached, complete));
    let counts = st.counts;
    if !st.flush().await {
        return;
    }
    tracing::debug!(
        info = meta.written,
        alive = counts.alive,
        dead = counts.dead,
        unknown = counts.unknown,
        cached,
        complete,
        "peers stream done"
    );
    // A lookup whose sources all finished is about to land in the cache.
    if let Some(task) = lookup_task.as_mut() {
        if sources_done {
            let _ = tokio::time::timeout(CACHE_GRACE, &mut task.0).await;
        }
    }
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
            assert!(is_dialable(&ok.parse().unwrap(), false), "{ok}");
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
            assert!(!is_dialable(&bad.parse().unwrap(), false), "{bad}");
        }
        // Local tests and development may reach private peers, never low ports.
        assert!(is_dialable(&"127.0.0.1:6881".parse().unwrap(), true));
        assert!(!is_dialable(&"127.0.0.1:80".parse().unwrap(), true));
    }

    #[test]
    fn lines_are_ndjson() {
        use crate::scout::Facts;
        let v4: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        let now = Instant::now();
        let hl = Duration::from_secs(1800);
        let alive = Entry {
            facts: Facts {
                alive: true,
                seed: false,
                have: Some(0.42),
                md: true,
                unchoked: false,
                rtt_ms: Some(45),
            },
            probed_at: now,
        };
        let bare = Entry {
            facts: Facts {
                have: None,
                rtt_ms: None,
                ..alive.facts
            },
            probed_at: now,
        };
        let dead = Entry {
            facts: Facts::dead(),
            probed_at: now,
        };
        let counts = Counts {
            alive: 1,
            dead: 1,
            unknown: 1,
        };
        for line in [
            scouted_line(&v4, &alive, now, hl),
            scouted_line(&v6, &bare, now, hl),
            scouted_line(&v6, &dead, now, hl),
            unknown_line(&v6),
            end_line(&counts, true, false),
        ] {
            assert!(line.ends_with('\n'));
            assert!(line.len() < 256, "{line}");
            let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
            assert!(v.is_object());
        }
        let v: serde_json::Value =
            serde_json::from_str(scouted_line(&v4, &alive, now, hl).trim_end()).unwrap();
        assert_eq!(v["peer"], "1.2.3.4:6881");
        assert_eq!(v["alive"], true);
        assert_eq!(v["seed"], false);
        assert_eq!(v["md"], true);
        assert_eq!(v["unchoked"], false);
        assert_eq!(v["have"], 0.42);
        assert_eq!(v["rtt"], 45);
        assert!(v["score"].as_f64().unwrap() > 0.5);
        assert_eq!(
            unknown_line(&v6),
            "{\"peer\":\"[2001:db8::1]:6881\",\"score\":0.20}\n"
        );
        assert_eq!(
            scouted_line(&v6, &dead, now, hl),
            "{\"peer\":\"[2001:db8::1]:6881\",\"score\":0.02,\"alive\":false}\n"
        );
        let end: serde_json::Value =
            serde_json::from_str(end_line(&counts, true, false).trim_end()).unwrap();
        assert_eq!(end["count"], 3);
        assert_eq!(end["unknown"], 1);
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
