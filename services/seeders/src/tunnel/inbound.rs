//! Announce and inbound connections (M4-02; docs/architecture.md › Chunk
//! recovery › Swarms with few seeders, step 3; docs/tunnel-protocol.md ›
//! Announce and inbound connections).
//!
//! On a session's `announce` the gateway joins the swarm of that info-hash:
//! it announces its own port to trackers and the DHT and accepts inbound
//! TCP and uTP connections on that port (M4-04e: one UDP socket with the
//! TCP listener's port number, `crate::utp::socket`). A remote must speak
//! first with a BitTorrent handshake for an info-hash some WebSocket
//! announces; anything else is closed without a byte in reply (a uTP one is
//! forgotten without even an `ST_FIN`). A valid one is answered with the
//! announcing session's reserved bytes and peer id and handed to that
//! WebSocket, which sends `accept` and relays it like an outbound
//! connection. The same UDP socket carries the outbound uTP connections of
//! the sessions that announce (`open` with transport uTP, `session.rs`).
//!
//! Only while asked: nothing is announced and no port listens until a
//! session announces; the TCP listener and the uTP gate close and the
//! trackers get `stopped` when the last WebSocket announcing an info-hash
//! withdraws or closes. The UDP socket itself stays bound, answering no
//! SYN, only while uTP connections made through it last (as accepted TCP
//! connections outlive their listener); a later announce reuses it.
//! Bounded: info-hashes per WebSocket, per client IP and in total, inbound
//! connections per session and in total, handshakes in flight, handshake
//! timeout. Every announce round a client causes (a new swarm, or trackers
//! added to one; each sends `started` and later `stopped` to up to 32
//! trackers, and a DHT announce) is charged to its IP and to the gateway,
//! per minute: a client that cycles `announce` and `withdraw` cannot turn
//! the gateway into a request generator. The session's trackers must be on
//! port 80, 443 or >= 1024. Logs carry counters and reason codes only (no
//! peer address, no info-hash, no client IP).

use super::codec::announce_status;
use super::gate;
use super::session::Group;
use super::stream::PeerStream;
use crate::dht::DhtClient;
use crate::netguard;
use crate::peerwire::HANDSHAKE_LEN;
use crate::tracker::{self, AnnounceEvent, AnnounceParams};
use crate::utp::socket::{SocketConfig, UtpAcceptor, UtpSocket, WeakUtpSocket};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinSet};

/// One tracker announce, and the DHT's, give up after this long.
const TRACKER_TIMEOUT: Duration = Duration::from_secs(15);
const DHT_TIMEOUT: Duration = Duration::from_secs(30);
/// The `stopped` announces when a swarm is left.
const STOPPED_TIMEOUT: Duration = Duration::from_secs(5);
/// `left` we announce: anything above 0 says "leecher", so trackers give our
/// address to seeders (a seed is not handed to other seeds).
const LEFT: u64 = 1;
/// The window of the announce-round rates.
const RATE_WINDOW: Duration = Duration::from_secs(60);
/// Client IPs whose round counts are remembered at once.
const MAX_TRACKED_IPS: usize = 4096;

/// Where announces go: the session's trackers, the configured defaults and
/// the DHT.
pub struct Announcer {
    pub http: reqwest::Client,
    pub dht: Option<Arc<DhtClient>>,
    pub default_trackers: Vec<String>,
    pub max_trackers: usize,
}

impl Announcer {
    /// Only the sessions' own trackers: no DHT, no defaults (tests).
    #[cfg(test)]
    pub fn trackers_only() -> Self {
        let http = reqwest::Client::builder()
            .dns_resolver(Arc::new(crate::resolver::GuardedResolver))
            .timeout(TRACKER_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            http,
            dht: None,
            default_trackers: Vec::new(),
            max_trackers: 20,
        }
    }

    /// One announce to every tracker (the session's first, then the
    /// defaults, deduplicated and capped) and, if `dht`, to the DHT, in
    /// parallel. Returns (succeeded, failed).
    async fn round(
        &self,
        params: &AnnounceParams,
        trackers: &[(String, bool)],
        dht: bool,
    ) -> (usize, usize) {
        let mut seen = std::collections::HashSet::new();
        let mut set = JoinSet::new();
        let defaults = self.default_trackers.iter().map(|t| (t.clone(), false));
        for (url, private) in trackers.iter().cloned().chain(defaults) {
            if seen.len() >= self.max_trackers || !seen.insert(url.clone()) {
                continue;
            }
            let http = self.http.clone();
            let mut params = params.clone();
            params.allow_private = private;
            set.spawn(async move {
                let call = tracker::announce(&http, &url, &params, TRACKER_TIMEOUT);
                matches!(tokio::time::timeout(TRACKER_TIMEOUT, call).await, Ok(Ok(_)))
            });
        }
        if dht {
            if let Some(dht) = self.dht.clone() {
                let (info_hash, port) = (params.info_hash, params.port);
                set.spawn(async move { dht.announce(info_hash, port, DHT_TIMEOUT).await });
            }
        }
        let (mut ok, mut failed) = (0, 0);
        while let Some(done) = set.join_next().await {
            if done.unwrap_or(false) {
                ok += 1;
            } else {
                failed += 1;
            }
        }
        (ok, failed)
    }
}

/// `TUNNEL_INBOUND_*`, `TUNNEL_ANNOUNCE*` (config.rs).
#[derive(Debug, Clone)]
pub struct InboundSettings {
    pub listen: SocketAddr,
    /// 0: the listener's own port.
    pub announce_port: u16,
    /// The uTP socket on the listener's port: limits and timers.
    pub utp: SocketConfig,
    pub interval: Duration,
    pub dht: bool,
    pub per_session: usize,
    pub max_inbound: usize,
    pub max_pending: usize,
    pub per_socket: usize,
    pub max_announces: usize,
    /// Info-hashes announced at once by the WebSockets of one client IP.
    pub per_ip: usize,
    /// Announce rounds one client IP may start per minute.
    pub rounds_per_ip: u32,
    /// Announce rounds started per minute, all clients together.
    pub rounds_max: u32,
    pub handshake_timeout: Duration,
}

/// The key of the per-IP limits: the address, an IPv6 one cut to its /64
/// (one subscriber's prefix).
fn ip_key(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => {
            let s = v6.segments();
            IpAddr::V6(Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0))
        }
        v4 => v4,
    }
}

/// Announce rounds started in the current minute, per client IP and in
/// total (fixed windows; a refused round is not counted).
#[derive(Default)]
struct Rates {
    all: Option<(Instant, u32)>,
    ips: HashMap<IpAddr, (Instant, u32)>,
}

impl Rates {
    /// Take one round for `ip`, if both windows have room.
    fn take(&mut self, ip: IpAddr, now: Instant, per_ip: u32, total: u32) -> bool {
        let fresh = |w: Option<(Instant, u32)>| match w {
            Some((start, n)) if now.duration_since(start) < RATE_WINDOW => (start, n),
            _ => (now, 0),
        };
        if self.ips.len() >= MAX_TRACKED_IPS && !self.ips.contains_key(&ip) {
            self.ips
                .retain(|_, (start, _)| now.duration_since(*start) < RATE_WINDOW);
            if self.ips.len() >= MAX_TRACKED_IPS {
                return false;
            }
        }
        let all = fresh(self.all);
        let mine = fresh(self.ips.get(&ip).copied());
        if all.1 >= total || mine.1 >= per_ip {
            return false;
        }
        self.all = Some((all.0, all.1 + 1));
        self.ips.insert(ip, (mine.0, mine.1 + 1));
        true
    }
}

/// An inbound peer that handshook for an info-hash its WebSocket announces.
pub struct InboundPeer {
    pub stream: PeerStream,
    pub addr: SocketAddr,
    pub info_hash: [u8; 20],
    /// The remote's reserved bytes and peer id.
    pub reserved: [u8; 8],
    pub peer_id: [u8; 20],
    /// Peer-wire bytes that came with its handshake.
    pub extra: Bytes,
    /// Our answer: the announcing session's reserved bytes and peer id.
    pub ours: ([u8; 8], [u8; 20]),
    /// Held for the connection's lifetime (per-session and total counts).
    pub slot: InboundSlot,
}

/// One inbound connection's place in its session's and the gateway's counts.
pub struct InboundSlot {
    group: Arc<Group>,
    _total: OwnedSemaphorePermit,
}

impl Drop for InboundSlot {
    fn drop(&mut self) {
        self.group.inbound.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Where an inbound peer goes, the handshake we answer it with, its slot.
type Pick = (
    mpsc::UnboundedSender<InboundPeer>,
    ([u8; 8], [u8; 20]),
    InboundSlot,
);

/// What a WebSocket registers when it announces.
pub struct Member {
    pub socket: u64,
    pub reserved: [u8; 8],
    pub peer_id: [u8; 20],
    pub group: Arc<Group>,
    pub tx: mpsc::UnboundedSender<InboundPeer>,
    /// The WebSocket's client IP (per-IP limits only; never logged).
    pub client: IpAddr,
    /// Loopback/private trackers and inbound addresses (local tests only).
    pub allow_private: bool,
}

struct Swarm {
    members: Vec<Member>,
    /// The sessions' trackers, each with whether it may be private.
    trackers: Arc<Mutex<Vec<(String, bool)>>>,
    params: AnnounceParams,
    task: AbortHandle,
    /// Round-robin among the members.
    next: usize,
}

/// The open inbound port: TCP listener and uTP gate on the same number.
struct Listener {
    /// The TCP accept loop (aborting it closes the TCP port).
    tcp: AbortHandle,
    /// The UDP socket: inbound uTP, and the sessions' outbound uTP.
    utp: UtpSocket,
    /// The uTP gate; closed with the listener.
    gate: Arc<UtpAcceptor>,
    /// The uTP accept loop.
    utp_task: AbortHandle,
    port: u16,
}

impl Drop for Listener {
    /// Closes both ports. Lock order: the `Inbound` state, then the uTP
    /// socket's (its address filter reads an atomic, never the state); a
    /// closed listener is still dropped after the state lock is released.
    fn drop(&mut self) {
        self.tcp.abort();
        self.gate.close();
        self.utp_task.abort();
    }
}

#[derive(Default)]
struct State {
    swarms: HashMap<[u8; 20], Swarm>,
    listener: Option<Listener>,
    /// The UDP socket of the last listener, alive while connections made
    /// through it last; reused by the next one.
    lingering: Option<WeakUtpSocket>,
    /// Announce rounds started this minute.
    rates: Rates,
}

/// Counters for the logs.
#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    rejected: AtomicU64,
    /// Announces (or tracker additions) refused by the round rates.
    rate_limited: AtomicU64,
}

pub struct Inbound {
    settings: InboundSettings,
    announcer: Arc<Announcer>,
    state: Mutex<State>,
    total: Arc<Semaphore>,
    pending: Arc<Semaphore>,
    next_socket: AtomicU64,
    counters: Counters,
    /// Some announcing WebSocket may see private addresses (local tests):
    /// the uTP gate's filter reads it without a lock.
    any_private: Arc<AtomicBool>,
    me: Weak<Inbound>,
}

impl Inbound {
    pub fn new(settings: InboundSettings, announcer: Arc<Announcer>) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            total: Arc::new(Semaphore::new(settings.max_inbound)),
            pending: Arc::new(Semaphore::new(settings.max_pending)),
            settings,
            announcer,
            state: Mutex::new(State::default()),
            next_socket: AtomicU64::new(1),
            counters: Counters::default(),
            any_private: Arc::new(AtomicBool::new(false)),
            me: me.clone(),
        })
    }

    /// An id for a WebSocket's memberships.
    pub fn socket_id(&self) -> u64 {
        self.next_socket.fetch_add(1, Ordering::Relaxed)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The port the listener is bound to (tests).
    #[cfg(test)]
    pub fn listening_port(&self) -> Option<u16> {
        self.lock().listener.as_ref().map(|l| l.port)
    }

    /// The uTP socket for an outbound uTP `open` of `group`'s session: only
    /// while that session announces `info_hash` (the listener is open then).
    pub fn utp_for(&self, group: &Arc<Group>, info_hash: &[u8; 20]) -> Option<UtpSocket> {
        let st = self.lock();
        let swarm = st.swarms.get(info_hash)?;
        if !swarm.members.iter().any(|m| Arc::ptr_eq(&m.group, group)) {
            return None;
        }
        st.listener.as_ref().map(|l| l.utp.clone())
    }

    fn refresh_private(&self, st: &State) {
        let any = st
            .swarms
            .values()
            .any(|s| s.members.iter().any(|m| m.allow_private));
        self.any_private.store(any, Ordering::Relaxed);
    }

    /// Join (or update) the swarm of `info_hash` for `member`'s WebSocket:
    /// the `announced` status.
    pub fn announce(&self, member: Member, info_hash: [u8; 20], trackers: Vec<String>) -> u8 {
        let private = member.allow_private;
        let trackers: Vec<(String, bool)> = trackers
            .into_iter()
            .filter(|t| tracker_url_ok(t))
            .map(|t| (t, private))
            .collect();
        let now = Instant::now();
        let client = ip_key(member.client);
        let (per_ip_rounds, max_rounds) = (self.settings.rounds_per_ip, self.settings.rounds_max);
        let mut guard = self.lock();
        let st = &mut *guard;
        let socket = member.socket;
        let mine = st
            .swarms
            .values()
            .filter(|s| s.members.iter().any(|m| m.socket == socket))
            .count();
        // Swarms this client IP is in, other than `info_hash`'s.
        let from_ip = st
            .swarms
            .iter()
            .filter(|(ih, s)| {
                **ih != info_hash && s.members.iter().any(|m| ip_key(m.client) == client)
            })
            .count();
        if let Some(swarm) = st.swarms.get_mut(&info_hash) {
            let in_swarm = swarm.members.iter().any(|m| ip_key(m.client) == client);
            match swarm.members.iter_mut().find(|m| m.socket == socket) {
                Some(m) => *m = member,
                None if mine >= self.settings.per_socket => return announce_status::LIMIT_REACHED,
                None if !in_swarm && from_ip >= self.settings.per_ip => {
                    return announce_status::LIMIT_REACHED
                }
                None => swarm.members.push(member),
            }
            let fresh = unknown(&swarm.trackers, trackers);
            if !fresh.is_empty() {
                if st.rates.take(client, now, per_ip_rounds, max_rounds) {
                    // New trackers need not wait for the next round.
                    let fresh = merge(&swarm.trackers, fresh);
                    let (announcer, mut params) = (self.announcer.clone(), swarm.params.clone());
                    params.event = AnnounceEvent::Started;
                    tokio::spawn(async move { announcer.round(&params, &fresh, false).await });
                } else {
                    // Still announced, through the trackers it has.
                    self.counters.rate_limited.fetch_add(1, Ordering::Relaxed);
                    tracing::info!(event = "tunnel.announce_rate_limited", what = "trackers");
                }
            }
            self.refresh_private(st);
            return announce_status::ACTIVE;
        }
        if mine >= self.settings.per_socket
            || from_ip >= self.settings.per_ip
            || st.swarms.len() >= self.settings.max_announces
        {
            return announce_status::LIMIT_REACHED;
        }
        if !st.rates.take(client, now, per_ip_rounds, max_rounds) {
            self.counters.rate_limited.fetch_add(1, Ordering::Relaxed);
            tracing::info!(event = "tunnel.announce_rate_limited", what = "swarm");
            return announce_status::LIMIT_REACHED;
        }
        let port = match &st.listener {
            Some(l) => l.port,
            None => {
                let lingering = st.lingering.as_ref().and_then(WeakUtpSocket::upgrade);
                match self.bind(lingering) {
                    Ok(l) => {
                        let port = l.port;
                        st.listener = Some(l);
                        st.lingering = None;
                        port
                    }
                    Err(e) => {
                        tracing::warn!(event = "tunnel.inbound_listen_failed", kind = ?e.kind());
                        return announce_status::INTERNAL_ERROR;
                    }
                }
            }
        };
        let mut params = AnnounceParams::new(info_hash);
        params.port = match self.settings.announce_port {
            0 => port,
            p => p,
        };
        params.left = LEFT;
        params.numwant = 0;
        let initial = trackers;
        let trackers = Arc::new(Mutex::new(Vec::new()));
        merge(&trackers, initial);
        let task = tokio::spawn(announce_loop(
            self.announcer.clone(),
            params.clone(),
            trackers.clone(),
            self.settings.interval,
            self.settings.dht,
        ));
        st.swarms.insert(
            info_hash,
            Swarm {
                members: vec![member],
                trackers,
                params,
                task: task.abort_handle(),
                next: 0,
            },
        );
        self.refresh_private(st);
        tracing::info!(event = "tunnel.announce_start", swarms = st.swarms.len());
        announce_status::ACTIVE
    }

    /// Leave the swarm of `info_hash` for one WebSocket.
    pub fn withdraw(&self, socket: u64, info_hash: &[u8; 20]) {
        let mut st = self.lock();
        let empty = match st.swarms.get_mut(info_hash) {
            Some(swarm) => {
                swarm.members.retain(|m| m.socket != socket);
                swarm.members.is_empty()
            }
            None => false,
        };
        if empty {
            if let Some(swarm) = st.swarms.remove(info_hash) {
                self.leave(swarm);
            }
        }
        self.refresh_private(&st);
        let closed = self.take_idle_listener(&mut st);
        drop(st);
        drop(closed);
    }

    /// A WebSocket is gone: leave every swarm it announced.
    pub fn withdraw_all(&self, socket: u64) {
        let mut st = self.lock();
        let mut gone = Vec::new();
        for (ih, swarm) in st.swarms.iter_mut() {
            swarm.members.retain(|m| m.socket != socket);
            if swarm.members.is_empty() {
                gone.push(*ih);
            }
        }
        for ih in gone {
            if let Some(swarm) = st.swarms.remove(&ih) {
                self.leave(swarm);
            }
        }
        self.refresh_private(&st);
        let closed = self.take_idle_listener(&mut st);
        drop(st);
        drop(closed);
    }

    /// Stop the announce loop and tell the trackers we left (best effort).
    fn leave(&self, swarm: Swarm) {
        swarm.task.abort();
        let announcer = self.announcer.clone();
        let mut params = swarm.params;
        params.event = AnnounceEvent::Stopped;
        let trackers = trackers_vec(swarm.trackers);
        tokio::spawn(async move {
            let _ =
                tokio::time::timeout(STOPPED_TIMEOUT, announcer.round(&params, &trackers, false))
                    .await;
        });
        tracing::info!(event = "tunnel.announce_stop");
    }

    /// With no swarm left, the listener to close: the caller drops it after
    /// releasing the state lock (see `Listener`'s `Drop`).
    fn take_idle_listener(&self, st: &mut State) -> Option<Listener> {
        if !st.swarms.is_empty() {
            return None;
        }
        let listener = st.listener.take()?;
        st.lingering = Some(listener.utp.downgrade());
        let utp = listener.utp.stats();
        tracing::info!(
            event = "tunnel.inbound_listener_closed",
            accepted = self.counters.accepted.load(Ordering::Relaxed),
            rejected = self.counters.rejected.load(Ordering::Relaxed),
            utp_syn_accepted = utp.syn_accepted,
            utp_syn_ignored =
                utp.dropped_syn_no_gate + utp.dropped_syn_limit + utp.dropped_syn_collision,
            utp_dropped = utp.dropped_malformed + utp.dropped_unknown,
        );
        Some(listener)
    }

    fn is_announced(&self, info_hash: &[u8; 20]) -> bool {
        self.lock().swarms.contains_key(info_hash)
    }

    /// Bind the TCP listener and the UDP socket on the same port and start
    /// accepting (std's TCP bind sets SO_REUSEADDR, so the port can be bound
    /// again right after a close). A UDP socket still alive from the last
    /// listener is reused; with port 0 a few ports are tried until both
    /// transports get the same one.
    fn bind(&self, lingering: Option<UtpSocket>) -> std::io::Result<Listener> {
        let ip = self.settings.listen.ip();
        if let Some(utp) = lingering {
            let port = utp.local_addr().port();
            let configured = self.settings.listen.port();
            if configured == 0 || configured == port {
                if let Ok(tcp) = std::net::TcpListener::bind(SocketAddr::new(ip, port)) {
                    return self.start(tcp, utp);
                }
            }
        }
        let attempts = if self.settings.listen.port() == 0 {
            16
        } else {
            1
        };
        let mut failure = std::io::Error::from(std::io::ErrorKind::AddrInUse);
        for _ in 0..attempts {
            let tcp = std::net::TcpListener::bind(self.settings.listen)?;
            let port = tcp.local_addr()?.port();
            match std::net::UdpSocket::bind(SocketAddr::new(ip, port)) {
                Ok(udp) => {
                    udp.set_nonblocking(true)?;
                    let udp = tokio::net::UdpSocket::from_std(udp)?;
                    let utp = UtpSocket::from_socket(udp, self.settings.utp.clone())?;
                    return self.start(tcp, utp);
                }
                Err(e) => failure = e,
            }
        }
        Err(failure)
    }

    fn start(&self, tcp: std::net::TcpListener, utp: UtpSocket) -> std::io::Result<Listener> {
        tcp.set_nonblocking(true)?;
        let listener = TcpListener::from_std(tcp)?;
        let port = listener.local_addr()?.port();
        // SYNs from non-public addresses only while a session may see them.
        let private = self.any_private.clone();
        let filter = Arc::new(move |addr: SocketAddr| {
            private.load(Ordering::Relaxed) || netguard::is_public(addr.ip().to_canonical())
        });
        let gate = Arc::new(utp.listen(Some(filter))?);
        let tcp = tokio::spawn(accept_loop(listener, self.me.clone()));
        let utp_task = tokio::spawn(utp_accept_loop(gate.clone(), self.me.clone()));
        tracing::info!(event = "tunnel.inbound_listener_open");
        Ok(Listener {
            tcp: tcp.abort_handle(),
            utp,
            gate,
            utp_task: utp_task.abort_handle(),
            port,
        })
    }

    fn reject(&self, why: &'static str) {
        self.counters.rejected.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(event = "tunnel.inbound_rejected", why);
    }

    /// Choose the WebSocket that gets an inbound peer for `info_hash`: round
    /// robin among those whose session is under its inbound limit (and that
    /// may see a private address, if it is one), within the total limit.
    fn pick(&self, info_hash: &[u8; 20], ip: IpAddr) -> Result<Pick, &'static str> {
        let mut st = self.lock();
        let Some(swarm) = st.swarms.get_mut(info_hash) else {
            return Err("no_session");
        };
        let n = swarm.members.len();
        let mut why = "session_full";
        for i in 0..n {
            let at = (swarm.next + i) % n;
            let m = &swarm.members[at];
            if m.tx.is_closed() {
                continue;
            }
            if !m.allow_private && !netguard::is_public(ip) {
                why = "forbidden_address";
                continue;
            }
            let room = m
                .group
                .inbound
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    (n < self.settings.per_session).then_some(n + 1)
                });
            if room.is_err() {
                continue;
            }
            let Ok(total) = self.total.clone().try_acquire_owned() else {
                m.group.inbound.fetch_sub(1, Ordering::AcqRel);
                return Err("busy");
            };
            swarm.next = at + 1;
            let slot = InboundSlot {
                group: m.group.clone(),
                _total: total,
            };
            return Ok((m.tx.clone(), (m.reserved, m.peer_id), slot));
        }
        Err(why)
    }

    /// One inbound connection: wait for its handshake (bounded), check it,
    /// and hand it to a WebSocket; anything else is dropped without a reply.
    async fn admit(
        self: Arc<Self>,
        mut stream: PeerStream,
        addr: SocketAddr,
        permit: OwnedSemaphorePermit,
    ) {
        if let PeerStream::Tcp(s) = &stream {
            let _ = s.set_nodelay(true);
        }
        let me = self.clone();
        let read = async {
            let mut buf = BytesMut::with_capacity(HANDSHAKE_LEN + 4096);
            loop {
                match stream.read_buf(&mut buf).await {
                    Ok(0) | Err(_) => return Err("remote_closed"),
                    Ok(_) => match gate::check_inbound(&buf, |ih| me.is_announced(ih)) {
                        gate::Inbound::NeedMore => {
                            if buf.capacity() == buf.len() {
                                buf.reserve(4096);
                            }
                        }
                        gate::Inbound::NotBitTorrent => return Err("not_bittorrent"),
                        gate::Inbound::NoSession => return Err("no_session"),
                        gate::Inbound::Accepted {
                            info_hash,
                            reserved,
                            peer_id,
                        } => {
                            let extra = buf.split_off(HANDSHAKE_LEN).freeze();
                            return Ok((info_hash, reserved, peer_id, extra));
                        }
                    },
                }
            }
        };
        let outcome = tokio::time::timeout(self.settings.handshake_timeout, read).await;
        drop(permit);
        let (info_hash, reserved, peer_id, extra) = match outcome {
            Err(_) => return self.refuse(stream, "timeout"),
            Ok(Err(why)) => return self.refuse(stream, why),
            Ok(Ok(v)) => v,
        };
        let ip = addr.ip().to_canonical();
        let (tx, ours, slot) = match self.pick(&info_hash, ip) {
            Ok(v) => v,
            Err(why) => return self.refuse(stream, why),
        };
        let peer = InboundPeer {
            stream,
            addr: SocketAddr::new(ip, addr.port()),
            info_hash,
            reserved,
            peer_id,
            extra,
            ours,
            slot,
        };
        if let Err(unsent) = tx.send(peer) {
            return self.refuse(unsent.0.stream, "session_gone");
        }
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);
    }

    /// Drop a remote without a byte in reply.
    fn refuse(&self, stream: PeerStream, why: &'static str) {
        stream.discard();
        self.reject(why);
    }
}

async fn accept_loop(listener: TcpListener, inbound: Weak<Inbound>) {
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => {
                // Out of descriptors and the like: back off, do not spin.
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let Some(inbound) = inbound.upgrade() else {
            return;
        };
        let Ok(permit) = inbound.pending.clone().try_acquire_owned() else {
            inbound.reject("too_many_handshakes");
            continue;
        };
        tokio::spawn(inbound.admit(PeerStream::Tcp(stream), addr, permit));
    }
}

/// Inbound uTP: connections that completed the uTP handshake through the
/// gate go through the same handshake gate and session check as TCP ones.
async fn utp_accept_loop(gate: Arc<UtpAcceptor>, inbound: Weak<Inbound>) {
    // Fails once the gate is closed.
    while let Ok(stream) = gate.accept().await {
        let Some(inbound) = inbound.upgrade() else {
            return;
        };
        let addr = stream.peer_addr();
        let stream = PeerStream::Utp(stream);
        let Ok(permit) = inbound.pending.clone().try_acquire_owned() else {
            inbound.refuse(stream, "too_many_handshakes");
            continue;
        };
        tokio::spawn(inbound.admit(stream, addr, permit));
    }
}

async fn announce_loop(
    announcer: Arc<Announcer>,
    mut params: AnnounceParams,
    trackers: Arc<Mutex<Vec<(String, bool)>>>,
    interval: Duration,
    dht: bool,
) {
    params.event = AnnounceEvent::Started;
    loop {
        let snapshot = trackers_vec(trackers.clone());
        let (ok, failed) = announcer.round(&params, &snapshot, dht).await;
        tracing::info!(event = "tunnel.announce_round", ok, failed);
        params.event = AnnounceEvent::None;
        tokio::time::sleep(interval).await;
    }
}

fn trackers_vec(trackers: Arc<Mutex<Vec<(String, bool)>>>) -> Vec<(String, bool)> {
    trackers.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Add the trackers not there yet (at most MAX_ANNOUNCE_TRACKERS · 2 in a
/// swarm); returns those added.
fn merge(into: &Mutex<Vec<(String, bool)>>, add: Vec<(String, bool)>) -> Vec<(String, bool)> {
    let mut list = into.lock().unwrap_or_else(|e| e.into_inner());
    let mut fresh = Vec::new();
    for (url, private) in add {
        if list.len() >= 2 * super::codec::MAX_ANNOUNCE_TRACKERS {
            break;
        }
        if list.iter().any(|(u, _)| *u == url) {
            continue;
        }
        list.push((url.clone(), private));
        fresh.push((url, private));
    }
    fresh
}

/// Of `add`, those `merge` would add: not in `list` yet, deduplicated,
/// within the room left under the cap.
fn unknown(list: &Mutex<Vec<(String, bool)>>, add: Vec<(String, bool)>) -> Vec<(String, bool)> {
    let list = list.lock().unwrap_or_else(|e| e.into_inner());
    let room = (2 * super::codec::MAX_ANNOUNCE_TRACKERS).saturating_sub(list.len());
    let mut out: Vec<(String, bool)> = Vec::new();
    for (url, private) in add {
        if out.len() >= room {
            break;
        }
        if !list.iter().any(|(u, _)| *u == url) && !out.iter().any(|(u, _)| *u == url) {
            out.push((url, private));
        }
    }
    out
}

/// An `http`, `https` or `udp` URL with a host, on port 80, 443 or >= 1024
/// as trackers are (the port a session names is not a way to reach other
/// services through the gateway; the SSRF guard applies when it is
/// announced to: tracker/http.rs, tracker/udp.rs).
pub fn tracker_url_ok(url: &str) -> bool {
    let Ok(u) = url::Url::parse(url) else {
        return false;
    };
    if !matches!(u.scheme(), "http" | "https" | "udp") || u.host().is_none() {
        return false;
    }
    matches!(u.port_or_known_default(), Some(80 | 443 | 1024..))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> InboundSettings {
        InboundSettings {
            listen: "127.0.0.1:0".parse().unwrap(),
            announce_port: 0,
            utp: SocketConfig::default(),
            interval: Duration::from_secs(900),
            dht: false,
            per_session: 2,
            max_inbound: 3,
            max_pending: 4,
            per_socket: 2,
            max_announces: 3,
            per_ip: 10,
            rounds_per_ip: 100,
            rounds_max: 100,
            handshake_timeout: Duration::from_secs(1),
        }
    }

    fn member(
        inbound: &Inbound,
        group: &Arc<Group>,
    ) -> (Member, mpsc::UnboundedReceiver<InboundPeer>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let m = Member {
            socket: inbound.socket_id(),
            reserved: [1; 8],
            peer_id: [2; 20],
            group: group.clone(),
            tx,
            client: "127.0.0.1".parse().unwrap(),
            allow_private: true,
        };
        (m, rx)
    }

    fn again(m: &Member) -> Member {
        Member {
            socket: m.socket,
            reserved: m.reserved,
            peer_id: m.peer_id,
            group: m.group.clone(),
            tx: m.tx.clone(),
            client: m.client,
            allow_private: m.allow_private,
        }
    }

    #[tokio::test]
    async fn registry_tracks_swarms_and_the_listener_lives_with_them() {
        let inbound = Inbound::new(settings(), Arc::new(Announcer::trackers_only()));
        let group = Arc::new(Group::new());
        let (a, _ra) = member(&inbound, &group);
        let (b, _rb) = member(&inbound, &group);
        let (sa, sb) = (a.socket, b.socket);
        assert_eq!(inbound.listening_port(), None);
        assert_eq!(
            inbound.announce(again(&a), [1; 20], vec![]),
            announce_status::ACTIVE
        );
        let port = inbound.listening_port().expect("listening");
        assert!(inbound.is_announced(&[1; 20]));
        // Idempotent for the same WebSocket; another joins the same swarm.
        assert_eq!(
            inbound.announce(again(&a), [1; 20], vec![]),
            announce_status::ACTIVE
        );
        assert_eq!(
            inbound.announce(again(&b), [1; 20], vec![]),
            announce_status::ACTIVE
        );
        assert_eq!(
            inbound.announce(again(&a), [2; 20], vec![]),
            announce_status::ACTIVE
        );
        // Two per WebSocket.
        assert_eq!(
            inbound.announce(again(&a), [3; 20], vec![]),
            announce_status::LIMIT_REACHED
        );
        assert_eq!(
            inbound.announce(again(&b), [3; 20], vec![]),
            announce_status::ACTIVE
        );
        // Three in total.
        assert_eq!(
            inbound.announce(again(&b), [4; 20], vec![]),
            announce_status::LIMIT_REACHED
        );
        assert_eq!(inbound.listening_port(), Some(port));
        inbound.withdraw(sa, &[1; 20]);
        assert!(inbound.is_announced(&[1; 20]), "b still announces it");
        inbound.withdraw_all(sb);
        assert!(!inbound.is_announced(&[1; 20]));
        assert!(!inbound.is_announced(&[3; 20]));
        assert!(
            inbound.listening_port().is_some(),
            "a still announces [2; 20]"
        );
        inbound.withdraw(sa, &[2; 20]);
        assert_eq!(inbound.listening_port(), None);
    }

    #[tokio::test]
    async fn pick_respects_session_and_total_limits_and_private_addresses() {
        let inbound = Inbound::new(settings(), Arc::new(Announcer::trackers_only()));
        let g1 = Arc::new(Group::new());
        let g2 = Arc::new(Group::new());
        let (a, _ra) = member(&inbound, &g1);
        let (mut b, _rb) = member(&inbound, &g2);
        b.allow_private = false;
        inbound.announce(a, [1; 20], vec![]);
        inbound.announce(b, [1; 20], vec![]);
        let local: IpAddr = "127.0.0.1".parse().unwrap();
        let public: IpAddr = "8.8.8.8".parse().unwrap();
        assert_eq!(inbound.pick(&[9; 20], public).err(), Some("no_session"));
        // Loopback only to the member that allows it, two per session.
        let s1 = inbound.pick(&[1; 20], local).ok().unwrap().2;
        let s2 = inbound.pick(&[1; 20], local).ok().unwrap().2;
        assert_eq!(g1.inbound.load(Ordering::Acquire), 2);
        assert!(inbound.pick(&[1; 20], local).is_err());
        // A public address goes to the other session; then the total (3) is reached.
        let s3 = inbound.pick(&[1; 20], public).ok().unwrap().2;
        assert_eq!(g2.inbound.load(Ordering::Acquire), 1);
        assert!(inbound.pick(&[1; 20], public).is_err());
        drop((s1, s2, s3));
        assert_eq!(g1.inbound.load(Ordering::Acquire), 0);
        assert!(inbound.pick(&[1; 20], public).is_ok());
    }

    #[tokio::test]
    async fn one_client_ip_holds_a_bounded_number_of_swarms() {
        let mut s = settings();
        s.per_ip = 2;
        s.max_announces = 10;
        let inbound = Inbound::new(s, Arc::new(Announcer::trackers_only()));
        let group = Arc::new(Group::new());
        let (a, _ra) = member(&inbound, &group);
        let (b, _rb) = member(&inbound, &group);
        let (mut c, _rc) = member(&inbound, &group);
        c.client = "192.0.2.1".parse().unwrap();
        assert_eq!(
            inbound.announce(again(&a), [1; 20], vec![]),
            announce_status::ACTIVE
        );
        assert_eq!(
            inbound.announce(again(&b), [2; 20], vec![]),
            announce_status::ACTIVE
        );
        // Same IP, a third info-hash: refused; one it is in already: fine.
        assert_eq!(
            inbound.announce(again(&b), [3; 20], vec![]),
            announce_status::LIMIT_REACHED
        );
        assert_eq!(
            inbound.announce(again(&b), [1; 20], vec![]),
            announce_status::ACTIVE
        );
        // Another IP is not affected.
        assert_eq!(
            inbound.announce(again(&c), [3; 20], vec![]),
            announce_status::ACTIVE
        );
        // And joining a swarm of another IP counts for this one.
        inbound.withdraw_all(c.socket);
        assert_eq!(
            inbound.announce(again(&c), [1; 20], vec![]),
            announce_status::ACTIVE
        );
        assert_eq!(
            inbound.announce(again(&c), [2; 20], vec![]),
            announce_status::ACTIVE
        );
        let (mut d, _rd) = member(&inbound, &group);
        d.client = "192.0.2.1".parse().unwrap();
        assert_eq!(
            inbound.announce(again(&d), [4; 20], vec![]),
            announce_status::LIMIT_REACHED
        );
    }

    #[tokio::test]
    async fn announce_withdraw_cycles_are_rate_limited_per_ip_and_in_total() {
        let mut s = settings();
        s.rounds_per_ip = 3;
        s.rounds_max = 5;
        let inbound = Inbound::new(s, Arc::new(Announcer::trackers_only()));
        let group = Arc::new(Group::new());
        let (a, _ra) = member(&inbound, &group);
        for _ in 0..3 {
            assert_eq!(
                inbound.announce(again(&a), [1; 20], vec![]),
                announce_status::ACTIVE
            );
            inbound.withdraw(a.socket, &[1; 20]);
        }
        // The fourth new swarm within the minute: refused, nothing started.
        assert_eq!(
            inbound.announce(again(&a), [1; 20], vec![]),
            announce_status::LIMIT_REACHED
        );
        assert!(!inbound.is_announced(&[1; 20]));
        assert_eq!(inbound.listening_port(), None);
        // Another IP has its own budget, within the gateway's (5).
        let (mut b, _rb) = member(&inbound, &group);
        b.client = "2001:db8:1:2:3:4:5:6".parse().unwrap();
        assert_eq!(
            inbound.announce(again(&b), [2; 20], vec![]),
            announce_status::ACTIVE
        );
        // Trackers added to a swarm start a round: charged too. Over the
        // gateway's budget they are ignored, the announce stays active.
        let t1 = vec!["udp://t1.example:1337/announce".to_string()];
        assert_eq!(
            inbound.announce(again(&b), [2; 20], t1),
            announce_status::ACTIVE
        );
        let t2 = vec!["udp://t2.example:1337/announce".to_string()];
        assert_eq!(
            inbound.announce(again(&b), [2; 20], t2),
            announce_status::ACTIVE
        );
        let trackers = {
            let st = inbound.lock();
            trackers_vec(st.swarms[&[2; 20]].trackers.clone())
        };
        assert_eq!(trackers.len(), 1, "the second addition was over the budget");
        // Known trackers cost nothing.
        let again1 = vec!["udp://t1.example:1337/announce".to_string()];
        assert_eq!(
            inbound.announce(again(&b), [2; 20], again1),
            announce_status::ACTIVE
        );
    }

    #[test]
    fn an_ipv6_client_is_its_64() {
        let a: IpAddr = "2001:db8:1:2:3:4:5:6".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:ffff::1".parse().unwrap();
        let c: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_eq!(ip_key(a), ip_key(b));
        assert_ne!(ip_key(a), ip_key(c));
        let mapped: IpAddr = "::ffff:192.0.2.1".parse().unwrap();
        assert_eq!(ip_key(mapped), "192.0.2.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn rates_use_fixed_windows_and_refusals_are_free() {
        let mut r = Rates::default();
        let t0 = Instant::now();
        let ip: IpAddr = "192.0.2.7".parse().unwrap();
        assert!(r.take(ip, t0, 2, 10));
        assert!(r.take(ip, t0, 2, 10));
        assert!(!r.take(ip, t0, 2, 10));
        assert!(!r.take(ip, t0, 2, 10));
        assert!(r.take(ip, t0 + RATE_WINDOW, 2, 10));
        assert_eq!(r.all.map(|(_, n)| n), Some(1));
    }

    #[test]
    fn tracker_urls_must_be_http_https_or_udp_with_a_host() {
        assert!(tracker_url_ok("udp://tracker.example:1337/announce"));
        assert!(tracker_url_ok("https://t.example/announce"));
        assert!(tracker_url_ok("http://t.example/announce"));
        assert!(tracker_url_ok("http://t.example:6969/announce"));
        assert!(tracker_url_ok("udp://t.example:80/announce"));
        // Ports of other services, or none for udp.
        assert!(!tracker_url_ok("http://t.example:25/announce"));
        assert!(!tracker_url_ok("udp://t.example:53/announce"));
        assert!(!tracker_url_ok("udp://t.example/announce"));
        assert!(!tracker_url_ok("http://t.example:0/announce"));
        assert!(!tracker_url_ok("wss://t.example/announce"));
        assert!(!tracker_url_ok("file:///etc/passwd"));
        assert!(!tracker_url_ok("not a url"));
    }
}
