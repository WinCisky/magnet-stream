//! uTP multiplexer on one UDP socket (M4-04d; docs/decisions/utp.md ›
//! Split of M4-04).
//!
//! One [`UtpSocket`] owns one `tokio::net::UdpSocket` and every uTP
//! connection on it, inbound and outbound alike, so they all share the local
//! port (R2: BEP 55 needs our outbound packets to leave from the port the
//! remote's NAT has seen). A background task (the driver) reads datagrams,
//! routes each by `(remote address, connection id)` to its
//! [`Connection`] (`super::conn`, sans-IO) and fires the connections'
//! timers; the application side ([`UtpStream`], `AsyncRead + AsyncWrite`)
//! reads and writes the same connections under one lock. Datagrams are sent
//! with `try_send_to`: one the kernel cannot take now is dropped and
//! counted, and uTP retransmits it like any other loss.
//!
//! **Accept gate, decided by the caller.** A `ST_SYN` that matches no
//! connection is answered only when
//!
//! - an acceptor for its exact address is open ([`UtpSocket::expect`]: a
//!   hole punch, M4-05), or
//! - a [`UtpSocket::connect`] to its exact address is pending (simultaneous
//!   open, R3: the first of the two connections to complete resolves the
//!   connect and the other one is dropped without a reply), or
//! - the listener is open ([`UtpSocket::listen`]: a session announces,
//!   M4-02) and its optional address filter admits the address.
//!
//! Anything else gets no reply: a SYN outside a gate, a packet of any other
//! type for an unknown connection, a datagram that does not decode (strict
//! parsing, `super::packet`). No `ST_RESET` is ever sent to a stranger, and
//! each SYN received draws at most one `ST_STATE` of the same size (the
//! state machine never retransmits it): no reflection, no amplification.
//!
//! **Bounds.** Connections in total and per remote IP, inbound half-open
//! connections (SYN answered, handshake not complete) in total and per
//! remote IP, and connected streams waiting in an acceptor
//! ([`SocketConfig`]). A SYN beyond any of them is ignored without a reply;
//! a connect beyond them fails. Each connection bounds its own buffers and
//! times out its handshake (10 s) and idleness (90 s) (`super::conn`). A
//! stream dropped without reading to the end is closed gracefully (`ST_FIN`)
//! and its incoming bytes discarded; it is aborted if it has not finished
//! within [`SocketConfig::linger`].
//!
//! Nothing here logs; [`UtpSocket::stats`] counts datagrams, drops and
//! connections by reason (no addresses, no payload).
//!
//! Simultaneous open between two endpoints that both [`UtpSocket::connect`]
//! at once may leave each side holding a different one of the two
//! connections when both handshakes complete at the same moment; telling
//! the duplicates apart is the hole-punch layer's job (M4-05,
//! `crate::tunnel::punch`), which holds an [`UtpSocket::expect`] acceptor
//! to see every inbound connection from the address itself and keeps the
//! connection on which the remote answers the BitTorrent handshake.

use std::collections::{HashMap, VecDeque};
use std::future::{pending, poll_fn};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UdpSocket;
use tokio::sync::Notify;
use tokio::task::AbortHandle;

use super::conn::{self, accept_recv_id, CloseReason, Connection, State};
use super::packet::{Packet, PacketType, SeqNr};

/// Datagrams read in one go before timers are looked at again.
const RECV_BATCH: usize = 256;
/// Largest UDP payload.
const MAX_DATAGRAM: usize = 65_535;

/// Limits of one socket. Zero values are raised to 1 at bind time.
#[derive(Debug, Clone)]
pub struct SocketConfig {
    /// Limits and timers of every connection.
    pub conn: conn::Config,
    /// Connections of every kind, in total.
    pub max_connections: usize,
    /// Connections of every kind with one remote IP.
    pub max_connections_per_ip: usize,
    /// Inbound connections whose SYN was answered and whose handshake is
    /// not complete, in total.
    pub max_half_open: usize,
    /// The same, per remote IP.
    pub max_half_open_per_ip: usize,
    /// Per acceptor: half-open connections plus connected streams not yet
    /// taken by [`UtpAcceptor::accept`].
    pub accept_backlog: usize,
    /// How long a dropped stream may take to finish its graceful close.
    pub linger: Duration,
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            conn: conn::Config::default(),
            max_connections: 256,
            max_connections_per_ip: 8,
            max_half_open: 64,
            max_half_open_per_ip: 4,
            accept_backlog: 32,
            linger: Duration::from_secs(10),
        }
    }
}

impl SocketConfig {
    fn sanitized(mut self) -> Self {
        self.max_connections = self.max_connections.max(1);
        self.max_connections_per_ip = self.max_connections_per_ip.max(1);
        self.max_half_open = self.max_half_open.max(1);
        self.max_half_open_per_ip = self.max_half_open_per_ip.max(1);
        self.accept_backlog = self.accept_backlog.max(1);
        self
    }
}

/// Counters of one socket (no addresses, no payload).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SocketStats {
    pub datagrams_received: u64,
    pub datagrams_sent: u64,
    /// The kernel did not take a datagram (dropped, uTP retransmits).
    pub send_errors: u64,
    /// Not a uTP packet as `super::packet` parses it.
    pub dropped_malformed: u64,
    /// A non-SYN packet for no known connection.
    pub dropped_unknown: u64,
    /// A SYN for no connection with no gate open for its address.
    pub dropped_syn_no_gate: u64,
    /// A SYN beyond a limit (connections, half-open, backlog).
    pub dropped_syn_limit: u64,
    /// A SYN whose connection id collides with one in use.
    pub dropped_syn_collision: u64,
    /// SYNs answered (a new inbound connection each).
    pub syn_accepted: u64,
    /// Inbound connections that completed their handshake.
    pub inbound_connected: u64,
    /// Outbound connections that completed their handshake.
    pub outbound_connected: u64,
    /// Connects that failed (timeout, reset, limits).
    pub connect_failed: u64,
    /// Inbound half-open connections dropped: handshake timeout, or the
    /// connect or acceptor they were for went away.
    pub half_open_dropped: u64,
    /// Dropped streams aborted after [`SocketConfig::linger`].
    pub linger_aborted: u64,
}

/// Admits (`true`) or ignores the SYN of an address, for the listener.
pub type AddressFilter = Arc<dyn Fn(SocketAddr) -> bool + Send + Sync>;

type Key = (SocketAddr, u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owner {
    /// Held by a [`UtpStream`].
    Stream,
    /// Its stream was dropped: closing, incoming bytes discarded.
    Detached { until: Instant },
    /// The outbound connection of a pending connect.
    Connect(u64),
    /// An inbound connection from the address of a pending connect.
    ConnectAlt(u64),
    /// Inbound, half-open, for an acceptor.
    Gate(u64),
    /// Inbound, connected, waiting in an acceptor's queue.
    Queued(u64),
}

struct Entry {
    conn: Connection,
    owner: Owner,
    /// Counted in the half-open totals (inbound, in `SynRecv`).
    half_open: bool,
    /// The application shut down its sending side.
    shutdown: bool,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
}

enum Scope {
    Any(Option<AddressFilter>),
    Addr(SocketAddr),
}

struct Gate {
    id: u64,
    scope: Scope,
    /// Half-open connections for this acceptor.
    pending: usize,
    ready: VecDeque<Key>,
    waker: Option<Waker>,
}

struct PendingConnect {
    id: u64,
    addr: SocketAddr,
    /// The outbound connection and the inbound alternatives.
    keys: Vec<Key>,
    result: Option<io::Result<Key>>,
    waker: Option<Waker>,
}

#[derive(Default, Clone, Copy)]
struct IpCount {
    conns: usize,
    half_open: usize,
}

struct Inner {
    cfg: SocketConfig,
    conns: HashMap<Key, Entry>,
    ips: HashMap<IpAddr, IpCount>,
    half_open: usize,
    gates: Vec<Gate>,
    connects: Vec<PendingConnect>,
    next_id: u64,
    stats: SocketStats,
    out: Vec<u8>,
    /// Wakers to call once the lock is released.
    wake: Vec<Waker>,
}

struct Shared {
    socket: Arc<UdpSocket>,
    local: SocketAddr,
    inner: Mutex<Inner>,
    /// Wakes the driver: a timer may have moved.
    notify: Arc<Notify>,
    driver: Mutex<Option<AbortHandle>>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let handle = self.driver.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(handle) = handle.take() {
            handle.abort();
        }
    }
}

/// The guard of [`Shared::inner`]; wakes the collected wakers on drop,
/// after the lock is released.
struct Locked<'a> {
    guard: Option<MutexGuard<'a, Inner>>,
}

impl std::ops::Deref for Locked<'_> {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        self.guard.as_ref().expect("held until drop")
    }
}

impl std::ops::DerefMut for Locked<'_> {
    fn deref_mut(&mut self) -> &mut Inner {
        self.guard.as_mut().expect("held until drop")
    }
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        let wake = self
            .guard
            .as_mut()
            .map(|g| std::mem::take(&mut g.wake))
            .unwrap_or_default();
        self.guard = None;
        for w in wake {
            w.wake();
        }
    }
}

impl Shared {
    fn lock(&self) -> Locked<'_> {
        Locked {
            guard: Some(self.inner.lock().unwrap_or_else(|e| e.into_inner())),
        }
    }
}

fn canonical(addr: SocketAddr) -> IpAddr {
    addr.ip().to_canonical()
}

fn close_error(reason: CloseReason) -> io::Error {
    let kind = match reason {
        CloseReason::Graceful => io::ErrorKind::BrokenPipe,
        CloseReason::Reset => io::ErrorKind::ConnectionReset,
        CloseReason::Aborted => io::ErrorKind::ConnectionAborted,
        CloseReason::ConnectTimeout
        | CloseReason::HandshakeTimeout
        | CloseReason::RetransmitTimeout
        | CloseReason::IdleTimeout => io::ErrorKind::TimedOut,
    };
    io::Error::new(kind, reason.code())
}

fn limit_error() -> io::Error {
    io::Error::other("utp connection limit")
}

impl Inner {
    fn new(cfg: SocketConfig) -> Self {
        Self {
            cfg,
            conns: HashMap::new(),
            ips: HashMap::new(),
            half_open: 0,
            gates: Vec::new(),
            connects: Vec::new(),
            next_id: 1,
            stats: SocketStats::default(),
            out: Vec::with_capacity(conn::MAX_PAYLOAD + 64),
            wake: Vec::new(),
        }
    }

    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// Sends every datagram `key`'s connection has due.
    fn flush(&mut self, now: Instant, key: Key, socket: &UdpSocket) {
        let Some(e) = self.conns.get_mut(&key) else {
            return;
        };
        while e.conn.poll_transmit(now, &mut self.out) {
            match socket.try_send_to(&self.out, key.0) {
                Ok(_) => self.stats.datagrams_sent += 1,
                Err(_) => self.stats.send_errors += 1,
            }
        }
    }

    fn insert(&mut self, key: Key, conn: Connection, owner: Owner, half_open: bool) {
        let ip = self.ips.entry(canonical(key.0)).or_default();
        ip.conns += 1;
        if half_open {
            ip.half_open += 1;
            self.half_open += 1;
        }
        self.conns.insert(
            key,
            Entry {
                conn,
                owner,
                half_open,
                shutdown: false,
                read_waker: None,
                write_waker: None,
            },
        );
    }

    fn uncount_half_open(&mut self, key: Key) {
        let Some(e) = self.conns.get_mut(&key) else {
            return;
        };
        if !e.half_open {
            return;
        }
        e.half_open = false;
        self.half_open -= 1;
        if let Some(ip) = self.ips.get_mut(&canonical(key.0)) {
            ip.half_open = ip.half_open.saturating_sub(1);
        }
    }

    /// Forgets a connection (sends nothing).
    fn remove(&mut self, key: Key) {
        self.uncount_half_open(key);
        let Some(mut e) = self.conns.remove(&key) else {
            return;
        };
        let ip = canonical(key.0);
        if let Some(c) = self.ips.get_mut(&ip) {
            c.conns = c.conns.saturating_sub(1);
            if c.conns == 0 {
                self.ips.remove(&ip);
            }
        }
        self.wake.extend(e.read_waker.take());
        self.wake.extend(e.write_waker.take());
    }

    fn gate_mut(&mut self, id: u64) -> Option<&mut Gate> {
        self.gates.iter_mut().find(|g| g.id == id)
    }

    /// Resolves a pending connect with `result`; every other connection it
    /// started or admitted is dropped without a reply (an outbound one
    /// still in `SynSent`, an inbound one still in `SynRecv`).
    fn resolve_connect(&mut self, id: u64, result: io::Result<Key>) {
        let Some(p) = self.connects.iter_mut().find(|p| p.id == id) else {
            return;
        };
        let winner = result.as_ref().ok().copied();
        let others: Vec<Key> = p.keys.drain(..).filter(|k| Some(*k) != winner).collect();
        if winner.is_none() {
            self.stats.connect_failed += 1;
        }
        p.result = Some(result);
        self.wake.extend(p.waker.take());
        for k in others {
            if self.conns.get(&k).is_some_and(|e| e.half_open) {
                self.stats.half_open_dropped += 1;
            }
            self.remove(k);
        }
    }

    /// Hands a connection on after its state may have changed.
    fn settle(&mut self, key: Key) {
        let Some(e) = self.conns.get_mut(&key) else {
            return;
        };
        let state = e.conn.state();
        let established = matches!(state, State::Connected | State::TimeWait);
        let closed = e.conn.is_closed();
        if state != State::SynRecv && e.half_open {
            self.uncount_half_open(key);
        }
        let Some(e) = self.conns.get_mut(&key) else {
            return;
        };
        match e.owner {
            Owner::Stream => {
                self.wake.extend(e.read_waker.take());
                self.wake.extend(e.write_waker.take());
            }
            Owner::Detached { .. } => {
                let mut sink = [0u8; 4096];
                while e.conn.read(&mut sink) > 0 {}
                if closed {
                    self.remove(key);
                }
            }
            Owner::Connect(id) | Owner::ConnectAlt(id) => {
                let outbound = matches!(e.owner, Owner::Connect(_));
                if established {
                    e.owner = Owner::Stream;
                    if outbound {
                        self.stats.outbound_connected += 1;
                    } else {
                        self.stats.inbound_connected += 1;
                    }
                    self.resolve_connect(id, Ok(key));
                } else if closed {
                    let reason = e.conn.close_reason().unwrap_or(CloseReason::Aborted);
                    if outbound {
                        self.resolve_connect(id, Err(close_error(reason)));
                    } else {
                        self.stats.half_open_dropped += 1;
                    }
                    self.remove(key);
                    if let Some(p) = self.connects.iter_mut().find(|p| p.id == id) {
                        p.keys.retain(|k| *k != key);
                    }
                }
            }
            Owner::Gate(id) => {
                if established {
                    e.owner = Owner::Queued(id);
                    self.stats.inbound_connected += 1;
                    if let Some(g) = self.gate_mut(id) {
                        g.pending = g.pending.saturating_sub(1);
                        g.ready.push_back(key);
                        let w = g.waker.take();
                        self.wake.extend(w);
                    }
                } else if closed {
                    self.stats.half_open_dropped += 1;
                    if let Some(g) = self.gate_mut(id) {
                        g.pending = g.pending.saturating_sub(1);
                    }
                    self.remove(key);
                }
            }
            Owner::Queued(id) => {
                if closed {
                    if let Some(g) = self.gate_mut(id) {
                        g.ready.retain(|k| *k != key);
                    }
                    self.remove(key);
                }
            }
        }
    }

    /// One datagram from `from`.
    fn on_datagram(&mut self, now: Instant, data: &[u8], from: SocketAddr, socket: &UdpSocket) {
        self.stats.datagrams_received += 1;
        let Ok(packet) = Packet::decode(data) else {
            self.stats.dropped_malformed += 1;
            return;
        };
        let h = packet.header;
        let id = if h.packet_type == PacketType::Syn {
            accept_recv_id(h.connection_id)
        } else {
            h.connection_id
        };
        let key = (from, id);
        if let Some(e) = self.conns.get_mut(&key) {
            e.conn.handle_packet(now, &packet);
            self.flush(now, key, socket);
            self.settle(key);
            return;
        }
        if h.packet_type != PacketType::Syn {
            self.stats.dropped_unknown += 1;
            return;
        }
        self.on_syn(now, key, &packet, socket);
    }

    /// Where an unknown SYN from `from` would go, if anywhere.
    fn gate_for(&self, from: SocketAddr) -> Option<Owner> {
        if let Some(g) = self
            .gates
            .iter()
            .find(|g| matches!(g.scope, Scope::Addr(a) if a == from))
        {
            return Some(Owner::Gate(g.id));
        }
        if let Some(p) = self
            .connects
            .iter()
            .find(|p| p.addr == from && p.result.is_none())
        {
            return Some(Owner::ConnectAlt(p.id));
        }
        self.gates.iter().find_map(|g| match &g.scope {
            Scope::Any(filter) if filter.as_ref().is_none_or(|f| f(from)) => {
                Some(Owner::Gate(g.id))
            }
            _ => None,
        })
    }

    fn on_syn(&mut self, now: Instant, key: Key, packet: &Packet<'_>, socket: &UdpSocket) {
        let from = key.0;
        let Some(owner) = self.gate_for(from) else {
            self.stats.dropped_syn_no_gate += 1;
            return;
        };
        // Its id (or the one we would answer on) is one of ours already.
        if self
            .conns
            .contains_key(&(from, packet.header.connection_id))
        {
            self.stats.dropped_syn_collision += 1;
            return;
        }
        let ip = self.ips.get(&canonical(from)).copied().unwrap_or_default();
        let backlog_full = match owner {
            Owner::Gate(id) => self
                .gates
                .iter()
                .find(|g| g.id == id)
                .is_none_or(|g| g.pending + g.ready.len() >= self.cfg.accept_backlog),
            _ => false,
        };
        if self.conns.len() >= self.cfg.max_connections
            || self.half_open >= self.cfg.max_half_open
            || ip.conns >= self.cfg.max_connections_per_ip
            || ip.half_open >= self.cfg.max_half_open_per_ip
            || backlog_full
        {
            self.stats.dropped_syn_limit += 1;
            return;
        }
        let Some(conn) =
            Connection::accept(self.cfg.conn.clone(), now, packet, SeqNr(rand::random()))
        else {
            return;
        };
        self.stats.syn_accepted += 1;
        self.insert(key, conn, owner, true);
        match owner {
            Owner::Gate(id) => {
                if let Some(g) = self.gate_mut(id) {
                    g.pending += 1;
                }
            }
            Owner::ConnectAlt(id) => {
                if let Some(p) = self.connects.iter_mut().find(|p| p.id == id) {
                    p.keys.push(key);
                }
            }
            _ => {}
        }
        // The one ST_STATE this SYN draws.
        self.flush(now, key, socket);
    }

    /// The earliest timer of any connection.
    fn next_deadline(&self) -> Option<Instant> {
        self.conns
            .values()
            .filter_map(|e| {
                let linger = match e.owner {
                    Owner::Detached { until } => Some(until),
                    _ => None,
                };
                match (e.conn.poll_timeout(), linger) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                }
            })
            .min()
    }

    /// Fires every timer due at `now`.
    fn on_timers(&mut self, now: Instant, socket: &UdpSocket) {
        let due: Vec<(Key, bool)> = self
            .conns
            .iter()
            .filter_map(|(k, e)| {
                let lingered = matches!(e.owner, Owner::Detached { until } if until <= now);
                let timer = e.conn.poll_timeout().is_some_and(|t| t <= now);
                (lingered || timer).then_some((*k, lingered))
            })
            .collect();
        for (key, lingered) in due {
            let Some(e) = self.conns.get_mut(&key) else {
                continue;
            };
            if lingered {
                e.conn.abort();
                self.stats.linger_aborted += 1;
            } else {
                e.conn.handle_timeout(now);
            }
            self.flush(now, key, socket);
            self.settle(key);
        }
    }

    /// A free receive id for an outbound connection to `addr`.
    fn free_recv_id(&self, addr: SocketAddr) -> Option<u16> {
        (0..64).map(|_| rand::random::<u16>()).find(|&id| {
            !self.conns.contains_key(&(addr, id))
                && !self.conns.contains_key(&(addr, id.wrapping_add(1)))
        })
    }
}

/// A uTP endpoint on one UDP socket: outbound connects and inbound
/// connections through caller-decided gates share its port. Cloning gives
/// another handle to the same socket; the socket (and its driver) lives as
/// long as any handle, acceptor or stream.
#[derive(Clone)]
pub struct UtpSocket {
    shared: Arc<Shared>,
}

impl UtpSocket {
    /// Binds a UDP socket at `addr` and starts the driver (needs a tokio
    /// runtime).
    pub async fn bind(addr: SocketAddr, cfg: SocketConfig) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Self::from_socket(socket, cfg)
    }

    /// Takes over an already bound socket.
    pub fn from_socket(socket: UdpSocket, cfg: SocketConfig) -> io::Result<Self> {
        let local = socket.local_addr()?;
        let socket = Arc::new(socket);
        let notify = Arc::new(Notify::new());
        let shared = Arc::new(Shared {
            socket: socket.clone(),
            local,
            inner: Mutex::new(Inner::new(cfg.sanitized())),
            notify: notify.clone(),
            driver: Mutex::new(None),
        });
        let task = tokio::spawn(drive(Arc::downgrade(&shared), socket, notify));
        *shared.driver.lock().unwrap_or_else(|e| e.into_inner()) = Some(task.abort_handle());
        Ok(Self { shared })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.shared.local
    }

    /// A handle that does not keep the socket alive.
    pub fn downgrade(&self) -> WeakUtpSocket {
        WeakUtpSocket {
            shared: Arc::downgrade(&self.shared),
        }
    }

    pub fn stats(&self) -> SocketStats {
        self.shared.lock().stats
    }

    /// Connections of every kind, and inbound half-open ones.
    pub fn connection_counts(&self) -> (usize, usize) {
        let inner = self.shared.lock();
        (inner.conns.len(), inner.half_open)
    }

    /// Opens a uTP connection to `addr` from this socket's port. While it is
    /// pending, a SYN from `addr` is answered too (simultaneous open): the
    /// first of the two connections to complete is returned. Fails after
    /// the connect timeout, on a reset, or beyond the connection limits.
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<UtpStream> {
        let shared = &self.shared;
        let id = {
            let mut inner = shared.lock();
            let ip = inner.ips.get(&canonical(addr)).copied().unwrap_or_default();
            if inner.conns.len() >= inner.cfg.max_connections
                || ip.conns >= inner.cfg.max_connections_per_ip
            {
                inner.stats.connect_failed += 1;
                return Err(limit_error());
            }
            let Some(recv_id) = inner.free_recv_id(addr) else {
                inner.stats.connect_failed += 1;
                return Err(limit_error());
            };
            let now = Instant::now();
            let id = inner.id();
            let conn =
                Connection::connect(inner.cfg.conn.clone(), now, recv_id, SeqNr(rand::random()));
            let key = (addr, recv_id);
            inner.insert(key, conn, Owner::Connect(id), false);
            inner.connects.push(PendingConnect {
                id,
                addr,
                keys: vec![key],
                result: None,
                waker: None,
            });
            inner.flush(now, key, &shared.socket);
            id
        };
        shared.notify.notify_one();
        let _guard = ConnectGuard {
            shared: shared.clone(),
            id,
        };
        let result = poll_fn(|cx| {
            let mut inner = shared.lock();
            let Some(p) = inner.connects.iter_mut().find(|p| p.id == id) else {
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::ConnectionAborted)));
            };
            match p.result.take() {
                Some(r) => Poll::Ready(r),
                None => {
                    p.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await;
        result.map(|key| UtpStream {
            shared: shared.clone(),
            key,
        })
    }

    /// Opens the listener: SYNs from any address `filter` admits (all when
    /// `None`) are answered, within the limits. One at a time; dropping the
    /// acceptor closes the gate.
    pub fn listen(&self, filter: Option<AddressFilter>) -> io::Result<UtpAcceptor> {
        let mut inner = self.shared.lock();
        if inner.gates.iter().any(|g| matches!(g.scope, Scope::Any(_))) {
            return Err(io::Error::from(io::ErrorKind::AddrInUse));
        }
        Ok(self.open_gate(&mut inner, Scope::Any(filter)))
    }

    /// Opens a gate for SYNs from `addr` only (a hole punch, or the inbound
    /// side of a simultaneous open). One per address; dropping the acceptor
    /// closes the gate.
    pub fn expect(&self, addr: SocketAddr) -> io::Result<UtpAcceptor> {
        let mut inner = self.shared.lock();
        if inner
            .gates
            .iter()
            .any(|g| matches!(g.scope, Scope::Addr(a) if a == addr))
        {
            return Err(io::Error::from(io::ErrorKind::AddrInUse));
        }
        Ok(self.open_gate(&mut inner, Scope::Addr(addr)))
    }

    fn open_gate(&self, inner: &mut Inner, scope: Scope) -> UtpAcceptor {
        let id = inner.id();
        inner.gates.push(Gate {
            id,
            scope,
            pending: 0,
            ready: VecDeque::new(),
            waker: None,
        });
        UtpAcceptor {
            shared: self.shared.clone(),
            id,
        }
    }
}

/// A [`UtpSocket`] that may be gone: the socket lives while any handle,
/// acceptor or stream does.
#[derive(Clone)]
pub struct WeakUtpSocket {
    shared: Weak<Shared>,
}

impl WeakUtpSocket {
    pub fn upgrade(&self) -> Option<UtpSocket> {
        self.shared.upgrade().map(|shared| UtpSocket { shared })
    }
}

/// Removes a pending connect when its future ends or is dropped; one
/// dropped before taking its result also drops its connections.
struct ConnectGuard {
    shared: Arc<Shared>,
    id: u64,
}

impl Drop for ConnectGuard {
    fn drop(&mut self) {
        let mut inner = self.shared.lock();
        let Some(i) = inner.connects.iter().position(|p| p.id == self.id) else {
            return;
        };
        let p = inner.connects.swap_remove(i);
        let now = Instant::now();
        // Abandoned before the result was taken: forget the half-done
        // connections, close a completed one like a dropped stream.
        for key in p.keys {
            inner.remove(key);
        }
        if let Some(Ok(key)) = p.result {
            detach(&mut inner, now, key, &self.shared.socket);
        }
        drop(inner);
        self.shared.notify.notify_one();
    }
}

/// A gate for inbound connections ([`UtpSocket::listen`],
/// [`UtpSocket::expect`]). Dropping it closes the gate: half-open
/// connections for it are dropped without a reply and connected ones not
/// accepted yet are aborted.
pub struct UtpAcceptor {
    shared: Arc<Shared>,
    id: u64,
}

impl UtpAcceptor {
    /// The next inbound connection that completed its handshake.
    pub async fn accept(&self) -> io::Result<UtpStream> {
        poll_fn(|cx| self.poll_accept(cx)).await
    }

    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<UtpStream>> {
        let mut inner = self.shared.lock();
        let Some(g) = inner.gate_mut(self.id) else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        match g.ready.pop_front() {
            Some(key) => {
                if let Some(e) = inner.conns.get_mut(&key) {
                    e.owner = Owner::Stream;
                }
                Poll::Ready(Ok(UtpStream {
                    shared: self.shared.clone(),
                    key,
                }))
            }
            None => {
                g.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl UtpAcceptor {
    /// Closes the gate now (as dropping it does): half-open connections for
    /// it are dropped without a reply, connected ones not accepted yet are
    /// aborted, and [`UtpAcceptor::accept`] fails from then on.
    pub fn close(&self) {
        let mut inner = self.shared.lock();
        let Some(i) = inner.gates.iter().position(|g| g.id == self.id) else {
            return;
        };
        let waker = inner.gates.swap_remove(i).waker;
        let id = self.id;
        let keys: Vec<(Key, bool)> = inner
            .conns
            .iter()
            .filter_map(|(k, e)| match e.owner {
                Owner::Gate(g) if g == id => Some((*k, false)),
                Owner::Queued(g) if g == id => Some((*k, true)),
                _ => None,
            })
            .collect();
        let now = Instant::now();
        for (key, connected) in keys {
            if connected {
                // An established remote: one ST_RESET.
                if let Some(e) = inner.conns.get_mut(&key) {
                    e.conn.abort();
                }
                inner.flush(now, key, &self.shared.socket);
            } else {
                inner.stats.half_open_dropped += 1;
            }
            inner.remove(key);
        }
        // Woken once the lock is released: accept() sees the gate gone.
        inner.wake.extend(waker);
    }
}

impl Drop for UtpAcceptor {
    fn drop(&mut self) {
        self.close();
    }
}

/// Closes a connection whose stream went away: `ST_FIN` after the bytes
/// written, incoming bytes discarded, aborted after the linger time.
fn detach(inner: &mut Inner, now: Instant, key: Key, socket: &UdpSocket) {
    let linger = inner.cfg.linger;
    let Some(e) = inner.conns.get_mut(&key) else {
        return;
    };
    if e.conn.is_closed() {
        inner.remove(key);
        return;
    }
    e.conn.close();
    e.owner = Owner::Detached {
        until: now + linger,
    };
    e.read_waker = None;
    e.write_waker = None;
    inner.flush(now, key, socket);
    inner.settle(key);
}

/// One uTP connection as a byte stream, like a `TcpStream`: reads return
/// 0 at the remote's `ST_FIN`, [`AsyncWrite::poll_shutdown`] sends ours,
/// dropping it closes gracefully.
pub struct UtpStream {
    shared: Arc<Shared>,
    key: Key,
}

impl std::fmt::Debug for UtpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpStream").finish_non_exhaustive()
    }
}

impl UtpStream {
    pub fn peer_addr(&self) -> SocketAddr {
        self.key.0
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.shared.local
    }

    /// Forgets the connection without sending anything (no `ST_FIN`, no
    /// `ST_RESET`): for a remote that is refused after the uTP handshake
    /// (no reply to a stranger). Its later packets match no connection and
    /// are dropped silently.
    pub fn abandon(self) {
        let mut inner = self.shared.lock();
        inner.remove(self.key);
        // Drop finds nothing left to close.
    }

    /// The connection's own counters.
    pub fn conn_stats(&self) -> Option<conn::Stats> {
        self.shared
            .lock()
            .conns
            .get(&self.key)
            .map(|e| e.conn.stats())
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut inner = this.shared.lock();
        let Some(e) = inner.conns.get_mut(&this.key) else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let n = e.conn.read(buf.initialize_unfilled());
        if n > 0 {
            buf.advance(n);
            // A reopened receive window is announced at once.
            inner.flush(Instant::now(), this.key, &this.shared.socket);
            return Poll::Ready(Ok(()));
        }
        if e.conn.is_eof() {
            return Poll::Ready(Ok(()));
        }
        match e.conn.close_reason() {
            Some(CloseReason::Graceful) => Poll::Ready(Ok(())),
            Some(reason) => Poll::Ready(Err(close_error(reason))),
            None => {
                e.read_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut inner = this.shared.lock();
        let Some(e) = inner.conns.get_mut(&this.key) else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        if e.shutdown {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
        }
        if let Some(reason) = e.conn.close_reason() {
            return Poll::Ready(Err(close_error(reason)));
        }
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let n = e.conn.write(data);
        if n == 0 {
            e.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        inner.flush(Instant::now(), this.key, &this.shared.socket);
        drop(inner);
        // The retransmission timer may have started.
        this.shared.notify.notify_one();
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut inner = this.shared.lock();
        let Some(e) = inner.conns.get_mut(&this.key) else {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::NotConnected)));
        };
        if !e.shutdown {
            e.shutdown = true;
            e.conn.close();
            inner.flush(Instant::now(), this.key, &this.shared.socket);
            drop(inner);
            this.shared.notify.notify_one();
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for UtpStream {
    fn drop(&mut self) {
        let mut inner = self.shared.lock();
        detach(&mut inner, Instant::now(), self.key, &self.shared.socket);
        drop(inner);
        self.shared.notify.notify_one();
    }
}

/// The driver: reads datagrams and fires timers until the socket's last
/// handle is gone.
async fn drive(shared: Weak<Shared>, socket: Arc<UdpSocket>, notify: Arc<Notify>) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let deadline = match shared.upgrade() {
            Some(s) => s.lock().next_deadline(),
            None => return,
        };
        let sleep = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                None => pending::<()>().await,
            }
        };
        tokio::select! {
            r = socket.recv_from(&mut buf) => {
                let Some(s) = shared.upgrade() else { return };
                let mut inner = s.lock();
                let now = Instant::now();
                if let Ok((n, from)) = r {
                    inner.on_datagram(now, &buf[..n], from, &socket);
                }
                for _ in 0..RECV_BATCH {
                    match socket.try_recv_from(&mut buf) {
                        Ok((n, from)) => inner.on_datagram(now, &buf[..n], from, &socket),
                        Err(_) => break,
                    }
                }
                inner.on_timers(now, &socket);
            }
            _ = sleep => {
                let Some(s) = shared.upgrade() else { return };
                s.lock().on_timers(Instant::now(), &socket);
            }
            _ = notify.notified() => {}
        }
    }
}

#[cfg(test)]
#[path = "socket_tests.rs"]
mod tests;
