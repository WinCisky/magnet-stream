//! One tunnel WebSocket and the TCP connections its frames drive: the VPS
//! twin of `apps/gateway/src/tunnel/session.ts`, with the same rules
//! (docs/tunnel-protocol.md › Rules, › Session lifetime):
//!
//! - handshake gate: the gateway writes the 68-byte handshake itself and
//!   relays nothing, either way, until the remote's handshake is valid for
//!   the same info-hash (`gate.rs`); `data` from the client before `opened`
//!   closes that connection with PROTOCOL_ERROR;
//! - at most `max_connections` (default 6) per WebSocket, counted until the
//!   TCP socket is really gone;
//! - only public addresses on ports >= 1024 (`netguard`);
//! - a malformed frame, a text message or a gateway-only frame closes the
//!   whole WebSocket (1002 / 1003);
//! - bytes held for a peer and not yet written are bounded (4 MiB): a peer
//!   that does not read is closed (WRITE_OVERFLOW), the others go on;
//! - a connection with no byte either way for the idle timeout (90 s) is
//!   closed (IDLE), and so is a WebSocket on which the client sent nothing;
//! - the session (all WebSockets with the same `?session=`) ends at its
//!   maximum length (2 h): `close` SESSION_TIMEOUT, then close code 4001.
//!
//! Unlike the Durable Object, a connect that timed out does not keep its
//! slot: the 20 s hold is a Cloudflare platform quirk (OQ-03), not a rule.
//!
//! Inbound (M4-02, `inbound.rs`): `announce` joins the swarm of an
//! info-hash for this WebSocket; inbound peers that handshook for it arrive
//! already answered and are sent as `accept` (ids 0x8000..=0xffff), then
//! relayed like outbound ones. They do not take the outbound slots; their
//! own limits are per session and in total. The announce ends with
//! `withdraw` or with the WebSocket.
//!
//! uTP (M4-04e): an `open` with transport uTP dials from the inbound port's
//! UDP socket (`crate::utp::socket`), only while this session announces the
//! `open`'s info-hash (that socket exists only then), else `failed`
//! UNSUPPORTED_TRANSPORT; the address rules and the handshake gate are the
//! same as for TCP. Inbound uTP peers arrive like TCP ones, with transport
//! uTP in their `accept`. The uTP dial holds the multiplexer's acceptor for
//! the remote's address while it connects, so a hole punch (M4-05, BEP 55)
//! comes up whichever side's SYN gets through first (`punch.rs`).
//!
//! Tasks: this loop owns all state; each connection is one task (connect,
//! gate, then relay) reporting through an event channel; one writer task
//! owns the WebSocket sink. Peer → client bytes take permits from a
//! per-WebSocket budget until the writer has sent them, so a client that
//! does not read stops the reads from its peers (TCP backpressure) instead
//! of growing memory. Nothing is stored; logs carry counters and reason
//! codes only (no client IP, peer address or info-hash).

use super::codec::{
    announce_status, close_code, close_reason, decode_message, encode_frames, fail_reason,
    format_session_end, transport, DecodeLimits, Frame, PeerFields, GATEWAY_CONN_MIN,
};
use super::inbound::{Inbound, InboundPeer, Member};
use super::punch::{self, Passed};
use super::stream::PeerStream;
use crate::netguard;
use crate::peerwire::encode_handshake_with;
use crate::utp::socket::UtpSocket;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::AbortHandle;

/// How often idle connections and the session length are checked.
const SWEEP: Duration = Duration::from_secs(1);
/// Largest read from a peer (one `data` frame at most).
const READ_CHUNK: usize = 64 * 1024;
/// Peer → client bytes read but not yet sent on the WebSocket.
const DOWNSTREAM_BUDGET: usize = 8 * 1024 * 1024;
/// Frames ready together go out as one message, up to about this size.
const MAX_BATCH_BYTES: usize = 512 * 1024;
/// How long the writer may take to flush the last frames and the close.
const WRITER_GRACE: Duration = Duration::from_secs(2);

/// The limits of one WebSocket (config: `TUNNEL_*`).
#[derive(Debug, Clone)]
pub struct Limits {
    pub connect_timeout: Duration,
    pub handshake_timeout: Duration,
    pub max_connections: usize,
    pub idle_timeout: Duration,
    pub session_timeout: Duration,
    pub max_pending_write: usize,
    /// Loopback/private peers allowed (local tests only); ports < 1024 never.
    pub allow_private: bool,
}

/// The WebSockets of one session share its start (for the 2 h cap).
#[derive(Debug)]
pub struct Group {
    pub started: Instant,
    pub expired: AtomicBool,
    /// Inbound connections of the session (all its WebSockets).
    pub inbound: AtomicUsize,
}

impl Group {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            expired: AtomicBool::new(false),
            inbound: AtomicUsize::new(0),
        }
    }

    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.expired.load(Ordering::Relaxed) || self.started.elapsed() >= timeout
    }
}

enum Event {
    Opened {
        conn: u16,
        generation: u64,
        reserved: [u8; 8],
        peer_id: [u8; 20],
        extra: Bytes,
        ms: u128,
    },
    Failed {
        conn: u16,
        generation: u64,
        reason: u8,
        ms: u128,
    },
    Data {
        conn: u16,
        generation: u64,
        payload: Bytes,
        permit: OwnedSemaphorePermit,
    },
    Ended {
        conn: u16,
        generation: u64,
        reason: u8,
    },
    /// A connection task is gone and its socket closed: its slot is free.
    Released,
}

enum Out {
    Message(Vec<u8>, Vec<OwnedSemaphorePermit>),
    Close(u16, String),
}

struct Conn {
    generation: u64,
    open: bool,
    write_tx: mpsc::UnboundedSender<Bytes>,
    /// Bytes queued for the peer and not yet written.
    pending: Arc<AtomicUsize>,
    task: AbortHandle,
    last_activity: Instant,
}

#[derive(Debug, Default)]
struct Stats {
    messages_in: u64,
    messages_out: u64,
    frames_out: u64,
    bytes_up: u64,
    bytes_down: u64,
    opens: u64,
    opened: u64,
    failed: u64,
    idle_closed: u64,
    overflow_closed: u64,
    announces: u64,
    accepted: u64,
    utp_opens: u64,
    utp_accepted: u64,
}

/// This WebSocket's announces; leaving them when it goes, however it goes.
struct Announces {
    inbound: Arc<Inbound>,
    socket: u64,
    tx: mpsc::UnboundedSender<InboundPeer>,
    /// The client's IP: the key of the per-IP announce limits (never logged).
    client: IpAddr,
    /// Info-hashes it announces now.
    active: Vec<[u8; 20]>,
}

impl Drop for Announces {
    fn drop(&mut self) {
        self.inbound.withdraw_all(self.socket);
    }
}

struct Socket {
    limits: Arc<Limits>,
    group: Arc<Group>,
    conns: HashMap<u16, Conn>,
    /// Connection tasks still alive (sockets not yet closed).
    slots: usize,
    generation: u64,
    events_tx: mpsc::UnboundedSender<Event>,
    out_tx: mpsc::UnboundedSender<Out>,
    budget: Arc<Semaphore>,
    batch: Vec<Frame>,
    batch_permits: Vec<OwnedSemaphorePermit>,
    batch_bytes: usize,
    last_message_in: Instant,
    started: Instant,
    ended: bool,
    end_reason: Option<&'static str>,
    stats: Stats,
    /// None: this gateway takes no inbound peers (`announced` UNSUPPORTED).
    announces: Option<Announces>,
    /// Next id to try for an `accept`.
    next_accept: u16,
}

/// Serve one WebSocket until it closes, idles out, breaks the protocol, its
/// session times out, or `shutdown` turns true.
pub async fn run(
    ws: WebSocket,
    limits: Arc<Limits>,
    group: Arc<Group>,
    inbound: Option<Arc<Inbound>>,
    client: IpAddr,
    mut shutdown: watch::Receiver<bool>,
) {
    let (mut sink, mut stream) = ws.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Out>();
    let writer = tokio::spawn(async move {
        while let Some(out) = out_rx.recv().await {
            match out {
                Out::Message(bytes, permits) => {
                    if sink.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                    drop(permits);
                }
                Out::Close(code, reason) => {
                    let frame = CloseFrame {
                        code,
                        reason: reason.into(),
                    };
                    let _ = sink.send(Message::Close(Some(frame))).await;
                    let _ = sink.close().await;
                    break;
                }
            }
        }
    });

    let (events_tx, mut events_rx) = mpsc::unbounded_channel::<Event>();
    let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel::<InboundPeer>();
    let announces = inbound.map(|inbound| Announces {
        socket: inbound.socket_id(),
        inbound,
        tx: inbound_tx,
        client,
        active: Vec::new(),
    });
    let now = Instant::now();
    let mut socket = Socket {
        limits,
        group,
        conns: HashMap::new(),
        slots: 0,
        generation: 0,
        events_tx,
        out_tx,
        budget: Arc::new(Semaphore::new(DOWNSTREAM_BUDGET)),
        batch: Vec::new(),
        batch_permits: Vec::new(),
        batch_bytes: 0,
        last_message_in: now,
        started: now,
        ended: false,
        end_reason: None,
        stats: Stats::default(),
        announces,
        next_accept: GATEWAY_CONN_MIN,
    };
    tracing::info!(event = "tunnel.session_start");

    let shutdown_requested = async move {
        if shutdown.wait_for(|stop| *stop).await.is_err() {
            // The sender is gone without asking: never.
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(shutdown_requested);
    let mut sweep = tokio::time::interval(SWEEP);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    while !socket.ended {
        tokio::select! {
            biased;
            _ = &mut shutdown_requested => {
                socket.shutdown("shutdown", close_code::SHUTDOWN, close_reason::SESSION_ENDING);
            }
            _ = sweep.tick() => socket.sweep(),
            Some(peer) = inbound_rx.recv() => socket.on_inbound(peer),
            Some(event) = events_rx.recv() => {
                socket.on_event(event);
                // Whatever else is ready rides in the same message.
                while socket.batch_bytes < MAX_BATCH_BYTES {
                    match events_rx.try_recv() {
                        Ok(event) => socket.on_event(event),
                        Err(_) => break,
                    }
                }
            }
            message = stream.next() => match message {
                Some(Ok(message)) => socket.on_ws_message(message),
                // The client went away (or the transport failed).
                None | Some(Err(_)) => socket.on_close("normal"),
            },
        }
        socket.flush();
    }
    // Let the writer send what is queued (the close included), then stop.
    drop(socket);
    let _ = tokio::time::timeout(WRITER_GRACE, writer).await;
}

impl Socket {
    fn close_reason(&self, reason: &str) -> String {
        format_session_end(
            reason,
            self.stats.messages_in,
            self.stats.bytes_up,
            self.stats.bytes_down,
        )
    }

    fn on_ws_message(&mut self, message: Message) {
        match message {
            Message::Binary(bytes) => {
                self.stats.messages_in += 1;
                self.last_message_in = Instant::now();
                match decode_message(&bytes, DecodeLimits::default()) {
                    Err(e) => {
                        tracing::info!(event = "tunnel.protocol_error", code = e.code.as_str());
                        self.abort(close_code::PROTOCOL_ERROR, "protocol_error");
                    }
                    Ok(frames) => {
                        for frame in frames {
                            if self.ended {
                                return;
                            }
                            self.on_frame(frame);
                        }
                    }
                }
            }
            Message::Text(_) => {
                self.stats.messages_in += 1;
                self.abort(close_code::UNSUPPORTED_DATA, "unsupported_data");
            }
            Message::Close(_) => {
                // Answer with the counters (the runtime may already have
                // echoed the client's close; then this is dropped).
                self.flush();
                let reason = self.close_reason("normal");
                let _ = self.out_tx.send(Out::Close(close_code::NORMAL, reason));
                self.on_close("normal");
            }
            // WebSocket-level pings are answered by the library; they are
            // not protocol messages and do not count as activity.
            Message::Ping(_) | Message::Pong(_) => {}
        }
    }

    fn on_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Open { conn, peer } => self.open(conn, peer),
            Frame::Data { conn, payload } => {
                let Some(c) = self.conns.get(&conn) else {
                    return; // may race with our close
                };
                if !c.open {
                    // Handshake gate: nothing goes to the peer before `opened`.
                    self.end_conn(conn, close_reason::PROTOCOL_ERROR, true);
                    return;
                }
                self.write(conn, payload);
            }
            Frame::Close { conn, .. } => {
                if self.conns.contains_key(&conn) {
                    self.end_conn(conn, close_reason::NORMAL, false);
                }
            }
            Frame::Ping { .. } => {}
            Frame::Announce {
                info_hash,
                reserved,
                peer_id,
                trackers,
            } => self.announce(info_hash, reserved, peer_id, trackers),
            Frame::Withdraw { info_hash } => {
                if let Some(a) = &mut self.announces {
                    a.inbound.withdraw(a.socket, &info_hash);
                    a.active.retain(|ih| *ih != info_hash);
                }
                self.push(
                    Frame::Announced {
                        info_hash,
                        status: announce_status::STOPPED,
                    },
                    None,
                );
            }
            Frame::Opened { .. }
            | Frame::Failed { .. }
            | Frame::Accept { .. }
            | Frame::Announced { .. } => {
                // Gateway → client only.
                tracing::info!(event = "tunnel.protocol_error", code = "unexpected_frame");
                self.abort(close_code::PROTOCOL_ERROR, "protocol_error");
            }
        }
    }

    fn open(&mut self, conn: u16, peer: PeerFields) {
        self.stats.opens += 1;
        if self.conns.contains_key(&conn) {
            return self.fail(conn, fail_reason::CONN_IN_USE);
        }
        if self.slots >= self.limits.max_connections {
            return self.fail(conn, fail_reason::TOO_MANY_CONNECTIONS);
        }
        if !netguard::tunnel_may_dial(&peer.addr, self.limits.allow_private) {
            return self.fail(conn, fail_reason::FORBIDDEN_ADDRESS);
        }
        let utp = match peer.transport {
            transport::TCP => None,
            transport::UTP => {
                // Only from the announced port, only while this session announces.
                let socket = self
                    .announces
                    .as_ref()
                    .and_then(|a| a.inbound.utp_for(&self.group, &peer.info_hash));
                let Some(socket) = socket else {
                    return self.fail(conn, fail_reason::UNSUPPORTED_TRANSPORT);
                };
                self.stats.utp_opens += 1;
                Some(socket)
            }
            _ => return self.fail(conn, fail_reason::UNSUPPORTED_TRANSPORT),
        };
        self.generation += 1;
        self.slots += 1;
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let pending = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(connection(Dial {
            conn,
            generation: self.generation,
            peer,
            utp,
            limits: self.limits.clone(),
            events: self.events_tx.clone(),
            write_rx,
            pending: pending.clone(),
            budget: self.budget.clone(),
        }));
        self.conns.insert(
            conn,
            Conn {
                generation: self.generation,
                open: false,
                write_tx,
                pending,
                task: task.abort_handle(),
                last_activity: Instant::now(),
            },
        );
    }

    /// Join the swarm of `info_hash` for this WebSocket (or update it).
    fn announce(
        &mut self,
        info_hash: [u8; 20],
        reserved: [u8; 8],
        peer_id: [u8; 20],
        trackers: Vec<String>,
    ) {
        self.stats.announces += 1;
        let status = match &mut self.announces {
            None => announce_status::UNSUPPORTED,
            Some(a) => {
                let member = Member {
                    socket: a.socket,
                    reserved,
                    peer_id,
                    group: self.group.clone(),
                    tx: a.tx.clone(),
                    client: a.client,
                    allow_private: self.limits.allow_private,
                };
                let status = a.inbound.announce(member, info_hash, trackers);
                if status == announce_status::ACTIVE && !a.active.contains(&info_hash) {
                    a.active.push(info_hash);
                }
                status
            }
        };
        tracing::info!(
            event = "tunnel.announce",
            status = announce_status::name(status)
        );
        self.push(Frame::Announced { info_hash, status }, None);
    }

    /// An inbound peer for an info-hash this WebSocket announces: `accept`
    /// it, and relay it from now on. Dropped (the socket closes) if the
    /// announce ended meanwhile or no id is free.
    fn on_inbound(&mut self, peer: InboundPeer) {
        if self.ended {
            return;
        }
        let announced = self
            .announces
            .as_ref()
            .is_some_and(|a| a.active.contains(&peer.info_hash));
        if !announced {
            // Not a byte was sent to it yet: drop it without a reply.
            return peer.stream.discard();
        }
        let Some(conn) = self.free_accept_id() else {
            return peer.stream.discard();
        };
        self.generation += 1;
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let pending = Arc::new(AtomicUsize::new(0));
        let InboundPeer {
            stream,
            addr,
            info_hash,
            reserved,
            peer_id,
            extra,
            ours,
            slot,
        } = peer;
        let peer_transport = stream.transport();
        let task = tokio::spawn(inbound_connection(
            Relay {
                conn,
                generation: self.generation,
                events: self.events_tx.clone(),
                write_rx,
                pending: pending.clone(),
                budget: self.budget.clone(),
            },
            stream,
            info_hash,
            ours,
            slot,
        ));
        self.conns.insert(
            conn,
            Conn {
                generation: self.generation,
                open: true,
                write_tx,
                pending,
                task: task.abort_handle(),
                last_activity: Instant::now(),
            },
        );
        self.stats.accepted += 1;
        if peer_transport == transport::UTP {
            self.stats.utp_accepted += 1;
        }
        tracing::debug!(
            event = "tunnel.accepted",
            transport = transport::name(peer_transport)
        );
        self.push(
            Frame::Accept {
                conn,
                peer: PeerFields {
                    info_hash,
                    reserved,
                    peer_id,
                    addr,
                    transport: peer_transport,
                },
            },
            None,
        );
        if !extra.is_empty() {
            self.stats.bytes_down += extra.len() as u64;
            self.push(
                Frame::Data {
                    conn,
                    payload: extra,
                },
                None,
            );
        }
    }

    fn free_accept_id(&mut self) -> Option<u16> {
        for _ in 0..=(u16::MAX - GATEWAY_CONN_MIN) {
            let id = self.next_accept;
            self.next_accept = if id == u16::MAX {
                GATEWAY_CONN_MIN
            } else {
                id + 1
            };
            if !self.conns.contains_key(&id) {
                return Some(id);
            }
        }
        None
    }

    /// Client → peer, in order. A peer that does not take its bytes is
    /// closed past the bound instead of growing the gateway's memory.
    fn write(&mut self, conn: u16, payload: Bytes) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let len = payload.len();
        if c.pending.load(Ordering::Acquire) + len > self.limits.max_pending_write {
            self.stats.overflow_closed += 1;
            self.end_conn(conn, close_reason::WRITE_OVERFLOW, true);
            return;
        }
        self.stats.bytes_up += len as u64;
        c.pending.fetch_add(len, Ordering::AcqRel);
        c.last_activity = Instant::now();
        let _ = c.write_tx.send(payload);
    }

    fn on_event(&mut self, event: Event) {
        match event {
            Event::Released => self.slots = self.slots.saturating_sub(1),
            Event::Opened {
                conn,
                generation,
                reserved,
                peer_id,
                extra,
                ms,
            } => {
                let Some(c) = self.current(conn, generation) else {
                    return;
                };
                if c.open {
                    return;
                }
                c.open = true;
                c.last_activity = Instant::now();
                self.stats.opened += 1;
                tracing::debug!(event = "tunnel.opened", ms = ms as u64);
                // The remote's peer-wire bytes that came with its handshake ride along.
                self.push(
                    Frame::Opened {
                        conn,
                        reserved,
                        peer_id,
                    },
                    None,
                );
                if !extra.is_empty() {
                    self.stats.bytes_down += extra.len() as u64;
                    self.push(
                        Frame::Data {
                            conn,
                            payload: extra,
                        },
                        None,
                    );
                }
            }
            Event::Failed {
                conn,
                generation,
                reason,
                ms,
            } => {
                let Some(c) = self.current(conn, generation) else {
                    return;
                };
                if c.open {
                    return;
                }
                self.conns.remove(&conn);
                self.stats.failed += 1;
                tracing::debug!(
                    event = "tunnel.open_failed",
                    reason = fail_reason::name(reason),
                    ms = ms as u64
                );
                self.push(Frame::Failed { conn, reason }, None);
            }
            Event::Data {
                conn,
                generation,
                payload,
                permit,
            } => {
                let Some(c) = self.current(conn, generation) else {
                    return;
                };
                if !c.open {
                    return;
                }
                c.last_activity = Instant::now();
                self.stats.bytes_down += payload.len() as u64;
                self.push(Frame::Data { conn, payload }, Some(permit));
            }
            Event::Ended {
                conn,
                generation,
                reason,
            } => {
                let open = match self.current(conn, generation) {
                    Some(c) => c.open,
                    None => return,
                };
                if open {
                    self.end_conn(conn, reason, true);
                }
            }
        }
    }

    /// The connection `conn` if it is still the one `generation` opened.
    fn current(&mut self, conn: u16, generation: u64) -> Option<&mut Conn> {
        self.conns
            .get_mut(&conn)
            .filter(|c| c.generation == generation)
    }

    /// Idle checks (connections and the WebSocket) and the session length.
    fn sweep(&mut self) {
        if self.ended {
            return;
        }
        if self.group.is_expired(self.limits.session_timeout) {
            self.group.expired.store(true, Ordering::Relaxed);
            return self.shutdown(
                "session_timeout",
                close_code::SESSION_TIMEOUT,
                close_reason::SESSION_TIMEOUT,
            );
        }
        let now = Instant::now();
        if now.duration_since(self.last_message_in) >= self.limits.idle_timeout {
            return self.shutdown("idle", close_code::IDLE, close_reason::SESSION_ENDING);
        }
        let idle: Vec<u16> = self
            .conns
            .iter()
            .filter(|(_, c)| {
                c.open && now.duration_since(c.last_activity) >= self.limits.idle_timeout
            })
            .map(|(id, _)| *id)
            .collect();
        for conn in idle {
            self.stats.idle_closed += 1;
            self.end_conn(conn, close_reason::IDLE, true);
        }
    }

    /// End one connection: forget it (its id is reusable), tell the client
    /// if `notify`, and drop its socket (the slot frees once the task is gone).
    fn end_conn(&mut self, conn: u16, reason: u8, notify: bool) {
        let Some(c) = self.conns.remove(&conn) else {
            return;
        };
        c.task.abort();
        if notify {
            self.push(Frame::Close { conn, reason }, None);
        }
        if c.open {
            tracing::debug!(event = "tunnel.closed", reason = close_reason::name(reason));
        }
    }

    fn fail(&mut self, conn: u16, reason: u8) {
        self.stats.failed += 1;
        tracing::debug!(
            event = "tunnel.open_failed",
            reason = fail_reason::name(reason)
        );
        self.push(Frame::Failed { conn, reason }, None);
    }

    fn push(&mut self, frame: Frame, permit: Option<OwnedSemaphorePermit>) {
        if self.ended {
            return;
        }
        self.batch_bytes += frame.encoded_len();
        self.batch.push(frame);
        self.batch_permits.extend(permit);
    }

    fn flush(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let frames = std::mem::take(&mut self.batch);
        let permits = std::mem::take(&mut self.batch_permits);
        self.batch_bytes = 0;
        self.stats.messages_out += 1;
        self.stats.frames_out += frames.len() as u64;
        let _ = self
            .out_tx
            .send(Out::Message(encode_frames(&frames), permits));
    }

    /// End the session from the gateway side: a `close` with `conn_reason`
    /// for every connection, then the WebSocket close with the counters.
    fn shutdown(&mut self, reason: &'static str, code: u16, conn_reason: u8) {
        if self.ended {
            return;
        }
        let mut ids: Vec<u16> = self.conns.keys().copied().collect();
        ids.sort_unstable();
        for conn in ids {
            self.push(
                Frame::Close {
                    conn,
                    reason: conn_reason,
                },
                None,
            );
        }
        self.flush();
        let _ = self
            .out_tx
            .send(Out::Close(code, self.close_reason(reason)));
        self.on_close(reason);
    }

    /// Close the WebSocket for a protocol violation (no `close` frames).
    fn abort(&mut self, code: u16, reason: &'static str) {
        if self.ended {
            return;
        }
        self.batch.clear();
        self.batch_permits.clear();
        let _ = self
            .out_tx
            .send(Out::Close(code, self.close_reason(reason)));
        self.on_close(reason);
    }

    /// The WebSocket is over: drop every connection and log the counters.
    fn on_close(&mut self, reason: &'static str) {
        if self.ended {
            return;
        }
        self.flush();
        self.ended = true;
        let reason = *self.end_reason.get_or_insert(reason);
        for (_, c) in self.conns.drain() {
            c.task.abort();
        }
        let s = &self.stats;
        tracing::info!(
            event = "tunnel.session_end",
            reason,
            ms = self.started.elapsed().as_millis() as u64,
            messages_in = s.messages_in,
            messages_out = s.messages_out,
            frames_out = s.frames_out,
            bytes_up = s.bytes_up,
            bytes_down = s.bytes_down,
            opens = s.opens,
            opened = s.opened,
            failed = s.failed,
            idle_closed = s.idle_closed,
            overflow_closed = s.overflow_closed,
            announces = s.announces,
            accepted = s.accepted,
            utp_opens = s.utp_opens,
            utp_accepted = s.utp_accepted,
        );
    }
}

/// Sends `Released` when the connection task ends, however it ends (an
/// abort drops the future, and with it the TCP socket and this guard).
struct ReleaseGuard(mpsc::UnboundedSender<Event>);

impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        let _ = self.0.send(Event::Released);
    }
}

struct Dial {
    conn: u16,
    generation: u64,
    peer: PeerFields,
    /// The socket to dial from, for a uTP `open`.
    utp: Option<UtpSocket>,
    limits: Arc<Limits>,
    events: mpsc::UnboundedSender<Event>,
    write_rx: mpsc::UnboundedReceiver<Bytes>,
    pending: Arc<AtomicUsize>,
    budget: Arc<Semaphore>,
}

/// One connection: connect, write our handshake, wait for a valid one, then
/// relay both ways until either side ends.
async fn connection(dial: Dial) {
    let Dial {
        conn,
        generation,
        peer,
        utp,
        limits,
        events,
        write_rx,
        pending,
        budget,
    } = dial;
    let _release = ReleaseGuard(events.clone());
    let started = Instant::now();
    let fail = |reason: u8| {
        let _ = events.send(Event::Failed {
            conn,
            generation,
            reason,
            ms: started.elapsed().as_millis(),
        });
    };

    let (stream, extra, reserved, peer_id) = match handshake(&peer, utp, &limits).await {
        Ok(v) => v,
        Err(reason) => return fail(reason),
    };
    let _ = events.send(Event::Opened {
        conn,
        generation,
        reserved,
        peer_id,
        extra,
        ms: started.elapsed().as_millis(),
    });

    let relay = Relay {
        conn,
        generation,
        events,
        write_rx,
        pending,
        budget,
    };
    relay.run(stream).await;
}

/// The relay half of a connection, once it passed the gate.
struct Relay {
    conn: u16,
    generation: u64,
    events: mpsc::UnboundedSender<Event>,
    write_rx: mpsc::UnboundedReceiver<Bytes>,
    pending: Arc<AtomicUsize>,
    budget: Arc<Semaphore>,
}

/// An accepted inbound peer: answer its handshake with the announcing
/// session's, then relay. `_slot` keeps its place in the inbound counts.
async fn inbound_connection(
    relay: Relay,
    mut stream: PeerStream,
    info_hash: [u8; 20],
    ours: ([u8; 8], [u8; 20]),
    _slot: super::inbound::InboundSlot,
) {
    let handshake = encode_handshake_with(&ours.0, &info_hash, &ours.1);
    if stream.write_all(&handshake).await.is_err() {
        let _ = relay.events.send(Event::Ended {
            conn: relay.conn,
            generation: relay.generation,
            reason: close_reason::REMOTE_ERROR,
        });
        return;
    }
    relay.run(stream).await;
}

impl Relay {
    /// Relay both ways until either side ends.
    async fn run(self, stream: PeerStream) {
        let Relay {
            conn,
            generation,
            events,
            mut write_rx,
            pending,
            budget,
        } = self;
        let (mut rd, mut wr) = tokio::io::split(stream);
        let reader = async {
            let mut buf = BytesMut::new();
            loop {
                buf.reserve(READ_CHUNK);
                match rd.read_buf(&mut buf).await {
                    Ok(0) => return close_reason::REMOTE_CLOSED,
                    Err(_) => return close_reason::REMOTE_ERROR,
                    Ok(n) => {
                        let payload = buf.split().freeze();
                        let Ok(permit) = budget.clone().acquire_many_owned(n as u32).await else {
                            return close_reason::INTERNAL_ERROR;
                        };
                        let data = Event::Data {
                            conn,
                            generation,
                            payload,
                            permit,
                        };
                        if events.send(data).is_err() {
                            return close_reason::SESSION_ENDING;
                        }
                    }
                }
            }
        };
        let writer = async {
            while let Some(bytes) = write_rx.recv().await {
                if wr.write_all(&bytes).await.is_err() {
                    return Some(close_reason::REMOTE_ERROR);
                }
                pending.fetch_sub(bytes.len(), Ordering::AcqRel);
            }
            None // the session dropped this connection
        };
        let reason = tokio::select! {
            r = reader => Some(r),
            w = writer => w,
        };
        if let Some(reason) = reason {
            let _ = events.send(Event::Ended {
                conn,
                generation,
                reason,
            });
        }
    }
}

/// Connect and pass the gate: the socket, the peer-wire bytes read past the
/// remote's handshake, and its reserved bytes and peer id; or the `failed`
/// reason. A uTP `open` dials through `punch.rs` (hole punching, M4-05).
async fn handshake(
    peer: &PeerFields,
    utp: Option<UtpSocket>,
    limits: &Limits,
) -> Result<Passed, u8> {
    let ours = encode_handshake_with(&peer.reserved, &peer.info_hash, &peer.peer_id);
    let Some(socket) = utp else {
        let stream =
            match tokio::time::timeout(limits.connect_timeout, TcpStream::connect(peer.addr)).await
            {
                Err(_) => return Err(fail_reason::TIMEOUT),
                Ok(Err(_)) => return Err(fail_reason::REFUSED),
                Ok(Ok(s)) => {
                    let _ = s.set_nodelay(true);
                    PeerStream::Tcp(s)
                }
            };
        return punch::exchange(stream, ours, peer.info_hash, limits.handshake_timeout).await;
    };
    punch::dial(
        socket,
        peer.addr,
        ours,
        peer.info_hash,
        limits.connect_timeout,
        limits.handshake_timeout,
    )
    .await
}
