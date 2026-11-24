//! uTP connection state machine (BEP 29), sans-IO (M4-04b).
//!
//! One [`Connection`] is one uTP stream with one remote. It does no I/O and
//! reads no clock: the caller (the UDP multiplexer, M4-04d; the simulated
//! link in the tests) drives it with
//!
//! - [`Connection::handle_packet`]: a decoded datagram already routed to
//!   this connection by `(address, connection_id)`;
//! - [`Connection::handle_timeout`]: when [`Connection::poll_timeout`] is
//!   reached;
//! - [`Connection::poll_transmit`]: called until it returns `false` after
//!   each of the above, it yields the datagrams to send;
//! - [`Connection::write`] / [`Connection::read`] / [`Connection::close`] /
//!   [`Connection::abort`] from the application side.
//!
//! States: `SynSent` (outbound, the SYN is retransmitted with a doubling
//! timeout until [`Config::connect_timeout`]) or `SynRecv` (inbound, one
//! `ST_STATE` per received SYN, never retransmitted on our own:
//! docs/decisions/utp.md › Split of M4-04) → `Connected` → `TimeWait`
//! (both FINs exchanged; duplicates of the remote's FIN are re-ACKed for
//! [`Config::time_wait`]) → `Closed`. `ST_RESET` from the remote, the
//! timeouts and [`Connection::abort`] go straight to `Closed`.
//!
//! Connection ids (BEP 29): the initiator picks `recv_id` and sends its SYN
//! with `connection_id = recv_id`, every later packet with `recv_id + 1`;
//! the acceptor receives on `syn.connection_id + 1` and sends with
//! `syn.connection_id`. Sequence numbers: `ST_DATA`, `ST_FIN` and `ST_SYN`
//! consume one; `ST_STATE` and `ST_RESET` carry the next one without
//! consuming it, so the initiator takes `ack_nr = state.seq_nr - 1` from
//! the acceptor's reply.
//!
//! Reliability: cumulative ACK plus selective ACK (BEP 29 extension 1) for
//! a bounded reorder buffer; retransmission timeout from RTT/RTT variance
//! as BEP 29 (`rto = max(rtt + 4 · rtt_var, 500 ms)`, initially 1 s,
//! doubled on every expiry, Karn's rule for samples); fast retransmit of a
//! packet after 3 duplicate ACKs or 3 later packets selectively ACKed.
//!
//! Congestion control is LEDBAT ([`super::ledbat`], M4-04c): every accepted
//! packet's `timestamp_difference_microseconds` is a delay sample, newly
//! acknowledged bytes (cumulative or selective) move the window, a fast
//! retransmit halves it and a retransmission timeout drops it to one
//! packet (not when the timeout comes from the remote's closed window).
//! Bytes in flight stay within both the congestion window and the remote's
//! advertised window, with one packet always allowed when nothing is in
//! flight (zero-window probe, retried on the retransmission timer): the
//! connection is never stalled by its own window.
//! Packets carry at most [`MAX_PAYLOAD`] bytes (fixed, no PMTU probing).
//!
//! Bounds (docs/decisions/utp.md › Split of M4-04): the send buffer
//! (unsent + unacknowledged bytes) never exceeds [`Config::send_buffer`],
//! the receive side (unread + out-of-order bytes) never exceeds
//! [`Config::recv_buffer`], and at most [`Config::max_packets_in_flight`] /
//! [`Config::max_reorder_packets`] packets are tracked, whatever the remote
//! sends. A packet whose `ack_nr` acknowledges something never sent, or
//! whose `seq_nr` is outside the receive window, is dropped without a
//! reply; a recent duplicate is re-ACKed (one `ST_STATE`, as its
//! retransmission means our ACK was lost). Nothing here logs: drops are
//! counted in [`Stats`].
//!
//! Simultaneous open (BEP 55): an outbound connect and an inbound SYN from
//! the same address are two independent connections with different ids;
//! both may complete and the caller keeps one. Choosing non-colliding ids
//! is the multiplexer's job ([`accept_recv_id`]).

use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

use super::ledbat::{self, Ledbat};
use super::packet::{
    timestamp_difference, timestamp_micros, Header, Packet, PacketType, SelectiveAck, SeqNr,
    MAX_SACK_LEN,
};

/// Largest payload of one packet (docs/decisions/utp.md, M4-04c: a
/// conservative fixed MTU, no PMTU probing).
pub const MAX_PAYLOAD: usize = 1400;
/// Hard cap on packets tracked in flight or in the reorder buffer: the
/// selective ACK bitmask covers at most `MAX_SACK_LEN * 8` packets after
/// `ack_nr + 1`.
pub const MAX_WINDOW_PACKETS: usize = MAX_SACK_LEN * 8;
/// Duplicate ACKs (or later packets selectively ACKed) that mark a packet
/// lost (BEP 29).
const LOSS_THRESHOLD: usize = 3;
/// How far behind `ack_nr` a duplicate `ST_DATA`/`ST_FIN` may be and still
/// be re-ACKed; older ones are dropped silently.
const DUPLICATE_WINDOW: u16 = 4096;

/// The receive id of the connection a SYN would open: the multiplexer
/// checks it against the ids in use before calling [`Connection::accept`].
pub const fn accept_recv_id(syn_connection_id: u16) -> u16 {
    syn_connection_id.wrapping_add(1)
}

/// Limits and timers of one connection. Out-of-range values are clamped
/// when a connection is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Payload bytes per `ST_DATA`, at most [`MAX_PAYLOAD`].
    pub max_payload: usize,
    /// Unsent plus unacknowledged bytes.
    pub send_buffer: usize,
    /// Unread plus out-of-order bytes; also the advertised window.
    pub recv_buffer: usize,
    /// Packets sent and not cumulatively acknowledged.
    pub max_packets_in_flight: usize,
    /// Span of sequence numbers after `ack_nr` accepted for reordering.
    pub max_reorder_packets: usize,
    /// LEDBAT target queueing delay (BEP 29: 100 ms).
    pub target_delay: Duration,
    /// LEDBAT: largest congestion window increase per round trip, bytes.
    pub window_gain: usize,
    /// Largest congestion window in bytes (the smallest is one packet).
    pub max_window: usize,
    /// Outbound: give up the SYN after this long. Inbound: the remote must
    /// complete the handshake within it (10 s, as TCP inbound).
    pub connect_timeout: Duration,
    /// Retransmission timeout before any RTT sample (BEP 29: 1 s).
    pub initial_rto: Duration,
    /// Lower bound of the retransmission timeout (BEP 29: 500 ms).
    pub min_rto: Duration,
    /// Upper bound of the doubled retransmission timeout.
    pub max_rto: Duration,
    /// Consecutive retransmission timeouts without progress before the
    /// connection is given up.
    pub max_timeouts: u32,
    /// Nothing received for this long closes the connection (90 s, as
    /// tunnels).
    pub idle_timeout: Duration,
    /// Nothing sent for this long sends an `ST_STATE`.
    pub keepalive: Duration,
    /// How long a closed connection keeps re-ACKing the remote's FIN.
    pub time_wait: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_payload: MAX_PAYLOAD,
            send_buffer: 512 * 1024,
            recv_buffer: 512 * 1024,
            max_packets_in_flight: 512,
            max_reorder_packets: 512,
            target_delay: ledbat::DEFAULT_TARGET,
            window_gain: ledbat::DEFAULT_GAIN,
            max_window: 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            initial_rto: Duration::from_secs(1),
            min_rto: Duration::from_millis(500),
            max_rto: Duration::from_secs(30),
            max_timeouts: 6,
            idle_timeout: Duration::from_secs(90),
            keepalive: Duration::from_secs(29),
            time_wait: Duration::from_secs(5),
        }
    }
}

impl Config {
    fn sanitized(mut self) -> Self {
        self.max_payload = self.max_payload.clamp(1, MAX_PAYLOAD);
        self.send_buffer = self.send_buffer.max(self.max_payload);
        self.recv_buffer = self.recv_buffer.clamp(self.max_payload, u32::MAX as usize);
        self.max_packets_in_flight = self.max_packets_in_flight.clamp(1, MAX_WINDOW_PACKETS);
        self.max_reorder_packets = self.max_reorder_packets.clamp(1, MAX_WINDOW_PACKETS);
        self.max_window = self.max_window.max(self.max_payload);
        self.target_delay = self.target_delay.max(Duration::from_millis(1));
        self.min_rto = self.min_rto.max(Duration::from_millis(1));
        self.max_rto = self.max_rto.max(self.min_rto);
        self.initial_rto = self.initial_rto.clamp(self.min_rto, self.max_rto);
        self.max_timeouts = self.max_timeouts.max(1);
        self.keepalive = self.keepalive.max(Duration::from_millis(1));
        self
    }
}

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Outbound, SYN sent.
    SynSent,
    /// Inbound, SYN answered, waiting for the remote's first packet.
    SynRecv,
    /// Established (possibly half-closed in either direction).
    Connected,
    /// Both FINs exchanged and acknowledged; lingering to re-ACK the
    /// remote's FIN.
    TimeWait,
    /// Finished: the multiplexer may forget the connection.
    Closed(CloseReason),
}

/// Why a connection closed. [`CloseReason::code`] is the reason code for
/// counters and logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// Both sides sent FIN and every byte was acknowledged.
    Graceful,
    /// The remote sent `ST_RESET`.
    Reset,
    /// Outbound: no answer to the SYN within the connect timeout.
    ConnectTimeout,
    /// Inbound: the remote did not complete the handshake in time.
    HandshakeTimeout,
    /// Too many retransmission timeouts in a row.
    RetransmitTimeout,
    /// Nothing received for the idle timeout.
    IdleTimeout,
    /// Closed locally by [`Connection::abort`].
    Aborted,
}

impl CloseReason {
    pub fn code(self) -> &'static str {
        match self {
            Self::Graceful => "graceful",
            Self::Reset => "reset",
            Self::ConnectTimeout => "connect_timeout",
            Self::HandshakeTimeout => "handshake_timeout",
            Self::RetransmitTimeout => "retransmit_timeout",
            Self::IdleTimeout => "idle_timeout",
            Self::Aborted => "aborted",
        }
    }
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// Counters of one connection (no addresses, no payload).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub packets_sent: u64,
    pub packets_received: u64,
    /// Packets sent again (timeout or fast retransmit).
    pub retransmits: u64,
    pub fast_retransmits: u64,
    pub timeouts: u64,
    /// Duplicates of packets already received.
    pub duplicates: u64,
    /// Wrong connection id, or a type not expected in the current state.
    pub dropped_unexpected: u64,
    /// `ack_nr` acknowledging something never sent.
    pub dropped_bad_ack: u64,
    /// `seq_nr` outside the receive window.
    pub dropped_out_of_window: u64,
    /// In-window data with no room in the receive buffer (ACKed with the
    /// current window so the sender waits).
    pub dropped_no_buffer: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Syn,
    Data,
    Fin,
}

/// A packet sent (or queued to be sent) and not cumulatively acknowledged.
#[derive(Debug)]
struct Outgoing {
    seq: SeqNr,
    kind: Kind,
    payload: Vec<u8>,
    transmissions: u32,
    sent_at: Option<Instant>,
    need_resend: bool,
    sacked: bool,
    fast_resent: bool,
}

impl Outgoing {
    fn outstanding(&self) -> bool {
        self.transmissions > 0 && !self.sacked && !self.need_resend
    }
}

#[derive(Debug)]
enum Slot {
    Data(Vec<u8>),
    Fin,
}

/// One uTP connection. See the module documentation.
#[derive(Debug)]
pub struct Connection {
    cfg: Config,
    state: State,
    epoch: Instant,
    recv_id: u16,
    send_id: u16,
    /// Next sequence number to send.
    seq_nr: SeqNr,
    /// Last sequence number received in order.
    ack_nr: SeqNr,
    /// Inbound: the remote's SYN sequence number (duplicates are re-ACKed).
    syn_seq: SeqNr,
    /// SynSent / SynRecv deadline.
    handshake_deadline: Option<Instant>,

    // Send side.
    send_buf: VecDeque<u8>,
    /// `inflight[i].seq == seq_nr - inflight.len() + i`.
    inflight: VecDeque<Outgoing>,
    inflight_payload: usize,
    close_requested: bool,
    fin_queued: bool,
    fin_acked: bool,
    peer_wnd: usize,
    srtt_us: Option<i64>,
    rttvar_us: i64,
    rto: Duration,
    rto_deadline: Option<Instant>,
    timeouts: u32,
    dup_acks: usize,
    reset_pending: bool,

    // Receive side.
    recv_buf: VecDeque<u8>,
    /// `reorder[i]` holds sequence number `ack_nr + 1 + i`; `reorder[0]`
    /// is always empty (it would have been delivered).
    reorder: VecDeque<Option<Slot>>,
    reorder_bytes: usize,
    /// Largest payload received, the room kept free for in-order data.
    largest_payload: usize,
    fin_seq: Option<SeqNr>,
    eof: bool,
    ack_pending: bool,
    last_advertised: usize,
    reply_micro: u32,
    /// The remote's latest `timestamp_difference_microseconds` (our
    /// one-way delay sample).
    their_delay_us: u32,
    cc: Ledbat,

    last_recv: Instant,
    last_sent: Instant,
    time_wait_until: Option<Instant>,
    stats: Stats,
}

impl Connection {
    fn new(cfg: Config, now: Instant, recv_id: u16, send_id: u16, state: State) -> Self {
        let cfg = cfg.sanitized();
        Self {
            state,
            epoch: now,
            recv_id,
            send_id,
            seq_nr: SeqNr(0),
            ack_nr: SeqNr(0),
            syn_seq: SeqNr(0),
            handshake_deadline: Some(now + cfg.connect_timeout),
            send_buf: VecDeque::new(),
            inflight: VecDeque::new(),
            inflight_payload: 0,
            close_requested: false,
            fin_queued: false,
            fin_acked: false,
            peer_wnd: cfg.max_payload,
            srtt_us: None,
            rttvar_us: 0,
            rto: cfg.initial_rto,
            rto_deadline: None,
            timeouts: 0,
            dup_acks: 0,
            reset_pending: false,
            recv_buf: VecDeque::new(),
            reorder: VecDeque::new(),
            reorder_bytes: 0,
            largest_payload: 0,
            fin_seq: None,
            eof: false,
            ack_pending: false,
            last_advertised: cfg.recv_buffer,
            reply_micro: 0,
            their_delay_us: 0,
            cc: Ledbat::new(ledbat::Params {
                target: cfg.target_delay,
                gain: cfg.window_gain,
                min_window: cfg.max_payload,
                max_window: cfg.max_window,
            }),
            last_recv: now,
            last_sent: now,
            time_wait_until: None,
            stats: Stats::default(),
            cfg,
        }
    }

    /// An outbound connection: receives on `recv_id`, sends its SYN with
    /// sequence number `initial_seq` on the first [`poll_transmit`].
    ///
    /// [`poll_transmit`]: Connection::poll_transmit
    pub fn connect(cfg: Config, now: Instant, recv_id: u16, initial_seq: SeqNr) -> Self {
        let mut conn = Self::new(cfg, now, recv_id, recv_id.wrapping_add(1), State::SynSent);
        conn.inflight.push_back(Outgoing {
            seq: initial_seq,
            kind: Kind::Syn,
            payload: Vec::new(),
            transmissions: 0,
            sent_at: None,
            need_resend: false,
            sacked: false,
            fast_resent: false,
        });
        conn.seq_nr = initial_seq.add(1);
        conn
    }

    /// An inbound connection for `syn` (`None` unless it is an `ST_SYN`):
    /// receives on [`accept_recv_id`]`(syn.connection_id)`, answers with
    /// one `ST_STATE` and sends its own data from `initial_seq`.
    pub fn accept(cfg: Config, now: Instant, syn: &Packet<'_>, initial_seq: SeqNr) -> Option<Self> {
        let h = &syn.header;
        if h.packet_type != PacketType::Syn {
            return None;
        }
        let mut conn = Self::new(
            cfg,
            now,
            accept_recv_id(h.connection_id),
            h.connection_id,
            State::SynRecv,
        );
        conn.seq_nr = initial_seq;
        conn.ack_nr = h.seq_nr;
        conn.syn_seq = h.seq_nr;
        conn.peer_wnd = h.wnd_size as usize;
        conn.note_timestamps(now, &syn.header);
        conn.stats.packets_received = 1;
        conn.ack_pending = true;
        Some(conn)
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn is_connected(&self) -> bool {
        self.state == State::Connected
    }

    /// The connection is over: no more packets will be sent or accepted.
    pub fn is_closed(&self) -> bool {
        matches!(self.state, State::Closed(_))
    }

    pub fn close_reason(&self) -> Option<CloseReason> {
        match self.state {
            State::Closed(reason) => Some(reason),
            _ => None,
        }
    }

    /// Connection id of the packets this connection receives.
    pub fn recv_id(&self) -> u16 {
        self.recv_id
    }

    /// Connection id of the packets this connection sends (after the SYN).
    pub fn send_id(&self) -> u16 {
        self.send_id
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Current retransmission timeout.
    pub fn rto(&self) -> Duration {
        self.rto
    }

    /// The remote's latest one-way delay sample for our packets (µs).
    pub fn their_delay_us(&self) -> u32 {
        self.their_delay_us
    }

    /// The congestion controller (window, delay estimate, counters).
    pub fn congestion(&self) -> &Ledbat {
        &self.cc
    }

    /// Payload bytes sent and not yet acknowledged (nor selectively
    /// acknowledged, nor marked for retransmission).
    pub fn bytes_in_flight(&self) -> usize {
        self.inflight
            .iter()
            .filter(|o| o.outstanding())
            .map(|o| o.payload.len())
            .sum()
    }

    /// Bytes held for sending (unsent + unacknowledged); at most
    /// [`Config::send_buffer`].
    pub fn send_buffered(&self) -> usize {
        self.send_buf.len() + self.inflight_payload
    }

    /// Bytes held on the receive side (unread + out of order); at most
    /// [`Config::recv_buffer`].
    pub fn recv_buffered(&self) -> usize {
        self.recv_buf.len() + self.reorder_bytes
    }

    /// Packets tracked in flight and in the reorder buffer.
    pub fn tracked_packets(&self) -> (usize, usize) {
        (self.inflight.len(), self.reorder.len())
    }

    /// Bytes [`Connection::read`] can return now.
    pub fn readable(&self) -> usize {
        self.recv_buf.len()
    }

    /// The remote finished sending and every byte has been read.
    pub fn is_eof(&self) -> bool {
        self.eof && self.recv_buf.is_empty()
    }

    /// Bytes [`Connection::write`] accepts now.
    pub fn writable(&self) -> usize {
        if self.close_requested
            || !matches!(
                self.state,
                State::SynSent | State::SynRecv | State::Connected
            )
        {
            return 0;
        }
        self.cfg.send_buffer.saturating_sub(self.send_buffered())
    }

    /// Queues bytes for sending; returns how many fit in the send buffer.
    /// Bytes written before the handshake completes are sent once it does.
    pub fn write(&mut self, data: &[u8]) -> usize {
        let n = data.len().min(self.writable());
        self.send_buf.extend(&data[..n]);
        n
    }

    /// Reads received bytes in order; 0 when none are ready (see
    /// [`Connection::is_eof`]).
    pub fn read(&mut self, buf: &mut [u8]) -> usize {
        let n = buf.len().min(self.recv_buf.len());
        for (dst, src) in buf.iter_mut().zip(self.recv_buf.drain(..n)) {
            *dst = src;
        }
        // Tell a sender stalled by our window that it reopened.
        let half = self.cfg.recv_buffer / 2;
        if n > 0
            && self.state == State::Connected
            && !self.eof
            && self.last_advertised < half
            && self.advertised_window() >= half
        {
            self.ack_pending = true;
        }
        n
    }

    /// Graceful close of our sending side: an `ST_FIN` follows the bytes
    /// already written (once connected, if the handshake is still under
    /// way). Reading continues until the remote's FIN. To give up a
    /// connection at once, [`Connection::abort`].
    pub fn close(&mut self) {
        match self.state {
            State::SynSent | State::SynRecv | State::Connected => self.close_requested = true,
            State::TimeWait | State::Closed(_) => {}
        }
    }

    /// Immediate close: one `ST_RESET` to an established remote (never to
    /// a remote that has not answered us), buffers dropped.
    pub fn abort(&mut self) {
        match self.state {
            State::SynRecv | State::Connected => {
                self.close_with(CloseReason::Aborted);
                self.reset_pending = true;
            }
            State::SynSent => self.close_with(CloseReason::Aborted),
            State::TimeWait | State::Closed(_) => {}
        }
    }

    fn close_with(&mut self, reason: CloseReason) {
        self.state = State::Closed(reason);
        self.send_buf = VecDeque::new();
        self.inflight = VecDeque::new();
        self.inflight_payload = 0;
        self.reorder = VecDeque::new();
        self.reorder_bytes = 0;
        if reason != CloseReason::Graceful {
            self.recv_buf = VecDeque::new();
        }
        self.rto_deadline = None;
        self.handshake_deadline = None;
        self.time_wait_until = None;
        self.ack_pending = false;
    }

    fn advertised_window(&self) -> usize {
        self.cfg.recv_buffer.saturating_sub(self.recv_buffered())
    }

    /// One packet always fits when nothing is outstanding (also the
    /// zero-window probe); otherwise the congestion window and the
    /// remote's advertised window bound the bytes in flight.
    fn window_allows(&self, size: usize) -> bool {
        let in_flight = self.bytes_in_flight();
        in_flight == 0 || in_flight + size <= self.cc.window().min(self.peer_wnd)
    }

    /// The remote has no room for our oldest packet: timeouts are its
    /// closed window, not congestion or loss.
    fn peer_window_closed(&self) -> bool {
        self.inflight
            .front()
            .is_some_and(|o| self.peer_wnd < o.payload.len().max(1))
    }

    fn front_seq(&self) -> SeqNr {
        self.seq_nr.sub(self.inflight.len() as u16)
    }

    fn note_timestamps(&mut self, now: Instant, h: &Header) {
        let now_us = timestamp_micros(self.epoch, now);
        self.reply_micro = timestamp_difference(now_us, h.timestamp_us);
        self.their_delay_us = h.timestamp_difference_us;
    }

    /// When [`Connection::handle_timeout`] must be called next.
    pub fn poll_timeout(&self) -> Option<Instant> {
        let min = |a: Option<Instant>, b: Option<Instant>| match (a, b) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        match self.state {
            State::Closed(_) => None,
            State::TimeWait => self.time_wait_until,
            State::SynSent | State::SynRecv => min(self.handshake_deadline, self.rto_deadline),
            State::Connected => min(
                self.rto_deadline,
                Some(
                    (self.last_recv + self.cfg.idle_timeout)
                        .min(self.last_sent + self.cfg.keepalive),
                ),
            ),
        }
    }

    /// Fires the timers due at `now`.
    pub fn handle_timeout(&mut self, now: Instant) {
        match self.state {
            State::Closed(_) => return,
            State::TimeWait => {
                if self.time_wait_until.is_some_and(|t| t <= now) {
                    self.close_with(CloseReason::Graceful);
                }
                return;
            }
            State::SynSent | State::SynRecv => {
                if self.handshake_deadline.is_some_and(|t| t <= now) {
                    self.close_with(if self.state == State::SynSent {
                        CloseReason::ConnectTimeout
                    } else {
                        CloseReason::HandshakeTimeout
                    });
                    return;
                }
            }
            State::Connected => {
                if now >= self.last_recv + self.cfg.idle_timeout {
                    self.close_with(CloseReason::IdleTimeout);
                    return;
                }
            }
        }

        if self.rto_deadline.is_some_and(|t| t <= now) {
            self.rto_deadline = None;
            if self
                .inflight
                .iter()
                .any(|o| o.transmissions > 0 && !o.sacked)
            {
                self.stats.timeouts += 1;
                if self.state != State::SynSent {
                    self.timeouts += 1;
                    if self.timeouts > self.cfg.max_timeouts {
                        self.close_with(CloseReason::RetransmitTimeout);
                        return;
                    }
                }
                self.rto = (self.rto * 2).min(self.cfg.max_rto);
                if self.state != State::SynSent && !self.peer_window_closed() {
                    self.cc.on_timeout(now);
                }
                for o in self.inflight.iter_mut() {
                    if o.transmissions > 0 && !o.sacked {
                        o.need_resend = true;
                    }
                }
                self.dup_acks = 0;
            }
        }

        if self.state == State::Connected && now >= self.last_sent + self.cfg.keepalive {
            self.ack_pending = true;
        }
    }

    /// Writes the next datagram to send into `out` (cleared first) and
    /// returns `true`, or returns `false` when nothing is due. Call it
    /// until it returns `false` after every input.
    pub fn poll_transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> bool {
        out.clear();
        if self.reset_pending {
            self.reset_pending = false;
            self.emit_control(now, PacketType::Reset, out);
            return true;
        }
        match self.state {
            State::Closed(_) => return false,
            State::SynSent => {
                let due = self
                    .inflight
                    .front()
                    .is_some_and(|o| o.transmissions == 0 || o.need_resend);
                if due {
                    self.transmit(now, 0, out);
                }
                return due;
            }
            State::Connected | State::TimeWait => {
                if let Some(i) = self.next_retransmit() {
                    self.transmit(now, i, out);
                    return true;
                }
                if self.state == State::Connected {
                    if let Some(i) = self.packetize() {
                        self.transmit(now, i, out);
                        return true;
                    }
                }
            }
            State::SynRecv => {}
        }
        if self.ack_pending {
            self.emit_control(now, PacketType::State, out);
            return true;
        }
        false
    }

    fn next_retransmit(&self) -> Option<usize> {
        let i = self.inflight.iter().position(|o| o.need_resend)?;
        self.window_allows(self.inflight[i].payload.len())
            .then_some(i)
    }

    fn packetize(&mut self) -> Option<usize> {
        if self.inflight.len() >= self.cfg.max_packets_in_flight {
            return None;
        }
        let (kind, payload) = if !self.send_buf.is_empty() {
            let size = self.send_buf.len().min(self.cfg.max_payload);
            if !self.window_allows(size) {
                return None;
            }
            (Kind::Data, self.send_buf.drain(..size).collect::<Vec<u8>>())
        } else if self.close_requested && !self.fin_queued {
            self.fin_queued = true;
            (Kind::Fin, Vec::new())
        } else {
            return None;
        };
        self.inflight_payload += payload.len();
        self.inflight.push_back(Outgoing {
            seq: self.seq_nr,
            kind,
            payload,
            transmissions: 0,
            sent_at: None,
            need_resend: false,
            sacked: false,
            fast_resent: false,
        });
        self.seq_nr = self.seq_nr.add(1);
        Some(self.inflight.len() - 1)
    }

    fn header(&self, now: Instant, packet_type: PacketType, seq_nr: SeqNr, wnd: usize) -> Header {
        Header {
            packet_type,
            connection_id: if packet_type == PacketType::Syn {
                self.recv_id
            } else {
                self.send_id
            },
            timestamp_us: timestamp_micros(self.epoch, now),
            timestamp_difference_us: self.reply_micro,
            wnd_size: wnd.min(u32::MAX as usize) as u32,
            seq_nr,
            ack_nr: self.ack_nr,
        }
    }

    fn transmit(&mut self, now: Instant, i: usize, out: &mut Vec<u8>) {
        let wnd = self.advertised_window();
        let o = &self.inflight[i];
        let packet_type = match o.kind {
            Kind::Syn => PacketType::Syn,
            Kind::Data => PacketType::Data,
            Kind::Fin => PacketType::Fin,
        };
        let mut header = self.header(now, packet_type, o.seq, wnd);
        if o.kind == Kind::Syn {
            header.ack_nr = SeqNr(0);
        }
        let packet = Packet {
            header,
            selective_ack: None,
            payload: &o.payload,
            skipped_extensions: 0,
        };
        // Only ST_DATA carries a payload, so encoding cannot fail.
        let _ = packet.encode(out);

        let o = &mut self.inflight[i];
        if o.transmissions > 0 {
            self.stats.retransmits += 1;
        }
        o.transmissions += 1;
        o.sent_at = Some(now);
        o.need_resend = false;
        self.after_send(now, wnd);
        if self.rto_deadline.is_none() {
            self.rto_deadline = Some(now + self.rto);
        }
        // Data carries the cumulative ACK; a selective ACK still needs an
        // ST_STATE.
        if self.reorder.is_empty() {
            self.ack_pending = false;
        }
    }

    fn emit_control(&mut self, now: Instant, packet_type: PacketType, out: &mut Vec<u8>) {
        let wnd = self.advertised_window();
        let header = self.header(now, packet_type, self.seq_nr, wnd);
        let mut mask = [0u8; MAX_SACK_LEN];
        let mut sack_len = 0;
        if packet_type == PacketType::State {
            let bits = self.reorder.len().saturating_sub(1).min(MAX_WINDOW_PACKETS);
            for i in 0..bits {
                if self.reorder[i + 1].is_some() {
                    mask[i / 8] |= 1 << (i % 8);
                }
            }
            sack_len = bits.div_ceil(32) * 4;
        }
        let packet = Packet {
            header,
            selective_ack: SelectiveAck::new(&mask[..sack_len]),
            payload: &[],
            skipped_extensions: 0,
        };
        let _ = packet.encode(out);
        self.ack_pending = false;
        self.after_send(now, wnd);
    }

    fn after_send(&mut self, now: Instant, wnd: usize) {
        self.stats.packets_sent += 1;
        self.last_sent = now;
        self.last_advertised = wnd;
    }

    /// `ack_nr` acknowledges only what was sent: at most the last sequence
    /// number used, at least a bounded distance before the oldest one
    /// still in flight (older ACKs come from reordered packets).
    fn ack_in_range(&self, ack_nr: SeqNr) -> bool {
        let last_sent = self.seq_nr.sub(1);
        usize::from(last_sent.distance_from(ack_nr))
            <= self.inflight.len() + self.cfg.max_packets_in_flight
    }

    /// Handles one datagram routed to this connection.
    pub fn handle_packet(&mut self, now: Instant, packet: &Packet<'_>) {
        let h = &packet.header;
        let expected_id = if h.packet_type == PacketType::Syn {
            self.recv_id.wrapping_sub(1)
        } else {
            self.recv_id
        };
        if h.connection_id != expected_id || self.is_closed() {
            self.stats.dropped_unexpected += 1;
            return;
        }

        if self.state == State::TimeWait {
            // Only the remote's retransmitted FIN (or data before it)
            // deserves an answer: our ACK was lost.
            if matches!(h.packet_type, PacketType::Data | PacketType::Fin)
                && self.ack_in_range(h.ack_nr)
                && self.is_recent_duplicate(h.seq_nr)
            {
                self.stats.duplicates += 1;
                self.stats.packets_received += 1;
                self.ack_pending = true;
            } else {
                self.stats.dropped_unexpected += 1;
            }
            return;
        }

        match h.packet_type {
            PacketType::Reset => {
                // A remote that has not answered our SYN yet cannot reset it,
                // and a reset must acknowledge something we sent: libutp and
                // libtorrent echo the seq_nr of the packet they reset on
                // (our ST_STATEs carry the next, unused one), and an abort
                // carries the sender's ack_nr. A blind spoofer must now guess
                // the sequence window as well as the connection id.
                if self.state == State::SynSent
                    || !(self.ack_in_range(h.ack_nr) || h.ack_nr == self.seq_nr)
                {
                    self.stats.dropped_unexpected += 1;
                } else {
                    self.stats.packets_received += 1;
                    self.close_with(CloseReason::Reset);
                }
                return;
            }
            PacketType::Syn => {
                // A retransmitted SYN: its ST_STATE was lost. One per SYN.
                if self.state == State::SynRecv && h.seq_nr == self.syn_seq {
                    self.stats.packets_received += 1;
                    self.stats.duplicates += 1;
                    self.note_timestamps(now, h);
                    self.ack_pending = true;
                } else {
                    self.stats.dropped_unexpected += 1;
                }
                return;
            }
            _ => {}
        }

        if self.state == State::SynSent {
            // The answer must acknowledge our SYN exactly.
            if h.ack_nr != self.seq_nr.sub(1) {
                self.stats.dropped_bad_ack += 1;
                return;
            }
            self.ack_nr = h.seq_nr.sub(1);
            self.state = State::Connected;
            self.handshake_deadline = None;
            // Confirm at once, so the acceptor leaves SynRecv even if we
            // have nothing to say first.
            self.ack_pending = true;
        } else if !self.ack_in_range(h.ack_nr) {
            self.stats.dropped_bad_ack += 1;
            return;
        }

        let carries_seq = matches!(h.packet_type, PacketType::Data | PacketType::Fin);
        let mut duplicate = false;
        if carries_seq {
            let next = self.ack_nr.add(1);
            if h.seq_nr.is_before(next) {
                if !self.is_recent_duplicate(h.seq_nr) {
                    self.stats.dropped_out_of_window += 1;
                    return;
                }
                duplicate = true;
            } else if usize::from(h.seq_nr.distance_from(next)) >= self.cfg.max_reorder_packets {
                self.stats.dropped_out_of_window += 1;
                return;
            }
        }

        self.stats.packets_received += 1;
        self.last_recv = now;
        self.note_timestamps(now, h);
        self.cc.on_delay_sample(now, h.timestamp_difference_us);
        if self.state == State::SynRecv {
            self.state = State::Connected;
            self.handshake_deadline = None;
        }

        let old_peer_wnd = self.peer_wnd;
        self.peer_wnd = h.wnd_size as usize;
        self.process_ack(
            now,
            h.ack_nr,
            packet.selective_ack,
            h.packet_type == PacketType::State,
        );
        self.on_window_change(old_peer_wnd);

        if duplicate {
            self.stats.duplicates += 1;
            self.ack_pending = true;
        } else {
            match h.packet_type {
                PacketType::Data => self.receive(h.seq_nr, Some(packet.payload)),
                PacketType::Fin => self.receive(h.seq_nr, None),
                _ => {}
            }
        }

        if self.fin_acked && self.eof && self.state == State::Connected {
            self.state = State::TimeWait;
            // Long enough for a retransmission of the remote's FIN.
            self.time_wait_until = Some(now + self.cfg.time_wait.max(self.rto * 2));
            self.rto_deadline = None;
        }
    }

    fn is_recent_duplicate(&self, seq: SeqNr) -> bool {
        let next = self.ack_nr.add(1);
        seq.is_before(next) && next.distance_from(seq) <= DUPLICATE_WINDOW
    }

    fn process_ack(
        &mut self,
        now: Instant,
        ack_nr: SeqNr,
        sack: Option<SelectiveAck<'_>>,
        pure_state: bool,
    ) {
        let flight_before = self.bytes_in_flight();
        // Payload bytes acknowledged for the first time (LEDBAT).
        let mut acked_bytes = 0;
        let len = self.inflight.len();
        let acked = usize::from(ack_nr.add(1).distance_from(self.front_seq()));
        let acked = if acked <= len { acked } else { 0 };
        for _ in 0..acked {
            let Some(o) = self.inflight.pop_front() else {
                break;
            };
            self.inflight_payload -= o.payload.len();
            if o.transmissions > 0 && !o.sacked {
                acked_bytes += o.payload.len();
            }
            if o.kind == Kind::Fin {
                self.fin_acked = true;
            }
            if o.transmissions == 1 && !o.sacked {
                if let Some(sent) = o.sent_at {
                    self.rtt_sample(now.saturating_duration_since(sent));
                }
            }
        }

        if acked > 0 {
            self.dup_acks = 0;
            self.timeouts = 0;
            // Progress ends the backoff (as libutp): the timeout returns
            // to the estimate, even when every sample was a retransmission.
            self.rto = self.computed_rto();
            self.rto_deadline = self
                .inflight
                .iter()
                .any(|o| o.transmissions > 0 && !o.sacked)
                .then(|| now + self.rto);
        } else if pure_state && sack.is_none() && self.inflight.iter().any(Outgoing::outstanding) {
            self.dup_acks += 1;
            if self.dup_acks == LOSS_THRESHOLD {
                if let Some(o) = self.inflight.front_mut() {
                    if o.outstanding() && !o.fast_resent {
                        o.need_resend = true;
                        o.fast_resent = true;
                        self.stats.fast_retransmits += 1;
                        if let Some(sent) = o.sent_at {
                            self.cc.on_loss(now, sent);
                        }
                    }
                }
            }
        }

        if let Some(sack) = sack {
            let front = self.front_seq();
            let mut newly = false;
            for seq in sack.acked(ack_nr) {
                let i = usize::from(seq.distance_from(front));
                let Some(o) = self.inflight.get_mut(i) else {
                    continue;
                };
                if o.transmissions > 0 && !o.sacked {
                    o.sacked = true;
                    o.need_resend = false;
                    newly = true;
                    acked_bytes += o.payload.len();
                    if o.transmissions == 1 {
                        if let Some(sent) = o.sent_at {
                            let sample = now.saturating_duration_since(sent);
                            self.rtt_sample(sample);
                        }
                    }
                }
            }
            if newly {
                self.timeouts = 0;
            }
        }
        // Growth first: a loss in the same ACK then halves the result.
        self.cc.on_ack(acked_bytes, flight_before);

        if sack.is_some() {
            // A packet with LOSS_THRESHOLD later packets selectively
            // acknowledged is lost.
            let mut later = self.inflight.iter().filter(|o| o.sacked).count();
            for o in self.inflight.iter_mut() {
                if later < LOSS_THRESHOLD {
                    break;
                }
                if o.sacked {
                    later -= 1;
                } else if o.outstanding() && !o.fast_resent {
                    o.need_resend = true;
                    o.fast_resent = true;
                    self.stats.fast_retransmits += 1;
                    if let Some(sent) = o.sent_at {
                        self.cc.on_loss(now, sent);
                    }
                }
            }
        }

        // A remote with no room for our oldest packet is alive, not lost:
        // its zero window does not count towards giving up.
        if let Some(o) = self.inflight.front() {
            if self.peer_wnd < o.payload.len().max(1) {
                self.timeouts = 0;
            }
        }
    }

    /// The remote reopened a window too small for our oldest packet: what
    /// it dropped meanwhile is sent again now rather than on the timer.
    fn on_window_change(&mut self, old_peer_wnd: usize) {
        let Some(front) = self.inflight.front() else {
            return;
        };
        let size = front.payload.len().max(1);
        if old_peer_wnd < size && self.peer_wnd >= size && front.outstanding() {
            for o in self.inflight.iter_mut() {
                if o.outstanding() {
                    o.need_resend = true;
                }
            }
            self.rto = self.computed_rto();
        }
    }

    fn rtt_sample(&mut self, sample: Duration) {
        let sample = i64::try_from(sample.as_micros()).unwrap_or(i64::MAX / 2);
        match self.srtt_us {
            None => {
                self.srtt_us = Some(sample);
                self.rttvar_us = sample / 2;
            }
            Some(srtt) => {
                let delta = (srtt - sample).abs();
                self.rttvar_us += (delta - self.rttvar_us) / 4;
                self.srtt_us = Some(srtt + (sample - srtt) / 8);
            }
        }
        self.rto = self.computed_rto();
    }

    fn computed_rto(&self) -> Duration {
        match self.srtt_us {
            None => self.cfg.initial_rto,
            Some(srtt) => {
                let us = (srtt + 4 * self.rttvar_us).max(0) as u64;
                Duration::from_micros(us).clamp(self.cfg.min_rto, self.cfg.max_rto)
            }
        }
    }

    /// An in-window `ST_DATA` (`Some(payload)`) or `ST_FIN` (`None`).
    fn receive(&mut self, seq: SeqNr, payload: Option<&[u8]>) {
        self.ack_pending = true;
        if self.eof {
            return;
        }
        if let Some(fin) = self.fin_seq {
            // Nothing after the FIN, and only one FIN.
            let bad = match payload {
                None => seq != fin,
                Some(_) => !seq.is_before(fin),
            };
            if bad {
                self.stats.dropped_out_of_window += 1;
                return;
            }
        }
        let next = self.ack_nr.add(1);
        let i = usize::from(seq.distance_from(next));
        if self.reorder.get(i).is_some_and(Option::is_some) {
            self.stats.duplicates += 1;
            return;
        }
        let len = payload.map_or(0, <[u8]>::len);
        self.largest_payload = self.largest_payload.max(len);
        // Out-of-order data never takes the room of the packet that fills
        // the hole: otherwise, with nothing left to read, the reorder
        // buffer could block the stream for good.
        let reserve = if i > 0 && len > 0 {
            self.largest_payload.max(self.cfg.max_payload)
        } else {
            0
        };
        if self.recv_buffered() + len + reserve > self.cfg.recv_buffer {
            self.stats.dropped_no_buffer += 1;
            return;
        }

        if payload.is_none() {
            self.fin_seq = Some(seq);
            // Drop anything buffered beyond the FIN.
            while self.reorder.len() > i + 1 {
                if let Some(Some(Slot::Data(d))) = self.reorder.pop_back() {
                    self.reorder_bytes -= d.len();
                }
            }
        }

        if i == 0 {
            self.reorder.pop_front();
            self.deliver(payload);
            while let Some(Some(_)) = self.reorder.front() {
                match self.reorder.pop_front() {
                    Some(Some(Slot::Data(d))) => {
                        self.reorder_bytes -= d.len();
                        self.deliver(Some(&d));
                    }
                    Some(Some(Slot::Fin)) => self.deliver(None),
                    _ => break,
                }
            }
        } else {
            if self.reorder.len() <= i {
                self.reorder.resize_with(i + 1, || None);
            }
            self.reorder[i] = Some(match payload {
                Some(p) => {
                    self.reorder_bytes += p.len();
                    Slot::Data(p.to_vec())
                }
                None => Slot::Fin,
            });
        }
    }

    fn deliver(&mut self, payload: Option<&[u8]>) {
        self.ack_nr = self.ack_nr.add(1);
        match payload {
            Some(p) => self.recv_buf.extend(p),
            None => {
                self.eof = true;
                self.reorder.clear();
                self.reorder_bytes = 0;
            }
        }
    }
}

#[cfg(test)]
#[path = "conn_tests.rs"]
mod tests;
