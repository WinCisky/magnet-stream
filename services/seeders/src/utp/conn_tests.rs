//! Tests of the uTP connection state machine with a simulated clock and an
//! in-memory link (loss, delay jitter → reordering, duplication).

use super::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

const A: usize = 0;
const B: usize = 1;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[derive(Clone, Copy)]
struct LinkCfg {
    loss: f64,
    dup: f64,
    delay: Duration,
    jitter: Duration,
    /// Bottleneck rate in bytes per second, each direction (0: none).
    rate: u64,
    /// Bytes the bottleneck queue holds before dropping (tail drop).
    queue: usize,
}

const CLEAN: LinkCfg = LinkCfg {
    loss: 0.0,
    dup: 0.0,
    delay: Duration::from_millis(20),
    jitter: Duration::ZERO,
    rate: 0,
    queue: 0,
};

/// 10 % loss, 5 % duplication, 20–60 ms one-way delay (heavy reordering).
const LOSSY: LinkCfg = LinkCfg {
    loss: 0.10,
    dup: 0.05,
    delay: Duration::from_millis(20),
    jitter: Duration::from_millis(40),
    rate: 0,
    queue: 0,
};

/// One side of the link: its connections, routed like the multiplexer
/// (M4-04d) will: SYNs by [`accept_recv_id`], the rest by connection id.
struct Endpoint {
    cfg: Config,
    conns: Vec<Connection>,
    accept: bool,
    next_seq: u16,
}

impl Endpoint {
    fn new(cfg: Config, accept: bool, next_seq: u16) -> Self {
        Self {
            cfg,
            conns: Vec::new(),
            accept,
            next_seq,
        }
    }

    fn route(&mut self, now: Instant, bytes: &[u8]) {
        let packet = Packet::decode(bytes).expect("the state machine sends valid packets");
        let h = packet.header;
        let id = if h.packet_type == PacketType::Syn {
            accept_recv_id(h.connection_id)
        } else {
            h.connection_id
        };
        if let Some(conn) = self
            .conns
            .iter_mut()
            .find(|c| c.recv_id() == id && !c.is_closed())
        {
            conn.handle_packet(now, &packet);
        } else if h.packet_type == PacketType::Syn && self.accept {
            if self.conns.iter().any(|c| c.recv_id() == id) {
                return; // that id is still in use
            }
            let conn = Connection::accept(self.cfg.clone(), now, &packet, SeqNr(self.next_seq))
                .expect("a SYN");
            self.next_seq = self.next_seq.wrapping_add(7919);
            self.conns.push(conn);
        }
        // Anything else: no connection, no reply.
    }
}

type Filter = Box<dyn FnMut(usize, &Packet<'_>) -> bool>;
/// A datagram in flight: arrival time, tie-breaker, destination side, bytes.
type InFlight = Reverse<(Instant, u64, usize, Vec<u8>)>;

struct Sim {
    start: Instant,
    now: Instant,
    link: LinkCfg,
    rng: StdRng,
    queue: BinaryHeap<InFlight>,
    counter: u64,
    eps: [Endpoint; 2],
    /// Datagrams sent by each side, with their send time.
    log: Vec<(Duration, usize, Vec<u8>)>,
    keep_log: bool,
    /// Returns `true` to drop a datagram sent by `side`.
    filter: Option<Filter>,
    /// When each direction's bottleneck finishes its queue (`rate > 0`).
    busy_until: [Instant; 2],
    /// Datagrams dropped by a full bottleneck queue, per sending side.
    queue_drops: [u64; 2],
    /// Queueing delay seen by the latest datagram, per sending side.
    queue_delay: [Duration; 2],
}

impl Sim {
    fn new(link: LinkCfg, seed: u64, a: Endpoint, b: Endpoint) -> Self {
        let start = Instant::now();
        Self {
            start,
            now: start,
            link,
            rng: StdRng::seed_from_u64(seed),
            queue: BinaryHeap::new(),
            counter: 0,
            eps: [a, b],
            log: Vec::new(),
            keep_log: true,
            filter: None,
            busy_until: [start; 2],
            queue_drops: [0; 2],
            queue_delay: [Duration::ZERO; 2],
        }
    }

    /// A connects to B (which accepts).
    fn pair(link: LinkCfg, seed: u64, cfg: Config, a_seq: u16, b_seq: u16) -> Self {
        let mut sim = Self::new(
            link,
            seed,
            Endpoint::new(cfg.clone(), false, 0),
            Endpoint::new(cfg.clone(), true, b_seq),
        );
        let conn = Connection::connect(cfg, sim.now, 0x1234, SeqNr(a_seq));
        sim.eps[A].conns.push(conn);
        sim
    }

    fn elapsed(&self) -> Duration {
        self.now - self.start
    }

    fn conn(&mut self, side: usize, i: usize) -> &mut Connection {
        &mut self.eps[side].conns[i]
    }

    fn send(&mut self, from: usize, bytes: Vec<u8>) {
        if self.keep_log {
            self.log.push((self.elapsed(), from, bytes.clone()));
        }
        if let Some(filter) = self.filter.as_mut() {
            if filter(from, &Packet::decode(&bytes).unwrap()) {
                return;
            }
        }
        if self.rng.gen_bool(self.link.loss) {
            return;
        }
        if self.link.rate > 0 {
            // A bottleneck: FIFO queue drained at `rate`, tail drop.
            let start = self.busy_until[from].max(self.now);
            let backlog = start - self.now;
            let queued = backlog.as_secs_f64() * self.link.rate as f64;
            if queued + bytes.len() as f64 > self.link.queue as f64 {
                self.queue_drops[from] += 1;
                return;
            }
            self.queue_delay[from] = backlog;
            let tx = Duration::from_secs_f64(bytes.len() as f64 / self.link.rate as f64);
            self.busy_until[from] = start + tx;
            let at = self.busy_until[from] + self.link.delay;
            self.counter += 1;
            self.queue
                .push(Reverse((at, self.counter, 1 - from, bytes)));
            return;
        }
        let copies = if self.rng.gen_bool(self.link.dup) {
            2
        } else {
            1
        };
        for _ in 0..copies {
            let jitter = self.link.jitter.as_micros() as u64;
            let extra = if jitter > 0 {
                self.rng.gen_range(0..=jitter)
            } else {
                0
            };
            let at = self.now + self.link.delay + Duration::from_micros(extra);
            self.counter += 1;
            self.queue
                .push(Reverse((at, self.counter, 1 - from, bytes.clone())));
        }
    }

    fn flush(&mut self) {
        let mut out = Vec::new();
        let mut pending = Vec::new();
        for side in [A, B] {
            for conn in self.eps[side].conns.iter_mut() {
                while conn.poll_transmit(self.now, &mut out) {
                    pending.push((side, out.clone()));
                }
            }
        }
        for (side, bytes) in pending {
            self.send(side, bytes);
        }
    }

    fn next_event(&self) -> Option<Instant> {
        let timers = self
            .eps
            .iter()
            .flat_map(|e| e.conns.iter())
            .filter_map(Connection::poll_timeout);
        let arrivals = self.queue.peek().map(|Reverse((t, ..))| *t);
        timers.chain(arrivals).min()
    }

    /// Sends what is due, then moves the clock to the next event and
    /// processes it. `false` when nothing is left to happen.
    fn step(&mut self) -> bool {
        self.flush();
        let Some(next) = self.next_event() else {
            return false;
        };
        self.now = self.now.max(next);
        while let Some(Reverse((t, ..))) = self.queue.peek() {
            if *t > self.now {
                break;
            }
            let Reverse((_, _, to, bytes)) = self.queue.pop().unwrap();
            self.eps[to].route(self.now, &bytes);
        }
        let now = self.now;
        for side in [A, B] {
            for conn in self.eps[side].conns.iter_mut() {
                if conn.poll_timeout().is_some_and(|t| t <= now) {
                    conn.handle_timeout(now);
                }
            }
        }
        self.flush();
        true
    }

    /// Steps until `f` (run before every step) returns true, or `limit`
    /// of simulated time passes, or nothing is left to happen.
    fn run(&mut self, limit: Duration, mut f: impl FnMut(&mut Sim) -> bool) -> bool {
        loop {
            if f(self) {
                return true;
            }
            if self.elapsed() > limit || !self.step() {
                return f(self);
            }
        }
    }

    fn wait(&mut self, limit: Duration, done: impl Fn(&Sim) -> bool) -> bool {
        self.run(limit, |s| done(s))
    }

    /// The datagrams `side` sent, decoded.
    fn sent_by(&self, side: usize) -> Vec<(Duration, Header, usize)> {
        self.log
            .iter()
            .filter(|(_, s, _)| *s == side)
            .map(|(t, _, b)| (*t, Packet::decode(b).unwrap().header, b.len()))
            .collect()
    }
}

/// A byte stream fed into a connection and read out of the other end.
struct Stream {
    src: Vec<u8>,
    written: usize,
    got: Vec<u8>,
    closed: bool,
}

impl Stream {
    fn new(len: usize, seed: u64) -> Self {
        let mut src = vec![0u8; len];
        StdRng::seed_from_u64(seed).fill(&mut src[..]);
        Self {
            src,
            written: 0,
            got: Vec::with_capacity(len),
            closed: false,
        }
    }

    fn pump_write(&mut self, conn: &mut Connection, close: bool) {
        if self.written < self.src.len() {
            self.written += conn.write(&self.src[self.written..]);
        }
        if close && self.written == self.src.len() && !self.closed {
            conn.close();
            self.closed = true;
        }
    }

    fn pump_read(&mut self, conn: &mut Connection) {
        let mut buf = [0u8; 16 * 1024];
        loop {
            let n = conn.read(&mut buf);
            if n == 0 {
                break;
            }
            self.got.extend_from_slice(&buf[..n]);
        }
    }

    fn complete(&self) -> bool {
        self.got.len() == self.src.len()
    }

    fn assert_identical(&self) {
        assert_eq!(self.got.len(), self.src.len());
        assert!(self.got == self.src, "stream corrupted");
    }
}

fn established(link: LinkCfg, seed: u64, cfg: Config) -> Sim {
    let mut sim = Sim::pair(link, seed, cfg, 100, 5000);
    assert!(sim.wait(secs(30), |s| s.eps[B]
        .conns
        .first()
        .is_some_and(Connection::is_connected)
        && s.eps[A].conns[0].is_connected()));
    sim
}

#[test]
fn handshake_ids_and_sequence_numbers() {
    let mut sim = established(CLEAN, 1, Config::default());
    let (a, b) = (&sim.eps[A].conns[0], &sim.eps[B].conns[0]);
    assert_eq!((a.recv_id(), a.send_id()), (0x1234, 0x1235));
    assert_eq!((b.recv_id(), b.send_id()), (0x1235, 0x1234));

    let a_sent = sim.sent_by(A);
    let b_sent = sim.sent_by(B);
    let syn = a_sent[0].1;
    assert_eq!(syn.packet_type, PacketType::Syn);
    assert_eq!(syn.connection_id, 0x1234);
    assert_eq!(syn.seq_nr, SeqNr(100));
    assert_eq!(syn.wnd_size as usize, Config::default().recv_buffer);
    // The acceptor's single ST_STATE: same id as the SYN, ACKs it, carries
    // its next sequence number.
    assert_eq!(b_sent.len(), 1);
    let state = b_sent[0].1;
    assert_eq!(state.packet_type, PacketType::State);
    assert_eq!(state.connection_id, 0x1234);
    assert_eq!(state.ack_nr, SeqNr(100));
    assert_eq!(state.seq_nr, SeqNr(5000));
    assert!(b_sent[0].2 <= a_sent[0].2);
    // The initiator confirms on the send id; the acceptor is then connected.
    let confirm = a_sent[1].1;
    assert_eq!(confirm.packet_type, PacketType::State);
    assert_eq!(confirm.connection_id, 0x1235);
    assert_eq!(confirm.ack_nr, SeqNr(4999));
    assert_eq!(confirm.seq_nr, SeqNr(101));

    // First data each way uses the next numbers.
    sim.conn(A, 0).write(b"hello");
    sim.conn(B, 0).write(b"world");
    sim.step();
    let a_data = sim
        .sent_by(A)
        .into_iter()
        .find(|p| p.1.packet_type == PacketType::Data);
    let b_data = sim
        .sent_by(B)
        .into_iter()
        .find(|p| p.1.packet_type == PacketType::Data);
    assert_eq!(a_data.unwrap().1.seq_nr, SeqNr(101));
    assert_eq!(b_data.unwrap().1.seq_nr, SeqNr(5000));
    // The RTT sample of the handshake replaced the initial 1 s timeout.
    assert_eq!(sim.conn(A, 0).rto(), Config::default().min_rto);
}

/// The acceptance criterion: 16 MiB each way over 10 % loss, reordering
/// and duplication arrives byte-identical. Sequence numbers start near the
/// wrap so both directions cross 65535 → 0.
#[test]
fn lossy_transfer_16mib_both_directions() {
    const LEN: usize = 16 * 1024 * 1024;
    let mut sim = Sim::pair(LOSSY, 0x5554_5002, Config::default(), 65_000, 64_000);
    sim.keep_log = false;
    let mut up = Stream::new(LEN, 1);
    let mut down = Stream::new(LEN, 2);
    let ok = sim.run(secs(3600), |s| {
        let a = &mut s.eps[A].conns[0];
        up.pump_write(a, true);
        down.pump_read(a);
        if let Some(b) = s.eps[B].conns.first_mut() {
            down.pump_write(b, true);
            up.pump_read(b);
        }
        s.eps[A].conns[0].is_closed() && s.eps[B].conns.first().is_some_and(Connection::is_closed)
    });
    assert!(ok, "not finished after {:?}", sim.elapsed());
    up.assert_identical();
    down.assert_identical();
    for side in [A, B] {
        let c = &sim.eps[side].conns[0];
        assert_eq!(c.close_reason(), Some(CloseReason::Graceful), "side {side}");
        assert!(c.is_eof());
        let st = c.stats();
        assert!(st.retransmits > 0 && st.fast_retransmits > 0 && st.duplicates > 0);
    }
}

#[test]
fn lossy_transfer_small_buffers_and_windows() {
    // Tight limits exercise the window, in-flight and reorder bounds.
    let cfg = Config {
        send_buffer: 8 * 1024,
        recv_buffer: 6 * 1024,
        max_packets_in_flight: 8,
        max_reorder_packets: 6,
        max_window: 4 * 1024,
        max_payload: 1000,
        ..Config::default()
    };
    let mut sim = Sim::pair(LOSSY, 77, cfg.clone(), 1, 65_530);
    sim.keep_log = false;
    let mut up = Stream::new(1024 * 1024, 3);
    let mut down = Stream::new(512 * 1024 + 17, 4);
    let ok = sim.run(secs(3600), |s| {
        for side in [A, B] {
            for c in &s.eps[side].conns {
                assert!(c.send_buffered() <= cfg.send_buffer);
                assert!(c.recv_buffered() <= cfg.recv_buffer);
                let (inflight, reorder) = c.tracked_packets();
                assert!(inflight <= cfg.max_packets_in_flight);
                assert!(reorder <= cfg.max_reorder_packets);
            }
        }
        let a = &mut s.eps[A].conns[0];
        up.pump_write(a, true);
        down.pump_read(a);
        if let Some(b) = s.eps[B].conns.first_mut() {
            down.pump_write(b, true);
            up.pump_read(b);
        }
        s.eps[A].conns[0].is_closed() && s.eps[B].conns.first().is_some_and(Connection::is_closed)
    });
    assert!(ok);
    up.assert_identical();
    down.assert_identical();
}

#[test]
fn lost_syn_is_retransmitted_with_backoff() {
    let mut sim = Sim::pair(CLEAN, 2, Config::default(), 10, 20);
    let mut syns = 0;
    sim.filter = Some(Box::new(move |_, p| {
        if p.header.packet_type == PacketType::Syn {
            syns += 1;
            return syns <= 2;
        }
        false
    }));
    assert!(sim.wait(secs(30), |s| s.eps[A].conns[0].is_connected()));
    let syn_times: Vec<Duration> = sim
        .sent_by(A)
        .into_iter()
        .filter(|p| p.1.packet_type == PacketType::Syn)
        .map(|p| p.0)
        .collect();
    // 1 s initial timeout, doubled: sent at 0, 1 s, 3 s; same id and seq.
    assert_eq!(syn_times, vec![Duration::ZERO, secs(1), secs(3)]);
    assert!(sim
        .sent_by(A)
        .iter()
        .filter(|p| p.1.packet_type == PacketType::Syn)
        .all(|p| p.1.seq_nr == SeqNr(10) && p.1.connection_id == 0x1234));
    // B answered only the SYN that reached it.
    assert_eq!(sim.sent_by(B).len(), 1);
    assert_eq!(sim.conn(A, 0).stats().retransmits, 2);
}

#[test]
fn connect_fails_within_the_configured_timeout() {
    for timeout in [secs(2), secs(10)] {
        let cfg = Config {
            connect_timeout: timeout,
            ..Config::default()
        };
        // B does not accept: unsolicited SYNs get no reply at all.
        let mut sim = Sim::new(
            CLEAN,
            3,
            Endpoint::new(cfg.clone(), false, 0),
            Endpoint::new(cfg.clone(), false, 0),
        );
        let conn = Connection::connect(cfg, sim.now, 7, SeqNr(1));
        sim.eps[A].conns.push(conn);
        sim.conn(A, 0).write(b"queued before the handshake");
        assert!(sim.wait(secs(60), |s| s.eps[A].conns[0].is_closed()));
        assert_eq!(
            sim.conn(A, 0).close_reason(),
            Some(CloseReason::ConnectTimeout)
        );
        assert_eq!(sim.elapsed(), timeout);
        assert!(sim.sent_by(B).is_empty());
        // Only SYNs went out, never data or a reset.
        assert!(sim
            .sent_by(A)
            .iter()
            .all(|p| p.1.packet_type == PacketType::Syn));
        assert!(sim.conn(A, 0).poll_timeout().is_none());
    }
}

fn syn_bytes(cid: u16, seq: u16) -> Vec<u8> {
    Packet::new(Header {
        packet_type: PacketType::Syn,
        connection_id: cid,
        timestamp_us: 1,
        timestamp_difference_us: 0,
        wnd_size: 65_536,
        seq_nr: SeqNr(seq),
        ack_nr: SeqNr(0),
    })
    .encode_to_vec()
    .unwrap()
}

fn drain(conn: &mut Connection, now: Instant) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut sent = Vec::new();
    while conn.poll_transmit(now, &mut out) {
        sent.push(out.clone());
    }
    sent
}

#[test]
fn acceptor_sends_at_most_one_state_per_syn() {
    let t0 = Instant::now();
    let syn = syn_bytes(40, 9);
    let mut conn = Connection::accept(
        Config::default(),
        t0,
        &Packet::decode(&syn).unwrap(),
        SeqNr(3),
    )
    .unwrap();
    let first = drain(&mut conn, t0);
    assert_eq!(first.len(), 1);
    assert!(first[0].len() <= syn.len());
    assert_eq!(drain(&mut conn, t0).len(), 0);

    // Each retransmitted SYN draws one ST_STATE; a burst of three handled
    // before polling draws one.
    for _ in 0..3 {
        conn.handle_packet(t0 + ms(10), &Packet::decode(&syn).unwrap());
        let sent = drain(&mut conn, t0 + ms(10));
        assert_eq!(sent.len(), 1);
        assert!(sent[0].len() <= syn.len());
    }
    for _ in 0..3 {
        conn.handle_packet(t0 + ms(20), &Packet::decode(&syn).unwrap());
    }
    assert_eq!(drain(&mut conn, t0 + ms(20)).len(), 1);
    // A SYN with another sequence number is not ours to answer.
    conn.handle_packet(t0 + ms(30), &Packet::decode(&syn_bytes(40, 10)).unwrap());
    assert!(drain(&mut conn, t0 + ms(30)).is_empty());

    // Never retransmitted on its own: silence until the handshake timeout.
    let mut now = t0 + ms(30);
    while let Some(t) = conn.poll_timeout() {
        now = t;
        conn.handle_timeout(now);
        assert!(drain(&mut conn, now).is_empty());
    }
    assert_eq!(conn.close_reason(), Some(CloseReason::HandshakeTimeout));
    assert_eq!(now - t0, Config::default().connect_timeout);
    assert!(Connection::accept(
        Config::default(),
        t0,
        &Packet::decode(&syn_bytes(1, 1)[..])
            .map(|mut p| {
                p.header.packet_type = PacketType::State;
                p
            })
            .unwrap(),
        SeqNr(0)
    )
    .is_none());
}

fn packet_bytes(t: PacketType, cid: u16, seq: u16, ack: u16, payload: &[u8]) -> Vec<u8> {
    let mut p = Packet::new(Header {
        packet_type: t,
        connection_id: cid,
        timestamp_us: 0,
        timestamp_difference_us: 0,
        wnd_size: 1 << 20,
        seq_nr: SeqNr(seq),
        ack_nr: SeqNr(ack),
    });
    p.payload = payload;
    p.encode_to_vec().unwrap()
}

#[test]
fn invalid_packets_are_dropped_without_reply() {
    let t0 = Instant::now();
    // B accepts a SYN (id 40 → B receives on 41, its next seq is 1000) and
    // the initiator confirms: B is connected, ack_nr = 9.
    let mut b = Connection::accept(
        Config::default(),
        t0,
        &Packet::decode(&syn_bytes(40, 9)).unwrap(),
        SeqNr(1000),
    )
    .unwrap();
    drain(&mut b, t0);
    let feed = |b: &mut Connection, bytes: Vec<u8>| {
        b.handle_packet(t0 + ms(5), &Packet::decode(&bytes).unwrap());
        drain(b, t0 + ms(5))
    };
    assert_eq!(
        feed(&mut b, packet_bytes(PacketType::State, 41, 10, 999, &[])).len(),
        0
    );
    assert!(b.is_connected());

    let before = b.stats();
    // ack_nr acknowledging data never sent (B's last seq is 999).
    assert!(feed(&mut b, packet_bytes(PacketType::Data, 41, 10, 1000, b"x")).is_empty());
    assert!(feed(&mut b, packet_bytes(PacketType::Data, 41, 10, 30_000, b"x")).is_empty());
    // seq_nr far ahead of the receive window, or half the ring away.
    assert!(feed(
        &mut b,
        packet_bytes(PacketType::Data, 41, 10 + 600, 999, b"x")
    )
    .is_empty());
    assert!(feed(
        &mut b,
        packet_bytes(PacketType::Data, 41, 10 + 0x8000, 999, b"x")
    )
    .is_empty());
    // Far in the past (beyond the duplicate window).
    assert!(feed(
        &mut b,
        packet_bytes(PacketType::Data, 41, 9u16.wrapping_sub(5000), 999, b"x")
    )
    .is_empty());
    // Wrong connection ids (the send id, a stranger's), a SYN once
    // connected, a reset under another id.
    assert!(feed(&mut b, packet_bytes(PacketType::Data, 40, 10, 999, b"x")).is_empty());
    assert!(feed(&mut b, packet_bytes(PacketType::Data, 7, 10, 999, b"x")).is_empty());
    assert!(feed(&mut b, syn_bytes(40, 9)).is_empty());
    assert!(feed(&mut b, packet_bytes(PacketType::Reset, 7, 0, 0, &[])).is_empty());
    let after = b.stats();
    assert_eq!(after.dropped_bad_ack - before.dropped_bad_ack, 2);
    assert_eq!(
        after.dropped_out_of_window - before.dropped_out_of_window,
        3
    );
    assert_eq!(after.dropped_unexpected - before.dropped_unexpected, 4);
    assert_eq!(b.readable(), 0);
    assert!(b.is_connected());

    // In order: delivered and ACKed once. Its duplicate: re-ACKed once.
    assert_eq!(
        feed(&mut b, packet_bytes(PacketType::Data, 41, 10, 999, b"ok")).len(),
        1
    );
    assert_eq!(
        feed(&mut b, packet_bytes(PacketType::Data, 41, 10, 999, b"ok")).len(),
        1
    );
    assert_eq!(b.readable(), 2);
    assert_eq!(b.stats().duplicates, 1);
}

#[test]
fn out_of_order_data_is_selectively_acked() {
    let t0 = Instant::now();
    let mut b = Connection::accept(
        Config::default(),
        t0,
        &Packet::decode(&syn_bytes(40, 9)).unwrap(),
        SeqNr(1000),
    )
    .unwrap();
    drain(&mut b, t0);
    // seq 11 and 13 before 10.
    for seq in [11u16, 13] {
        b.handle_packet(
            t0,
            &Packet::decode(&packet_bytes(PacketType::Data, 41, seq, 999, &[seq as u8])).unwrap(),
        );
    }
    let sent = drain(&mut b, t0);
    assert_eq!(sent.len(), 1);
    let ack = Packet::decode(&sent[0]).unwrap();
    assert_eq!(ack.header.packet_type, PacketType::State);
    assert_eq!(ack.header.ack_nr, SeqNr(9));
    // Bits for 11 (ack + 2) and 13 (ack + 4).
    let acked: Vec<u16> = ack
        .selective_ack
        .unwrap()
        .acked(SeqNr(9))
        .map(SeqNr::get)
        .collect();
    assert_eq!(acked, vec![11, 13]);
    assert_eq!(b.readable(), 0);
    b.handle_packet(
        t0,
        &Packet::decode(&packet_bytes(PacketType::Data, 41, 10, 999, &[10])).unwrap(),
    );
    let ack = drain(&mut b, t0);
    let ack = Packet::decode(&ack[0]).unwrap();
    assert_eq!(ack.header.ack_nr, SeqNr(11));
    assert_eq!(
        ack.selective_ack
            .unwrap()
            .acked(SeqNr(11))
            .map(SeqNr::get)
            .collect::<Vec<_>>(),
        vec![13]
    );
    let mut buf = [0u8; 8];
    assert_eq!(b.read(&mut buf), 2);
    assert_eq!(&buf[..2], &[10, 11]);
}

#[test]
fn graceful_close_and_half_close() {
    let mut sim = established(CLEAN, 4, Config::default());
    // A finishes sending first; B keeps sending to A afterwards.
    let mut up = Stream::new(100_000, 5);
    let mut down = Stream::new(300_000, 6);
    assert!(sim.run(secs(60), |s| {
        up.pump_write(&mut s.eps[A].conns[0], true);
        up.pump_read(&mut s.eps[B].conns[0]);
        s.eps[B].conns[0].is_eof()
    }));
    up.assert_identical();
    // Half-closed: A can no longer write but still reads.
    assert_eq!(sim.conn(A, 0).write(b"late"), 0);
    assert!(sim.conn(A, 0).is_connected());
    assert!(sim.run(secs(120), |s| {
        down.pump_write(&mut s.eps[B].conns[0], true);
        down.pump_read(&mut s.eps[A].conns[0]);
        s.eps[A].conns[0].is_closed() && s.eps[B].conns[0].is_closed()
    }));
    down.assert_identical();
    for side in [A, B] {
        assert_eq!(
            sim.conn(side, 0).close_reason(),
            Some(CloseReason::Graceful)
        );
        assert!(sim.conn(side, 0).is_eof());
        // Exactly one FIN each on a clean link.
        let fins = sim
            .sent_by(side)
            .iter()
            .filter(|p| p.1.packet_type == PacketType::Fin)
            .count();
        assert_eq!(fins, 1);
    }
}

#[test]
fn time_wait_re_acks_a_retransmitted_fin() {
    let mut sim = established(CLEAN, 8, Config::default());
    sim.conn(A, 0).close();
    sim.conn(B, 0).close();
    // Drop B's ACK of A's FIN (B's first ST_STATE from now on).
    let mut dropped = false;
    sim.filter = Some(Box::new(move |side, p| {
        let drop = side == B && p.header.packet_type == PacketType::State && !dropped;
        dropped |= drop;
        drop
    }));
    assert!(sim.wait(secs(60), |s| s.eps[A].conns[0].is_closed()
        && s.eps[B].conns[0].is_closed()));
    assert_eq!(sim.conn(A, 0).close_reason(), Some(CloseReason::Graceful));
    assert_eq!(sim.conn(B, 0).close_reason(), Some(CloseReason::Graceful));
    let a_fins = sim
        .sent_by(A)
        .iter()
        .filter(|p| p.1.packet_type == PacketType::Fin)
        .count();
    assert_eq!(a_fins, 2);
}

#[test]
fn reset_and_abort() {
    let mut sim = established(CLEAN, 9, Config::default());
    sim.conn(B, 0).write(b"unread");
    sim.step();
    sim.step();
    assert_eq!(sim.conn(A, 0).readable(), 6);
    sim.conn(A, 0).abort();
    assert_eq!(sim.conn(A, 0).close_reason(), Some(CloseReason::Aborted));
    assert_eq!(sim.conn(A, 0).readable(), 0);
    assert!(sim.wait(secs(5), |s| s.eps[B].conns[0].is_closed()));
    assert_eq!(sim.conn(B, 0).close_reason(), Some(CloseReason::Reset));
    let resets: Vec<_> = sim
        .sent_by(A)
        .into_iter()
        .filter(|p| p.1.packet_type == PacketType::Reset)
        .collect();
    assert_eq!(resets.len(), 1);
    assert_eq!(resets[0].1.connection_id, 0x1235);
    // Both closed: nothing more is sent.
    let sent = sim.log.len();
    sim.wait(secs(10), |_| false);
    assert_eq!(sim.log.len(), sent);

    // A pending connect is neither reset by a stranger's ST_RESET nor
    // sends one when aborted.
    let t0 = Instant::now();
    let mut c = Connection::connect(Config::default(), t0, 50, SeqNr(1));
    drain(&mut c, t0);
    let reset = packet_bytes(PacketType::Reset, 50, 0, 1, &[]);
    c.handle_packet(t0, &Packet::decode(&reset).unwrap());
    assert_eq!(c.state(), State::SynSent);
    c.abort();
    assert!(drain(&mut c, t0).is_empty());
    assert_eq!(c.close_reason(), Some(CloseReason::Aborted));
}

#[test]
fn a_reset_must_acknowledge_something_we_sent() {
    let t0 = Instant::now();
    let connected = || {
        // As in invalid_packets_are_dropped_without_reply: B receives on 41,
        // its last seq is 999, its next 1000.
        let mut b = Connection::accept(
            Config::default(),
            t0,
            &Packet::decode(&syn_bytes(40, 9)).unwrap(),
            SeqNr(1000),
        )
        .unwrap();
        drain(&mut b, t0);
        let state = packet_bytes(PacketType::State, 41, 10, 999, &[]);
        b.handle_packet(t0, &Packet::decode(&state).unwrap());
        assert!(b.is_connected());
        b
    };
    let reset = |b: &mut Connection, ack: u16| {
        let bytes = packet_bytes(PacketType::Reset, 41, 31_337, ack, &[]);
        b.handle_packet(t0 + ms(5), &Packet::decode(&bytes).unwrap());
        drain(b, t0 + ms(5))
    };
    // A blind guess of the sequence window: ignored, without a reply.
    for ack in [5_000u16, 30_000, 999u16.wrapping_sub(20_000), 1001] {
        let mut b = connected();
        assert!(reset(&mut b, ack).is_empty());
        assert!(b.is_connected(), "reset with ack {ack} taken");
    }
    // The echo of our last packet (libutp, libtorrent), of our ST_STATE
    // (the next seq), or an abort's ack_nr: taken.
    for ack in [999u16, 1000] {
        let mut b = connected();
        assert!(reset(&mut b, ack).is_empty());
        assert_eq!(b.close_reason(), Some(CloseReason::Reset));
    }
}

#[test]
fn idle_and_retransmit_timeouts() {
    let cfg = Config {
        idle_timeout: secs(5),
        keepalive: secs(2),
        ..Config::default()
    };
    // Idle: the link dies with nothing to send; keepalives go unanswered.
    let mut sim = established(CLEAN, 10, cfg);
    let t = sim.elapsed();
    sim.link.loss = 1.0;
    assert!(sim.wait(secs(30), |s| s.eps[A].conns[0].is_closed()));
    assert_eq!(
        sim.conn(A, 0).close_reason(),
        Some(CloseReason::IdleTimeout)
    );
    assert!(sim.elapsed() - t <= secs(5));
    let keepalives = sim
        .sent_by(A)
        .iter()
        .filter(|p| p.0 > t && p.1.packet_type == PacketType::State)
        .count();
    assert_eq!(keepalives, 2);

    // Retransmit: data outstanding on a dead link, idle timeout far away.
    let cfg = Config {
        idle_timeout: secs(3600),
        max_timeouts: 3,
        ..Config::default()
    };
    let mut sim = established(CLEAN, 11, cfg);
    sim.link.loss = 1.0;
    sim.conn(A, 0).write(&[1; 5000]);
    assert!(sim.wait(secs(600), |s| s.eps[A].conns[0].is_closed()));
    assert_eq!(
        sim.conn(A, 0).close_reason(),
        Some(CloseReason::RetransmitTimeout)
    );
    assert_eq!(sim.conn(A, 0).stats().timeouts, 4);
}

/// A peer that never reads: memory stays within the configured limits on
/// both sides and the connection survives (zero-window probes), then
/// resumes intact once the peer reads.
#[test]
fn memory_stays_bounded_with_a_peer_that_does_not_read() {
    let cfg = Config {
        send_buffer: 64 * 1024,
        recv_buffer: 48 * 1024,
        ..Config::default()
    };
    let mut sim = established(LOSSY, 12, cfg.clone());
    sim.keep_log = false;
    let mut up = Stream::new(1024 * 1024, 13);
    let start = sim.elapsed();
    let mut max_send = 0;
    let mut max_recv = 0;
    // B does not read for five simulated minutes.
    sim.run(secs(10_000), |s| {
        up.pump_write(&mut s.eps[A].conns[0], true);
        max_send = max_send.max(s.eps[A].conns[0].send_buffered());
        max_recv = max_recv.max(s.eps[B].conns[0].recv_buffered());
        s.elapsed() - start > secs(300)
    });
    assert!(max_send <= cfg.send_buffer);
    assert_eq!(max_send, cfg.send_buffer);
    assert!(max_recv <= cfg.recv_buffer);
    assert!(sim.conn(B, 0).readable() >= cfg.recv_buffer / 2);
    assert!(sim.conn(A, 0).is_connected() && sim.conn(B, 0).is_connected());
    // B reads again: the rest arrives.
    assert!(sim.run(secs(10_000), |s| {
        up.pump_write(&mut s.eps[A].conns[0], true);
        up.pump_read(&mut s.eps[B].conns[0]);
        assert!(s.eps[A].conns[0].send_buffered() <= cfg.send_buffer);
        assert!(s.eps[B].conns[0].recv_buffered() <= cfg.recv_buffer);
        up.complete()
    }));
    up.assert_identical();
}

fn simultaneous_sim(link: LinkCfg, seed: u64, cfg: &Config, a_id: u16, b_id: u16) -> Sim {
    let mut sim = Sim::new(
        link,
        seed,
        Endpoint::new(cfg.clone(), true, 30_000),
        Endpoint::new(cfg.clone(), true, 40_000),
    );
    let a = Connection::connect(cfg.clone(), sim.now, a_id, SeqNr(1));
    let b = Connection::connect(cfg.clone(), sim.now, b_id, SeqNr(65_535));
    sim.eps[A].conns.push(a);
    sim.eps[B].conns.push(b);
    sim
}

/// BEP 55: both sides connect to each other at once; each SYN opens its
/// own connection, both complete, the four streams stay separate and
/// intact. The first SYN from A dies (as at a NAT without a mapping yet).
#[test]
fn simultaneous_open_both_complete() {
    for (seed, link) in [(20u64, CLEAN), (21, LOSSY), (22, LOSSY)] {
        let mut sim = simultaneous_sim(link, seed, &Config::default(), 100, 200);
        let mut first = true;
        sim.filter = Some(Box::new(move |side, p| {
            let drop = side == A && p.header.packet_type == PacketType::Syn && first;
            first &= !drop;
            drop
        }));
        let mut streams: Vec<Stream> = (0..4)
            .map(|i| Stream::new(200_000 + i * 1111, 50 + i as u64))
            .collect();
        // (writer side, writer conn, reader side, reader conn): conn 0 is
        // each side's outbound, conn 1 the one its peer's SYN opened.
        let routes = [(A, 0, B, 1), (A, 1, B, 0), (B, 0, A, 1), (B, 1, A, 0)];
        let ok = sim.run(secs(600), |s| {
            for (st, &(ws, wc, rs, rc)) in streams.iter_mut().zip(&routes) {
                if s.eps[ws].conns.len() > wc {
                    st.pump_write(&mut s.eps[ws].conns[wc], false);
                }
                if s.eps[rs].conns.len() > rc {
                    st.pump_read(&mut s.eps[rs].conns[rc]);
                }
            }
            streams.iter().all(Stream::complete)
        });
        assert!(ok, "seed {seed}");
        for st in &streams {
            st.assert_identical();
        }
        let ids = |side: usize| {
            sim.eps[side]
                .conns
                .iter()
                .map(Connection::recv_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(A), vec![100, 201]);
        assert_eq!(ids(B), vec![200, 101]);
        assert!(sim
            .eps
            .iter()
            .all(|e| e.conns.iter().all(Connection::is_connected)));
    }
}

/// The caller keeps one of the two connections and aborts the other: the
/// kept stream is unaffected.
#[test]
fn simultaneous_open_caller_keeps_one() {
    let mut sim = simultaneous_sim(LOSSY, 23, &Config::default(), 100, 200);
    sim.keep_log = false;
    let mut kept = Stream::new(1024 * 1024, 60);
    let mut aborted = false;
    assert!(sim.run(secs(600), |s| {
        // Keep A's outbound (A 0 ↔ B 1), drop A's inbound once it exists.
        if !aborted && s.eps[A].conns.len() == 2 {
            s.eps[A].conns[1].abort();
            aborted = true;
        }
        kept.pump_write(&mut s.eps[A].conns[0], true);
        if s.eps[B].conns.len() == 2 {
            kept.pump_read(&mut s.eps[B].conns[1]);
            return kept.complete() && s.eps[B].conns[1].is_eof();
        }
        false
    }));
    kept.assert_identical();
    assert_eq!(sim.conn(A, 1).close_reason(), Some(CloseReason::Aborted));
    // B's outbound ends: reset, or a timeout if the reset was lost or came
    // before A's answer to its SYN.
    assert!(sim.wait(secs(1200), |s| s.eps[B].conns[0].is_closed()));
    assert!(sim.conn(B, 0).close_reason() != Some(CloseReason::Graceful));
    assert_eq!(sim.eps[A].conns.len(), 2);
}

/// When the remote's SYN would open a connection whose receive id is
/// already ours, it is not accepted (it reaches our outbound connection,
/// which drops it): our connect still completes.
#[test]
fn simultaneous_open_with_colliding_ids() {
    let cfg = Config {
        connect_timeout: secs(5),
        ..Config::default()
    };
    // B's SYN id 99 → inbound id 100 at A, which A's outbound uses.
    let mut sim = simultaneous_sim(CLEAN, 24, &cfg, 100, 99);
    assert!(sim.wait(secs(30), |s| s.eps[B].conns[0].is_closed()));
    assert_eq!(sim.eps[A].conns.len(), 1);
    assert!(sim.conn(A, 0).is_connected());
    assert!(sim.conn(A, 0).stats().dropped_unexpected > 0);
    assert_eq!(
        sim.conn(B, 0).close_reason(),
        Some(CloseReason::ConnectTimeout)
    );
    assert!(sim.conn(B, 1).is_connected());
    // A data stream on the surviving pair is intact.
    let mut st = Stream::new(100_000, 61);
    assert!(sim.run(secs(120), |s| {
        st.pump_write(&mut s.eps[A].conns[0], false);
        st.pump_read(&mut s.eps[B].conns[1]);
        st.complete()
    }));
    st.assert_identical();
}

#[test]
fn config_is_clamped() {
    let cfg = Config {
        max_payload: 0,
        send_buffer: 0,
        recv_buffer: 0,
        max_packets_in_flight: 1_000_000,
        max_reorder_packets: 0,
        max_window: 0,
        min_rto: Duration::ZERO,
        max_rto: Duration::ZERO,
        initial_rto: secs(100),
        max_timeouts: 0,
        keepalive: Duration::ZERO,
        ..Config::default()
    }
    .sanitized();
    assert_eq!(cfg.max_payload, 1);
    assert_eq!(cfg.send_buffer, 1);
    assert_eq!(cfg.recv_buffer, 1);
    assert_eq!(cfg.max_packets_in_flight, MAX_WINDOW_PACKETS);
    assert_eq!(cfg.max_reorder_packets, 1);
    assert!(cfg.min_rto > Duration::ZERO && cfg.max_rto >= cfg.min_rto);
    assert_eq!(cfg.initial_rto, cfg.max_rto);
    let big = Config {
        max_payload: 100_000,
        ..Config::default()
    }
    .sanitized();
    assert_eq!(big.max_payload, MAX_PAYLOAD);
}

/// Random datagrams thrown at an established connection never panic and
/// never break the limits.
#[test]
fn property_random_packets_never_panic() {
    let cfg = Config {
        recv_buffer: 16 * 1024,
        max_reorder_packets: 64,
        ..Config::default()
    };
    let mut rng = StdRng::seed_from_u64(0xbe90_0055);
    let t0 = Instant::now();
    let mut b = Connection::accept(
        cfg.clone(),
        t0,
        &Packet::decode(&syn_bytes(40, 9)).unwrap(),
        SeqNr(1000),
    )
    .unwrap();
    b.write(&[7; 50_000]);
    let mut payload = [0u8; 1400];
    for i in 0..50_000u32 {
        let now = t0 + ms(u64::from(i));
        let t = PacketType::from_u8(rng.gen_range(0..=4)).unwrap();
        let len = if t == PacketType::Data {
            rng.gen_range(0..=1400)
        } else {
            0
        };
        rng.fill(&mut payload[..len]);
        let bytes = packet_bytes(
            t,
            if rng.gen_bool(0.9) { 41 } else { rng.gen() },
            if rng.gen_bool(0.8) {
                10u16.wrapping_add(rng.gen_range(0..80))
            } else {
                rng.gen()
            },
            if rng.gen_bool(0.8) {
                999u16.wrapping_add(rng.gen_range(0..40))
            } else {
                rng.gen()
            },
            &payload[..len],
        );
        let mut bytes = bytes;
        if rng.gen_bool(0.3) {
            bytes[12..16].copy_from_slice(&rng.gen_range(0u32..20_000).to_be_bytes());
        }
        if t == PacketType::Reset {
            continue;
        }
        b.handle_packet(now, &Packet::decode(&bytes).unwrap());
        if b.poll_timeout().is_some_and(|d| d <= now) {
            b.handle_timeout(now);
        }
        drain(&mut b, now);
        assert!(b.recv_buffered() <= cfg.recv_buffer);
        assert!(b.send_buffered() <= cfg.send_buffer);
        let (inflight, reorder) = b.tracked_packets();
        assert!(inflight <= cfg.max_packets_in_flight && reorder <= cfg.max_reorder_packets);
        if rng.gen_bool(0.1) {
            let mut buf = [0u8; 4096];
            b.read(&mut buf);
        }
    }
}

#[path = "ledbat_sim_tests.rs"]
mod ledbat_sim;
