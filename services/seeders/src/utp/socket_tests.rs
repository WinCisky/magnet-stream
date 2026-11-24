//! Integration tests of the uTP multiplexer on loopback: a lossy UDP relay
//! (loss, duplication, reordering) for echo and bulk transfers both ways,
//! raw UDP sockets for the no-reply rules and the limits.

use super::*;
use crate::utp::packet::Header;
use crate::utp::testutil::{relay, LOSSY};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

const LOCAL: &str = "127.0.0.1:0";

fn local() -> SocketAddr {
    LOCAL.parse().unwrap()
}

fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..len).map(|_| rng.gen()).collect()
}

async fn bind(cfg: SocketConfig) -> UtpSocket {
    UtpSocket::bind(local(), cfg).await.unwrap()
}

#[tokio::test]
async fn echo_through_a_lossy_relay() {
    let server = bind(SocketConfig::default()).await;
    let acceptor = server.listen(None).unwrap();
    let via = relay(server.local_addr(), LOSSY, 1).await;
    let echo = tokio::spawn(async move {
        let mut s = acceptor.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        loop {
            let n = s.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            s.write_all(&buf[..n]).await.unwrap();
        }
        s.shutdown().await.unwrap();
    });

    let client = bind(SocketConfig::default()).await;
    let mut s = timeout(Duration::from_secs(10), client.connect(via))
        .await
        .unwrap()
        .unwrap();
    let mut rng = StdRng::seed_from_u64(2);
    for i in 0..30 {
        let msg = pattern(rng.gen_range(1..20_000), i);
        s.write_all(&msg).await.unwrap();
        let mut back = vec![0u8; msg.len()];
        timeout(Duration::from_secs(20), s.read_exact(&mut back))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(back, msg, "message {i}");
    }
    s.shutdown().await.unwrap();
    let mut rest = Vec::new();
    timeout(Duration::from_secs(20), s.read_to_end(&mut rest))
        .await
        .unwrap()
        .unwrap();
    assert!(rest.is_empty());
    timeout(Duration::from_secs(20), echo)
        .await
        .unwrap()
        .unwrap();
}

/// Sends `out` and reads everything the other side sends, at once.
async fn exchange(s: UtpStream, out: Vec<u8>) -> Vec<u8> {
    let (mut r, mut w) = tokio::io::split(s);
    let writer = tokio::spawn(async move {
        for chunk in out.chunks(10_000) {
            w.write_all(chunk).await.unwrap();
        }
        w.shutdown().await.unwrap();
        w
    });
    let mut got = Vec::new();
    r.read_to_end(&mut got).await.unwrap();
    let w = writer.await.unwrap();
    // Keep the stream until both directions are done.
    drop(r.unsplit(w));
    got
}

#[tokio::test]
async fn bulk_both_ways_through_a_lossy_relay() {
    const SIZE: usize = 2 * 1024 * 1024;
    let server = bind(SocketConfig::default()).await;
    let acceptor = server.listen(None).unwrap();
    let via = relay(server.local_addr(), LOSSY, 3).await;
    let up = pattern(SIZE, 10);
    let down = pattern(SIZE + 777, 11);

    let down2 = down.clone();
    let srv = tokio::spawn(async move {
        let s = acceptor.accept().await.unwrap();
        exchange(s, down2).await
    });
    let client = bind(SocketConfig::default()).await;
    let s = client.connect(via).await.unwrap();
    let got_down = timeout(Duration::from_secs(60), exchange(s, up.clone()))
        .await
        .unwrap();
    let got_up = timeout(Duration::from_secs(60), srv)
        .await
        .unwrap()
        .unwrap();
    assert!(got_down == down, "server → client bytes differ");
    assert!(got_up == up, "client → server bytes differ");
    let stats = client.stats();
    assert_eq!(stats.outbound_connected, 1);
    assert_eq!(server.stats().inbound_connected, 1);
}

#[tokio::test]
async fn outbound_and_inbound_connections_share_the_local_port() {
    let a = bind(SocketConfig::default()).await;
    let b = bind(SocketConfig::default()).await;
    let c = bind(SocketConfig::default()).await;
    let a_in = a.listen(None).unwrap();
    let c_in = c.listen(None).unwrap();

    // B → A (inbound for A) and A → C (outbound from A), at the same time.
    let (from_b, to_c) = tokio::join!(b.connect(a.local_addr()), a.connect(c.local_addr()));
    let (mut from_b, mut to_c) = (from_b.unwrap(), to_c.unwrap());
    let mut at_a = a_in.accept().await.unwrap();
    let mut at_c = c_in.accept().await.unwrap();

    assert_eq!(from_b.peer_addr(), a.local_addr());
    assert_eq!(at_a.peer_addr(), b.local_addr());
    // C sees A's outbound connection come from A's (listening) port.
    assert_eq!(at_c.peer_addr(), a.local_addr());
    assert_eq!(to_c.local_addr(), a.local_addr());

    from_b.write_all(b"b to a").await.unwrap();
    to_c.write_all(b"a to c").await.unwrap();
    let mut buf = [0u8; 6];
    at_a.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"b to a");
    at_c.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"a to c");
    at_c.write_all(b"c to a").await.unwrap();
    to_c.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"c to a");
}

fn header(packet_type: PacketType, connection_id: u16, seq: u16, ack: u16) -> Header {
    Header {
        packet_type,
        connection_id,
        timestamp_us: 1,
        timestamp_difference_us: 0,
        wnd_size: 65_536,
        seq_nr: SeqNr(seq),
        ack_nr: SeqNr(ack),
    }
}

fn raw_packet(packet_type: PacketType, connection_id: u16, seq: u16, ack: u16) -> Vec<u8> {
    let mut p = Packet::new(header(packet_type, connection_id, seq, ack));
    let payload = [7u8; 32];
    if packet_type == PacketType::Data {
        p.payload = &payload;
    }
    p.encode_to_vec().unwrap()
}

async fn raw() -> UdpSocket {
    UdpSocket::bind(local()).await.unwrap()
}

/// Datagrams received within `wait`.
async fn received(sock: &UdpSocket, wait: Duration) -> Vec<Vec<u8>> {
    let mut got = Vec::new();
    let mut buf = vec![0u8; MAX_DATAGRAM];
    let end = tokio::time::Instant::now() + wait;
    while let Ok(Ok((n, _))) = tokio::time::timeout_at(end, sock.recv_from(&mut buf)).await {
        got.push(buf[..n].to_vec());
    }
    got
}

#[tokio::test]
async fn unsolicited_packets_get_no_reply() {
    let server = bind(SocketConfig::default()).await;
    let to = server.local_addr();
    let probe = raw().await;
    let strangers = [
        raw_packet(PacketType::Syn, 100, 1, 0),
        raw_packet(PacketType::State, 101, 1, 0),
        raw_packet(PacketType::Data, 102, 1, 0),
        raw_packet(PacketType::Fin, 103, 1, 0),
        raw_packet(PacketType::Reset, 104, 1, 0),
        vec![0x41; 20],           // bad version
        vec![0x01, 0x00],         // too short
        b"d1:ad2:id20:".to_vec(), // a DHT query on the wrong port
    ];
    for p in &strangers {
        probe.send_to(p, to).await.unwrap();
    }
    assert!(received(&probe, Duration::from_millis(500))
        .await
        .is_empty());
    let st = server.stats();
    assert_eq!(st.dropped_syn_no_gate, 1);
    assert_eq!(st.dropped_unknown, 4);
    assert_eq!(st.dropped_malformed, 3);
    assert_eq!(st.datagrams_sent, 0);

    // With the listener open, only a SYN opens a connection: the rest of
    // the strangers still get nothing.
    let _gate = server.listen(None).unwrap();
    for p in &strangers[1..] {
        probe.send_to(p, to).await.unwrap();
    }
    assert!(received(&probe, Duration::from_millis(500))
        .await
        .is_empty());
    assert_eq!(server.stats().datagrams_sent, 0);
    assert_eq!(server.connection_counts(), (0, 0));

    // A filter that refuses the address keeps the gate shut for it.
    drop(_gate);
    let _gate = server.listen(Some(Arc::new(|_| false))).unwrap();
    probe
        .send_to(&raw_packet(PacketType::Syn, 200, 1, 0), to)
        .await
        .unwrap();
    assert!(received(&probe, Duration::from_millis(300))
        .await
        .is_empty());
    assert_eq!(server.stats().dropped_syn_no_gate, 2);
}

#[tokio::test]
async fn a_syn_draws_at_most_one_state() {
    let server = bind(SocketConfig::default()).await;
    let _gate = server.listen(None).unwrap();
    let probe = raw().await;
    let syn = raw_packet(PacketType::Syn, 500, 42, 0);
    probe.send_to(&syn, server.local_addr()).await.unwrap();
    // Longer than the initial retransmission timeout: nothing is resent.
    let got = received(&probe, Duration::from_millis(1500)).await;
    assert_eq!(got.len(), 1, "one reply");
    let reply = Packet::decode(&got[0]).unwrap();
    assert_eq!(reply.header.packet_type, PacketType::State);
    assert_eq!(reply.header.connection_id, 500);
    assert_eq!(reply.header.ack_nr, SeqNr(42));
    assert_eq!(got[0].len(), syn.len(), "no amplification");

    // A retransmitted SYN: again exactly one.
    probe.send_to(&syn, server.local_addr()).await.unwrap();
    assert_eq!(received(&probe, Duration::from_millis(1200)).await.len(), 1);
    assert_eq!(server.connection_counts(), (1, 1));
}

#[tokio::test]
async fn syns_beyond_the_limits_are_ignored() {
    // Half-open per IP (every probe is 127.0.0.1).
    let cfg = SocketConfig {
        max_half_open_per_ip: 2,
        ..SocketConfig::default()
    };
    let server = bind(cfg).await;
    let _gate = server.listen(None).unwrap();
    let probe = raw().await;
    for id in [10u16, 20, 30, 40] {
        probe
            .send_to(&raw_packet(PacketType::Syn, id, 1, 0), server.local_addr())
            .await
            .unwrap();
    }
    assert_eq!(received(&probe, Duration::from_millis(400)).await.len(), 2);
    assert_eq!(server.stats().dropped_syn_limit, 2);
    assert_eq!(server.connection_counts(), (2, 2));

    // Half-open in total, from several ports.
    let cfg = SocketConfig {
        max_half_open: 3,
        max_half_open_per_ip: 100,
        max_connections_per_ip: 100,
        ..SocketConfig::default()
    };
    let server = bind(cfg).await;
    let _gate = server.listen(None).unwrap();
    let mut replies = 0;
    let mut probes = Vec::new();
    for _ in 0..5 {
        let p = raw().await;
        p.send_to(&raw_packet(PacketType::Syn, 7, 1, 0), server.local_addr())
            .await
            .unwrap();
        probes.push(p);
    }
    for p in &probes {
        replies += received(p, Duration::from_millis(300)).await.len();
    }
    assert_eq!(replies, 3);
    assert_eq!(server.connection_counts(), (3, 3));

    // The acceptor's backlog.
    let cfg = SocketConfig {
        accept_backlog: 1,
        ..SocketConfig::default()
    };
    let server = bind(cfg).await;
    let _gate = server.listen(None).unwrap();
    for id in [1u16, 2] {
        probe
            .send_to(&raw_packet(PacketType::Syn, id, 1, 0), server.local_addr())
            .await
            .unwrap();
    }
    assert_eq!(received(&probe, Duration::from_millis(300)).await.len(), 1);

    // An expired handshake frees its slot.
    let cfg = SocketConfig {
        max_half_open: 1,
        conn: conn::Config {
            connect_timeout: Duration::from_millis(300),
            ..conn::Config::default()
        },
        ..SocketConfig::default()
    };
    let server = bind(cfg).await;
    let _gate = server.listen(None).unwrap();
    let to = server.local_addr();
    probe
        .send_to(&raw_packet(PacketType::Syn, 1, 1, 0), to)
        .await
        .unwrap();
    probe
        .send_to(&raw_packet(PacketType::Syn, 3, 1, 0), to)
        .await
        .unwrap();
    assert_eq!(received(&probe, Duration::from_millis(200)).await.len(), 1);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(server.connection_counts(), (0, 0));
    assert_eq!(server.stats().half_open_dropped, 1);
    probe
        .send_to(&raw_packet(PacketType::Syn, 3, 1, 0), to)
        .await
        .unwrap();
    assert_eq!(received(&probe, Duration::from_millis(200)).await.len(), 1);
}

#[tokio::test]
async fn connection_limits_bound_connects() {
    let cfg = SocketConfig {
        max_connections_per_ip: 1,
        ..SocketConfig::default()
    };
    let client = bind(cfg).await;
    let silent = raw().await;
    let to = silent.local_addr().unwrap();
    let first = tokio::spawn({
        let client = client.clone();
        async move { client.connect(to).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let err = client.connect(to).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    first.abort();
    let _ = first.await;
    // The abandoned connect leaves nothing behind.
    assert_eq!(client.connection_counts(), (0, 0));
}

#[tokio::test]
async fn an_expected_address_opens_the_gate_for_it_only() {
    let server = bind(SocketConfig::default()).await;
    let expected = raw().await;
    let other = raw().await;
    let acceptor = server.expect(expected.local_addr().unwrap()).unwrap();
    assert!(server.expect(expected.local_addr().unwrap()).is_err());

    let to = server.local_addr();
    other
        .send_to(&raw_packet(PacketType::Syn, 9, 1, 0), to)
        .await
        .unwrap();
    expected
        .send_to(&raw_packet(PacketType::Syn, 9, 1, 0), to)
        .await
        .unwrap();
    assert!(received(&other, Duration::from_millis(300))
        .await
        .is_empty());
    let got = received(&expected, Duration::from_millis(300)).await;
    assert_eq!(got.len(), 1);
    let state = Packet::decode(&got[0]).unwrap().header;

    // Completing the handshake hands the connection to the acceptor.
    let mut data = Packet::new(header(
        PacketType::Data,
        10,
        2,
        state.seq_nr.get().wrapping_sub(1),
    ));
    data.payload = b"hello";
    expected
        .send_to(&data.encode_to_vec().unwrap(), to)
        .await
        .unwrap();
    let mut s = timeout(Duration::from_secs(2), acceptor.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.peer_addr(), expected.local_addr().unwrap());
    let mut buf = [0u8; 5];
    s.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    // Closing the gate: the address is a stranger again.
    drop(s);
    drop(acceptor);
    expected
        .send_to(&raw_packet(PacketType::Syn, 30, 1, 0), to)
        .await
        .unwrap();
    // (The dropped stream's own FIN may still arrive: it is no answer.)
    let answers = received(&expected, Duration::from_millis(300)).await;
    assert!(answers
        .iter()
        .all(|d| Packet::decode(d).unwrap().header.connection_id != 30));
}

#[tokio::test]
async fn a_pending_connect_accepts_the_remotes_syn() {
    // The remote's first SYN is lost at its NAT; ours never gets an
    // answer; its next SYN reaches us while our connect is pending, and the
    // connection it opens completes the connect (BEP 55).
    let client = bind(SocketConfig::default()).await;
    let remote = raw().await;
    let to_remote = remote.local_addr().unwrap();
    let connect = tokio::spawn({
        let client = client.clone();
        async move { client.connect(to_remote).await }
    });
    let mut buf = vec![0u8; MAX_DATAGRAM];
    let (n, from) = remote.recv_from(&mut buf).await.unwrap();
    assert_eq!(
        Packet::decode(&buf[..n]).unwrap().header.packet_type,
        PacketType::Syn
    );
    assert_eq!(from, client.local_addr());

    remote
        .send_to(&raw_packet(PacketType::Syn, 4000, 77, 0), from)
        .await
        .unwrap();
    let state = loop {
        let (n, _) = remote.recv_from(&mut buf).await.unwrap();
        let p = Packet::decode(&buf[..n]).unwrap();
        if p.header.packet_type == PacketType::State {
            break p.header;
        }
    };
    assert_eq!(state.connection_id, 4000);
    let mut data = Packet::new(header(
        PacketType::Data,
        4001,
        78,
        state.seq_nr.get().wrapping_sub(1),
    ));
    data.payload = b"punched";
    remote
        .send_to(&data.encode_to_vec().unwrap(), from)
        .await
        .unwrap();

    let mut s = timeout(Duration::from_secs(3), connect)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut got = [0u8; 7];
    s.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"punched");
    // Our own outbound attempt was abandoned without a word.
    assert_eq!(client.connection_counts(), (1, 0));
    assert_eq!(client.stats().inbound_connected, 1);
}

#[tokio::test]
async fn a_dropped_stream_closes_gracefully() {
    let cfg = SocketConfig {
        conn: conn::Config {
            time_wait: Duration::from_millis(100),
            ..conn::Config::default()
        },
        ..SocketConfig::default()
    };
    let server = bind(cfg.clone()).await;
    let acceptor = server.listen(None).unwrap();
    let client = bind(cfg).await;
    let mut s = client.connect(server.local_addr()).await.unwrap();
    let mut at_server = acceptor.accept().await.unwrap();
    s.write_all(b"last words").await.unwrap();
    drop(s);
    let mut got = Vec::new();
    timeout(Duration::from_secs(5), at_server.read_to_end(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, b"last words");
    at_server.shutdown().await.unwrap();
    drop(at_server);
    // Both sides forget the connection once the FINs are acknowledged
    // (time wait included).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while client.connection_counts().0 + server.connection_counts().0 > 0 {
        assert!(tokio::time::Instant::now() < deadline, "connections linger");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_dropped_stream_is_aborted_after_the_linger_time() {
    let cfg = SocketConfig {
        linger: Duration::from_millis(300),
        ..SocketConfig::default()
    };
    let server = bind(SocketConfig::default()).await;
    let acceptor = server.listen(None).unwrap();
    let client = bind(cfg).await;
    let s = client.connect(server.local_addr()).await.unwrap();
    let mut at_server = acceptor.accept().await.unwrap();
    // The server never closes its side: the client's close cannot finish.
    drop(s);
    let mut got = Vec::new();
    let end = timeout(Duration::from_secs(3), at_server.read_to_end(&mut got))
        .await
        .unwrap();
    assert!(got.is_empty());
    assert!(end.is_ok(), "the FIN arrives first: {end:?}");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(client.connection_counts(), (0, 0));
    assert_eq!(client.stats().linger_aborted, 1);
    // The ST_RESET reached the established remote.
    let err = at_server.write_all(b"too late").await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
}
