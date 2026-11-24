//! uTP through the VPS tunnel gateway (M4-04e): a seed speaking uTP on
//! loopback, reached through a lossy UDP relay, downloaded in full (every
//! piece SHA-1 checked) over an outbound uTP `open` and over an inbound uTP
//! `accept`; the rules that bound uTP to sessions that announce (no UDP
//! socket before an announce, no outbound uTP without one, no reply to an
//! inbound uTP peer for an info-hash nobody announces).

use super::*;
use crate::peerwire::frame as wire;
use crate::utp::packet::{Header, Packet, PacketType, SeqNr};
use crate::utp::socket::{SocketConfig, UtpSocket};
use crate::utp::testutil::{relay, LOSSY};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UdpSocket;

#[path = "punch_tests.rs"]
mod punch_tests;

const PIECE_LEN: usize = 64 * 1024;
const BLOCK: usize = 16 * 1024;
const SEED_ID: [u8; 20] = *b"-UT0001-utpseedpeer0";

/// The torrent's content: 1 MiB and a short last piece.
fn content() -> Arc<Vec<u8>> {
    Arc::new(
        (0..1024 * 1024 + 5_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect(),
    )
}

fn pieces(len: usize) -> usize {
    len.div_ceil(PIECE_LEN)
}

fn bitfield(len: usize) -> Vec<u8> {
    let n = pieces(len);
    let mut bits = vec![0xffu8; n.div_ceil(8)];
    if n % 8 != 0 {
        *bits.last_mut().unwrap() = 0xff << (8 - n % 8);
    }
    bits
}

/// A seed of `content` for `IH` on one byte stream: handshake (first when
/// `initiator`), bitfield, unchoke on interested, a piece per request.
async fn seed<S: AsyncRead + AsyncWrite + Unpin>(
    mut s: S,
    content: Arc<Vec<u8>>,
    initiator: bool,
) -> std::io::Result<()> {
    let ours = encode_handshake_with(&RESERVED, &IH, &SEED_ID);
    if initiator {
        s.write_all(&ours).await?;
    }
    let mut theirs = [0u8; HANDSHAKE_LEN];
    s.read_exact(&mut theirs).await?;
    assert_eq!(theirs, encode_handshake_with(&RESERVED, &IH, &PEER_ID));
    if !initiator {
        s.write_all(&ours).await?;
    }
    s.write_all(&wire(
        crate::peerwire::id::BITFIELD,
        &bitfield(content.len()),
    ))
    .await?;
    loop {
        let mut len = [0u8; 4];
        s.read_exact(&mut len).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len == 0 {
            continue;
        }
        let mut body = vec![0u8; len];
        s.read_exact(&mut body).await?;
        match body[0] {
            2 => {
                s.write_all(&wire(crate::peerwire::id::UNCHOKE, &[]))
                    .await?
            }
            6 if len == 13 => {
                let field = |i: usize| u32::from_be_bytes(body[i..i + 4].try_into().unwrap());
                let (index, begin, length) = (field(1), field(5), field(9) as usize);
                let at = index as usize * PIECE_LEN + begin as usize;
                let block = &content[at..at + length];
                let mut payload = Vec::with_capacity(8 + length);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(block);
                s.write_all(&wire(7, &payload)).await?;
            }
            _ => {}
        }
    }
}

/// A uTP endpoint on loopback serving `content` to whoever connects.
async fn utp_seed(content: Arc<Vec<u8>>) -> UtpSocket {
    let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap(), SocketConfig::default())
        .await
        .unwrap();
    let gate = socket.listen(None).unwrap();
    tokio::spawn(async move {
        while let Ok(stream) = gate.accept().await {
            tokio::spawn(seed(stream, content.clone(), false));
        }
    });
    socket
}

/// Peer-wire messages in `bytes` (length-prefixed), keep-alives skipped.
fn messages(bytes: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut at = 0;
    while bytes.len() - at >= 4 {
        let len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        if bytes.len() - at - 4 < len {
            break;
        }
        if len > 0 {
            out.push(&bytes[at + 4..at + 4 + len]);
        }
        at += 4 + len;
    }
    out
}

/// Download the whole torrent over `conn` and check every piece's SHA-1
/// against the torrent's (here: computed from the seed's content).
async fn download(t: &mut Client, conn: u16, content: &[u8]) {
    let hashes: Vec<[u8; 20]> = content
        .chunks(PIECE_LEN)
        .map(|p| Sha1::digest(p).into())
        .collect();
    t.send(&[data(conn, &wire(2, &[]))]).await; // interested
    let unchoked = |fs: &[Frame]| messages(&conn_data(fs, conn)).iter().any(|m| m[0] == 1);
    t.wait_for(unchoked, 20_000).await;
    let mut requests = Vec::new();
    let mut expected = t.data(conn).len();
    for (index, piece) in content.chunks(PIECE_LEN).enumerate() {
        for begin in (0..piece.len()).step_by(BLOCK) {
            let length = BLOCK.min(piece.len() - begin);
            let mut p = Vec::with_capacity(12);
            p.extend_from_slice(&(index as u32).to_be_bytes());
            p.extend_from_slice(&(begin as u32).to_be_bytes());
            p.extend_from_slice(&(length as u32).to_be_bytes());
            requests.extend_from_slice(&wire(6, &p));
            expected += 4 + 9 + length;
        }
    }
    t.send(&[data(conn, &requests)]).await;
    t.wait_for(|fs| data_len(fs, conn) >= expected, 90_000)
        .await;

    let mut got = vec![0u8; content.len()];
    let mut blocks = 0;
    for m in messages(&t.data(conn)).into_iter().filter(|m| m[0] == 7) {
        let index = u32::from_be_bytes(m[1..5].try_into().unwrap()) as usize;
        let begin = u32::from_be_bytes(m[5..9].try_into().unwrap()) as usize;
        let at = index * PIECE_LEN + begin;
        got[at..at + m.len() - 9].copy_from_slice(&m[9..]);
        blocks += 1;
    }
    assert_eq!(blocks, content.len().div_ceil(BLOCK));
    for (i, piece) in got.chunks(PIECE_LEN).enumerate() {
        let hash: [u8; 20] = Sha1::digest(piece).into();
        assert_eq!(hash, hashes[i], "piece {i} fails its hash");
    }
}

/// The bytes of `conn`'s `data` frames in `fs`.
fn conn_data(fs: &[Frame], conn: u16) -> Vec<u8> {
    fs.iter()
        .filter_map(|f| match f {
            Frame::Data { conn: c, payload } if *c == conn => Some(&payload[..]),
            _ => None,
        })
        .flatten()
        .copied()
        .collect()
}

fn open_utp(conn: u16, addr: SocketAddr, info_hash: [u8; 20]) -> Frame {
    Frame::Open {
        conn,
        peer: PeerFields {
            info_hash,
            reserved: RESERVED,
            peer_id: PEER_ID,
            addr,
            transport: transport::UTP,
        },
    }
}

/// Announce `IH` on `t` (no trackers): ACTIVE.
async fn announce(t: &mut Client, info_hash: [u8; 20]) {
    t.send(&[Frame::Announce {
        info_hash,
        reserved: RESERVED,
        peer_id: PEER_ID,
        trackers: vec![],
    }])
    .await;
    let active = Frame::Announced {
        info_hash,
        status: super::super::codec::announce_status::ACTIVE,
    };
    t.wait_for(|fs| fs.contains(&active), 5_000).await;
}

#[tokio::test]
async fn an_outbound_utp_open_downloads_a_whole_torrent_through_a_lossy_link() {
    let content = content();
    let seed = utp_seed(content.clone()).await;
    let via = relay(seed.local_addr(), LOSSY, 41).await;
    // Room for a retransmitted handshake (uTP's first timeout is 1 s).
    let server = inbound_server(|s| s.handshake_timeout = Duration::from_secs(5)).await;
    let mut t = Client::connect(&server, None).await;
    announce(&mut t, IH).await;
    t.send(&[open_utp(1, via, IH)]).await;
    assert_eq!(
        t.outcome(1).await,
        Frame::Opened {
            conn: 1,
            reserved: RESERVED,
            peer_id: SEED_ID
        }
    );
    download(&mut t, 1, &content).await;
    assert_eq!(seed.stats().inbound_connected, 1);
    t.close().await;
}

#[tokio::test]
async fn an_inbound_utp_peer_is_accepted_and_a_whole_torrent_downloaded_from_it() {
    let content = content();
    let (tracker, seen) = recording_tracker().await;
    // Longer than uTP's first retransmission timeout (1 s): a lost first
    // data packet must not cost the BitTorrent handshake.
    let server = inbound_server(|s| s.handshake_timeout = Duration::from_secs(5)).await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    // A seed behind NAT connects to the announced port over uTP.
    let via = relay(SocketAddr::from(([127, 0, 0, 1], port)), LOSSY, 42).await;
    let natted = UtpSocket::bind("127.0.0.1:0".parse().unwrap(), SocketConfig::default())
        .await
        .unwrap();
    let stream = natted.connect(via).await.unwrap();
    tokio::spawn(seed(stream, content.clone(), true));
    t.wait_for(
        |fs| fs.iter().any(|f| matches!(f, Frame::Accept { .. })),
        10_000,
    )
    .await;
    let Some(Frame::Accept { conn, peer }) = t
        .frames
        .iter()
        .find(|f| matches!(f, Frame::Accept { .. }))
        .cloned()
    else {
        unreachable!()
    };
    assert!(conn >= 0x8000);
    assert_eq!(peer.transport, transport::UTP);
    assert_eq!(peer.info_hash, IH);
    assert_eq!(peer.peer_id, SEED_ID);
    assert_eq!(peer.addr.ip().to_string(), "127.0.0.1");
    download(&mut t, conn, &content).await;
    t.close().await;
}

#[tokio::test]
async fn utp_opens_need_this_session_to_announce_the_info_hash() {
    let content = content();
    let seed = utp_seed(content).await;
    let to = seed.local_addr();
    let unsupported = |conn| Frame::Failed {
        conn,
        reason: fail_reason::UNSUPPORTED_TRANSPORT,
    };
    // A gateway without inbound peers has no UDP socket at all.
    let plain = serve(true, |_| {}).await;
    let mut t = Client::connect(&plain, None).await;
    t.send(&[open_utp(1, to, IH)]).await;
    assert_eq!(t.outcome(1).await, unsupported(1));

    let server = inbound_server(|_| {}).await;
    // No announce yet.
    let mut t = Client::connect(&server, None).await;
    t.send(&[open_utp(1, to, IH)]).await;
    assert_eq!(t.outcome(1).await, unsupported(1));
    // Another session announces IH: still not this one's.
    let mut other = Client::connect(&server, None).await;
    announce(&mut other, IH).await;
    t.send(&[open_utp(2, to, IH)]).await;
    assert_eq!(t.outcome(2).await, unsupported(2));
    // This session announces another info-hash only.
    announce(&mut t, [0x77; 20]).await;
    t.send(&[open_utp(3, to, IH)]).await;
    assert_eq!(t.outcome(3).await, unsupported(3));
    // An unknown transport.
    let mut odd = open_utp(4, to, [0x77; 20]);
    if let Frame::Open { peer, .. } = &mut odd {
        peer.transport = 9;
    }
    t.send(&[odd]).await;
    assert_eq!(t.outcome(4).await, unsupported(4));
    assert_eq!(seed.stats().datagrams_received, 0, "nothing dialled");
}

#[tokio::test]
async fn private_addresses_are_forbidden_over_utp_too() {
    let server = serve(false, |s| {
        s.inbound_addr = Some("127.0.0.1:0".parse().unwrap());
    })
    .await;
    let mut t = Client::connect(&server, None).await;
    announce(&mut t, IH).await;
    t.send(&[
        open_utp(1, "10.0.0.1:6881".parse().unwrap(), IH),
        open_utp(2, "127.0.0.1:7000".parse().unwrap(), IH),
        open_utp(3, "8.8.8.8:80".parse().unwrap(), IH),
    ])
    .await;
    for conn in 1..=3 {
        assert_eq!(
            t.outcome(conn).await,
            Frame::Failed {
                conn,
                reason: fail_reason::FORBIDDEN_ADDRESS
            }
        );
    }
}

/// A port free for both TCP and UDP on loopback right now.
fn free_port() -> u16 {
    loop {
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = tcp.local_addr().unwrap().port();
        if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn udp_free(port: u16) -> bool {
    std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok()
}

fn raw_header(packet_type: PacketType, connection_id: u16, seq: u16, ack: u16) -> Header {
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

/// The packets received on `sock` within `ms`.
async fn received(sock: &UdpSocket, ms: u64) -> Vec<(PacketType, SeqNr)> {
    let mut got = Vec::new();
    let mut buf = vec![0u8; 65_535];
    let end = tokio::time::Instant::now() + Duration::from_millis(ms);
    while let Ok(Ok((n, _))) = tokio::time::timeout_at(end, sock.recv_from(&mut buf)).await {
        let p = Packet::decode(&buf[..n]).expect("a uTP packet");
        got.push((p.header.packet_type, p.header.seq_nr));
    }
    got
}

#[tokio::test]
async fn the_udp_socket_exists_only_while_a_session_announces() {
    let port = free_port();
    let server = inbound_server(|s| {
        s.inbound_addr = Some(SocketAddr::from(([127, 0, 0, 1], port)));
    })
    .await;
    let mut t = Client::connect(&server, None).await;
    // Connected, even dialling over TCP: nothing bound on the UDP port.
    let echo = peer(Behaviour::Echo).await;
    t.send(&[open(1, echo.addr.port())]).await;
    assert!(matches!(t.outcome(1).await, Frame::Opened { .. }));
    assert!(udp_free(port), "a UDP socket before any announce");

    announce(&mut t, IH).await;
    assert!(!udp_free(port), "no UDP socket on the announced port");
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let to = SocketAddr::from(([127, 0, 0, 1], port));
    let syn = Packet::new(raw_header(PacketType::Syn, 300, 1, 0));
    probe
        .send_to(&syn.encode_to_vec().unwrap(), to)
        .await
        .unwrap();
    let got = received(&probe, 600).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, PacketType::State);

    t.send(&[Frame::Withdraw { info_hash: IH }]).await;
    wait_until(|| udp_free(port), 5_000).await;
    probe
        .send_to(&syn.encode_to_vec().unwrap(), to)
        .await
        .unwrap();
    assert!(received(&probe, 400).await.is_empty());
}

#[tokio::test]
async fn an_inbound_utp_peer_for_an_info_hash_nobody_announces_gets_no_reply() {
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|_| {}).await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    let to = SocketAddr::from(([127, 0, 0, 1], port));
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    // The uTP handshake is answered (one ST_STATE): the port takes uTP peers.
    let syn = Packet::new(raw_header(PacketType::Syn, 400, 1, 0));
    probe
        .send_to(&syn.encode_to_vec().unwrap(), to)
        .await
        .unwrap();
    let got = received(&probe, 600).await;
    assert_eq!(got.len(), 1);
    let (kind, their_seq) = got[0];
    assert_eq!(kind, PacketType::State);
    // Then a BitTorrent handshake for an info-hash nobody announces.
    let hs = encode_handshake_with(&RESERVED, &[0x5a; 20], b"-IN0001-inboundpeer0");
    let mut data = Packet::new(raw_header(PacketType::Data, 401, 2, their_seq.0));
    data.payload = &hs;
    probe
        .send_to(&data.encode_to_vec().unwrap(), to)
        .await
        .unwrap();
    // At most acknowledgements: no BitTorrent byte, no FIN, no RESET.
    let got = received(&probe, 1_500).await;
    assert!(
        got.iter().all(|(k, _)| *k == PacketType::State),
        "replies: {got:?}"
    );
    // Later packets of that connection match nothing: no reply at all.
    let mut again = Packet::new(raw_header(PacketType::Data, 401, 3, their_seq.0));
    again.payload = b"more";
    probe
        .send_to(&again.encode_to_vec().unwrap(), to)
        .await
        .unwrap();
    assert!(received(&probe, 500).await.is_empty());
    assert!(!t.frames.iter().any(|f| matches!(f, Frame::Accept { .. })));
}
