//! Hole punching through the VPS tunnel gateway (M4-05, BEP 55): a fake
//! relay introduces the client to a seed behind a simulated NAT, the client
//! sends a uTP `open` to the address the relay gave, and the whole torrent
//! is downloaded and checked; both connections of a simultaneous open
//! coming up leave one working tunnel.

use super::*;
use std::sync::atomic::AtomicBool;
use tokio::sync::oneshot;

/// The id the relay wants for ut_holepunch, and the one the client wants.
const RELAY_HP: u8 = 7;
const CLIENT_HP: u8 = 3;
const RELAY_ID: [u8; 20] = *b"-RL0001-holepunchrly";

/// BEP 55 message: msg_type, addr_type, addr, port, err_code.
fn holepunch(kind: u8, addr: SocketAddr, err: u32) -> Vec<u8> {
    let mut out = vec![kind];
    match addr {
        SocketAddr::V4(a) => {
            out.push(0);
            out.extend_from_slice(&a.ip().octets());
        }
        SocketAddr::V6(a) => {
            out.push(1);
            out.extend_from_slice(&a.ip().octets());
        }
    }
    out.extend_from_slice(&addr.port().to_be_bytes());
    out.extend_from_slice(&err.to_be_bytes());
    out
}

fn parse_holepunch(m: &[u8]) -> (u8, SocketAddr, u32) {
    assert_eq!(m[1], 0, "IPv4 only here");
    assert_eq!(m.len(), 12);
    let ip = std::net::Ipv4Addr::new(m[2], m[3], m[4], m[5]);
    let port = u16::from_be_bytes([m[6], m[7]]);
    let err = u32::from_be_bytes(m[8..12].try_into().unwrap());
    (m[0], SocketAddr::from((ip, port)), err)
}

/// One extended message (BEP 10) as a peer-wire message.
fn extended(id: u8, payload: &[u8]) -> Vec<u8> {
    let mut body = vec![id];
    body.extend_from_slice(payload);
    wire(crate::peerwire::id::EXTENDED, &body)
}

/// A NAT in front of `host`, for one remote: the host reaches `remote` by
/// sending to `inside`; everything leaves from `public`, after `delay`.
/// With `filter` (a port-restricted cone NAT), a datagram reaching `public`
/// gets in only from `remote` and only once the host has sent to it; the
/// others are dropped and counted. `first_in` fires when the first datagram
/// reaches `public`.
struct Nat {
    public: SocketAddr,
    inside: SocketAddr,
    dropped: Arc<AtomicUsize>,
    first_in: Arc<tokio::sync::Notify>,
}

async fn nat(remote: SocketAddr, host: SocketAddr, delay: Duration, filter: bool) -> Nat {
    let local: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let inside = Arc::new(UdpSocket::bind(local).await.unwrap());
    let public = Arc::new(UdpSocket::bind(local).await.unwrap());
    let dropped = Arc::new(AtomicUsize::new(0));
    let first_in = Arc::new(tokio::sync::Notify::new());
    let nat = Nat {
        public: public.local_addr().unwrap(),
        inside: inside.local_addr().unwrap(),
        dropped: dropped.clone(),
        first_in: first_in.clone(),
    };
    let forward = move |out: Arc<UdpSocket>, data: Vec<u8>, to: SocketAddr| {
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = out.send_to(&data, to).await;
        });
    };
    tokio::spawn(async move {
        let mut mapped = false;
        let mut seen = false;
        let (mut a, mut b) = (vec![0u8; 65_535], vec![0u8; 65_535]);
        loop {
            tokio::select! {
                r = inside.recv_from(&mut a) => {
                    let Ok((n, from)) = r else { continue };
                    if from == host {
                        mapped = true;
                        forward(public.clone(), a[..n].to_vec(), remote);
                    }
                }
                r = public.recv_from(&mut b) => {
                    let Ok((n, from)) = r else { continue };
                    if !seen {
                        seen = true;
                        first_in.notify_one();
                    }
                    if from == remote && (mapped || !filter) {
                        forward(inside.clone(), b[..n].to_vec(), host);
                    } else {
                        dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    });
    nat
}

/// The seed behind the NAT: it accepts uTP connections (only what its NAT
/// lets in reaches it) and, told to, connects to `inside` (the gateway as
/// it sees it). Every connection serves the whole torrent.
async fn natted_seed(content: Arc<Vec<u8>>) -> UtpSocket {
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

/// A relay (a peer connected to the client, which also knows the seed): it
/// answers a rendezvous for `target` with `connect` both ways (to the seed
/// through `notify`, with the gateway's announced address) and any other
/// with an error, NoSuchPeer (BEP 55).
async fn relay_peer(
    target: SocketAddr,
    gateway: SocketAddr,
    notify: oneshot::Sender<SocketAddr>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut theirs = [0u8; HANDSHAKE_LEN];
        s.read_exact(&mut theirs).await.unwrap();
        s.write_all(&encode_handshake_with(&RESERVED, &IH, &RELAY_ID))
            .await
            .unwrap();
        let m = format!("d1:md12:ut_holepunchi{RELAY_HP}eee");
        s.write_all(&extended(0, m.as_bytes())).await.unwrap();
        let mut notify = Some(notify);
        loop {
            let mut len = [0u8; 4];
            if s.read_exact(&mut len).await.is_err() {
                return;
            }
            let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
            if s.read_exact(&mut body).await.is_err() {
                return;
            }
            if body.first() != Some(&crate::peerwire::id::EXTENDED)
                || body.get(1) != Some(&RELAY_HP)
            {
                continue;
            }
            let (kind, to, _) = parse_holepunch(&body[2..]);
            assert_eq!(kind, 0, "a rendezvous");
            let reply = if to == target {
                if let Some(n) = notify.take() {
                    let _ = n.send(gateway);
                }
                holepunch(1, target, 0)
            } else {
                holepunch(2, to, 1)
            };
            s.write_all(&extended(CLIENT_HP, &reply)).await.unwrap();
        }
    });
    addr
}

/// The BEP 55 messages the relay sent on `conn` (client's id).
fn holepunches(t: &Client, conn: u16) -> Vec<(u8, SocketAddr, u32)> {
    messages(&t.data(conn))
        .into_iter()
        .filter(|m| m[0] == crate::peerwire::id::EXTENDED && m.get(1) == Some(&CLIENT_HP))
        .map(|m| parse_holepunch(&m[2..]))
        .collect()
}

/// Connect to the relay through the tunnel (TCP), advertise ut_holepunch
/// and send a rendezvous for `target`: the relay's answer.
async fn rendezvous(
    t: &mut Client,
    relay: SocketAddr,
    target: SocketAddr,
) -> (u8, SocketAddr, u32) {
    t.send(&[open_to(1, relay)]).await;
    assert!(matches!(t.outcome(1).await, Frame::Opened { .. }));
    let m = format!("d1:md12:ut_holepunchi{CLIENT_HP}eee");
    let mut out = extended(0, m.as_bytes());
    out.extend(extended(RELAY_HP, &holepunch(0, target, 0)));
    t.send(&[data(1, &out)]).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(m) = holepunches(t, 1).into_iter().next() {
            return m;
        }
        assert!(Instant::now() < deadline, "no answer to the rendezvous");
        let before = t.frames.len();
        t.wait_for(|fs| fs.len() > before, 10_000).await;
    }
}

#[tokio::test]
async fn a_seed_behind_nat_is_reached_through_a_relay() {
    let content = content();
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|s| {
        s.connect_timeout = Duration::from_secs(5);
        s.handshake_timeout = Duration::from_secs(5);
    })
    .await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    let gateway = SocketAddr::from(([127, 0, 0, 1], port));
    let seed_socket = natted_seed(content.clone()).await;
    let behind = nat(gateway, seed_socket.local_addr(), Duration::ZERO, true).await;

    let (tx, rx) = oneshot::channel();
    let relay = relay_peer(behind.public, gateway, tx).await;
    // The seed connects (BEP 55: "each peer initiates a uTP connection")
    // only once our first SYN has died at its NAT: the punch is needed.
    let dropped = behind.dropped.clone();
    let inside = behind.inside;
    let seed_connected = Arc::new(AtomicBool::new(false));
    let flag = seed_connected.clone();
    tokio::spawn(async move {
        let to = rx.await.unwrap();
        assert_eq!(to, gateway, "the relay gives the seed our announced port");
        while dropped.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if let Ok(stream) = seed_socket.connect(inside).await {
            flag.store(true, Ordering::Relaxed);
            let _ = seed(stream, content_for_seed(), true).await;
        }
    });

    let (kind, target, err) = rendezvous(&mut t, relay, behind.public).await;
    assert_eq!(
        (kind, target, err),
        (1, behind.public, 0),
        "connect, to the seed's public address"
    );
    t.send(&[open_utp(2, target, IH)]).await;
    assert_eq!(
        t.outcome(2).await,
        Frame::Opened {
            conn: 2,
            reserved: RESERVED,
            peer_id: SEED_ID
        }
    );
    assert!(
        behind.dropped.load(Ordering::Relaxed) >= 1,
        "our first SYN died at the NAT"
    );
    download(&mut t, 2, &content).await;
    // One tunnel: the seed's SYN went to the open, not to the listener.
    assert!(!t.frames.iter().any(|f| matches!(f, Frame::Accept { .. })));
    t.close().await;
}

/// The seed's content, as `content()` (a fresh `Arc` for the task).
fn content_for_seed() -> Arc<Vec<u8>> {
    content()
}

#[tokio::test]
async fn a_rendezvous_for_a_peer_the_relay_does_not_know_gets_an_error() {
    let server = inbound_server(|_| {}).await;
    let mut t = Client::connect(&server, None).await;
    announce(&mut t, IH).await;
    let (tx, _rx) = oneshot::channel();
    let relay = relay_peer(
        "127.0.0.1:40000".parse().unwrap(),
        "127.0.0.1:1".parse().unwrap(),
        tx,
    )
    .await;
    let other: SocketAddr = "127.0.0.1:40001".parse().unwrap();
    assert_eq!(rendezvous(&mut t, relay, other).await, (2, other, 1));
    t.close().await;
}

#[tokio::test]
async fn when_both_connections_of_a_simultaneous_open_come_up_the_one_the_seed_kept_is_used() {
    // The two SYNs cross on a 50 ms link, and so do the two ST_STATEs: each
    // side completes its own connect first and drops the other connection
    // (the multiplexer's simultaneous open, M4-04b). The gateway must use
    // the connection the seed answers on, not the one it completed first.
    let content = content();
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|s| {
        s.connect_timeout = Duration::from_secs(5);
        s.handshake_timeout = Duration::from_secs(5);
    })
    .await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    let gateway = SocketAddr::from(([127, 0, 0, 1], port));
    let seed_socket = natted_seed(content.clone()).await;
    let link = nat(
        gateway,
        seed_socket.local_addr(),
        Duration::from_millis(50),
        false,
    )
    .await;
    let (first_in, inside) = (link.first_in.clone(), link.inside);
    tokio::spawn(async move {
        // The gateway's SYN is on the wire: connect now, so the SYNs cross.
        first_in.notified().await;
        if let Ok(stream) = seed_socket.connect(inside).await {
            let _ = seed(stream, content_for_seed(), true).await;
        }
    });
    t.send(&[open_utp(1, link.public, IH)]).await;
    assert_eq!(
        t.outcome(1).await,
        Frame::Opened {
            conn: 1,
            reserved: RESERVED,
            peer_id: SEED_ID
        }
    );
    download(&mut t, 1, &content).await;
    assert!(!t.frames.iter().any(|f| matches!(f, Frame::Accept { .. })));
    t.close().await;
}
