//! Integration tests of the VPS tunnel gateway: the real `/v1/tunnel` route
//! on a loopback port, a WebSocket client, and scripted peers on loopback.
//! The scenarios of the Cloudflare gateway's suite
//! (apps/gateway/test/integration/tunnel.test.ts): open/data/close both
//! ways, failed (refused, timeout, not BitTorrent, info-hash mismatch,
//! forbidden address), the handshake gate, the 6-connection limit, Origin
//! and subprotocol checks, several WebSockets per session, idle and session
//! timeouts, socket cleanup, the per-IP upgrade limit, a peer that does not
//! read; announce and inbound peers; uTP (`utp_tests.rs`).

use super::codec::{
    close_code, close_reason, decode_message, encode_frames, fail_reason, transport, DecodeLimits,
    Frame, PeerFields, SUBPROTOCOL,
};
use super::{parse_session, router, Registry, Tunnel};
use crate::config::{AllowedOrigins, TunnelSettings};
use crate::peerwire::{encode_handshake_with, HANDSHAKE_LEN};
use axum::http::HeaderMap;
use bytes::Bytes;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

#[path = "utp_tests.rs"]
mod utp_tests;

const IH: [u8; 20] = [0xa5; 20];
const RESERVED: [u8; 8] = [0, 0, 0, 0, 0, 0x10, 0, 0x04];
const PEER_ID: [u8; 20] = *b"-MS0001-integration0";
const ORIGIN: &str = "http://localhost:4321";

// ------------------------------------------------------------ test peers

#[derive(Clone, Copy, PartialEq)]
enum Behaviour {
    /// Valid handshake, a have-all right behind it, then echoes every byte.
    Echo,
    /// Valid handshake, then closes after the first bytes it reads.
    HangUp,
    /// Valid handshake, then never reads.
    NoRead,
    /// Accepts and says nothing.
    Silent,
    /// Answers like an HTTP server.
    Http,
    /// Handshakes for another torrent.
    WrongHash,
}

struct Peer {
    addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    /// The handshakes the gateway wrote to this peer.
    handshakes: Arc<Mutex<Vec<Vec<u8>>>>,
}

const HAVE_ALL: [u8; 5] = [0, 0, 0, 1, 0x0e];

async fn peer(behaviour: Behaviour) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let handshakes = Arc::new(Mutex::new(Vec::new()));
    let (c, d, h) = (connections.clone(), closed.clone(), handshakes.clone());
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            c.fetch_add(1, Ordering::SeqCst);
            let (d, h) = (d.clone(), h.clone());
            tokio::spawn(async move {
                serve_peer(stream, behaviour, h).await;
                d.fetch_add(1, Ordering::SeqCst);
            });
        }
    });
    Peer {
        addr,
        connections,
        closed,
        handshakes,
    }
}

async fn serve_peer(mut s: TcpStream, behaviour: Behaviour, seen: Arc<Mutex<Vec<Vec<u8>>>>) {
    let hold = || tokio::time::sleep(Duration::from_secs(600));
    match behaviour {
        Behaviour::Silent => return hold().await,
        Behaviour::Http => {
            let _ = s
                .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                .await;
            return hold().await;
        }
        _ => {}
    }
    let mut theirs = [0u8; HANDSHAKE_LEN];
    if s.read_exact(&mut theirs).await.is_err() {
        return;
    }
    seen.lock().unwrap().push(theirs.to_vec());
    let ih = if behaviour == Behaviour::WrongHash {
        [0x5a; 20]
    } else {
        IH
    };
    let mut ours = encode_handshake_with(&RESERVED, &ih, b"-TP0001-abcdefghijkl").to_vec();
    if behaviour == Behaviour::Echo {
        ours.extend_from_slice(&HAVE_ALL);
    }
    if s.write_all(&ours).await.is_err() {
        return;
    }
    match behaviour {
        Behaviour::NoRead | Behaviour::WrongHash => hold().await,
        Behaviour::HangUp => {
            let mut buf = [0u8; 64];
            let _ = s.read(&mut buf).await;
        }
        _ => {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match s.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        if s.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// A port nobody listens on.
async fn closed_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

// --------------------------------------------------------------- server

struct Server {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
}

fn settings() -> TunnelSettings {
    TunnelSettings {
        connect_timeout: Duration::from_millis(1500),
        handshake_timeout: Duration::from_millis(1500),
        max_connections: 6,
        idle_timeout: Duration::from_secs(90),
        session_timeout: Duration::from_secs(7200),
        sockets_per_session: 3,
        upgrades_per_minute: 600,
        max_pending_write: 1 << 20,
        sockets_per_ip: 100,
        max_sockets: 100,
        inbound_addr: None,
        announce_port: 0,
        announce_interval: Duration::from_secs(900),
        announce_dht: false,
        inbound_per_session: 16,
        inbound_max: 256,
        inbound_pending: 64,
        announces_per_socket: 2,
        announces_max: 64,
        announces_per_ip: 64,
        announce_rounds_per_ip: 600,
        announce_rounds_max: 10_000,
    }
}

async fn serve(allow_private: bool, tune: impl FnOnce(&mut TunnelSettings)) -> Server {
    let mut s = settings();
    tune(&mut s);
    let (shutdown, rx) = watch::channel(false);
    let tunnel = Tunnel::new(
        s,
        AllowedOrigins::List(vec![ORIGIN.into()]),
        allow_private,
        "x-real-ip".into(),
        rx,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router::<()>(tunnel).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server { addr, shutdown }
}

// --------------------------------------------------------------- client

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

enum Incoming {
    Frames(Vec<Frame>),
    Closed(u16, String),
}

struct Client {
    sink: SplitSink<Ws, WsMessage>,
    rx: mpsc::UnboundedReceiver<Incoming>,
    frames: Vec<Frame>,
    closed: Option<(u16, String)>,
}

fn request(
    server: &Server,
    query: &str,
    headers: &[(&str, &str)],
) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    let mut req = format!("ws://{}/v1/tunnel{query}", server.addr)
        .into_client_request()
        .unwrap();
    for (k, v) in headers {
        req.headers_mut().insert(
            axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    req
}

/// HTTP status of an upgrade attempt (101 when it succeeded).
async fn upgrade_status(server: &Server, query: &str, headers: &[(&str, &str)]) -> u16 {
    match tokio_tungstenite::connect_async(request(server, query, headers)).await {
        Ok((ws, resp)) => {
            drop(ws);
            resp.status().as_u16()
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => resp.status().as_u16(),
        Err(e) => panic!("upgrade failed: {e}"),
    }
}

impl Client {
    async fn connect(server: &Server, session: Option<&str>) -> Client {
        let query = session.map(|s| format!("?session={s}")).unwrap_or_default();
        let req = request(
            server,
            &query,
            &[("sec-websocket-protocol", SUBPROTOCOL), ("origin", ORIGIN)],
        );
        let (ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
        assert_eq!(
            resp.headers().get("sec-websocket-protocol").unwrap(),
            SUBPROTOCOL
        );
        let (sink, mut stream) = ws.split();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(WsMessage::Binary(b)) => {
                        let frames = decode_message(&b, DecodeLimits::default()).unwrap();
                        let _ = tx.send(Incoming::Frames(frames));
                    }
                    Ok(WsMessage::Close(cf)) => {
                        let (code, reason) = cf
                            .map(|c| (u16::from(c.code), c.reason.to_string()))
                            .unwrap_or((1005, String::new()));
                        let _ = tx.send(Incoming::Closed(code, reason));
                        return;
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            let _ = tx.send(Incoming::Closed(1006, String::new()));
        });
        Client {
            sink,
            rx,
            frames: Vec::new(),
            closed: None,
        }
    }

    async fn send(&mut self, frames: &[Frame]) {
        let _ = self
            .sink
            .send(WsMessage::Binary(encode_frames(frames).into()))
            .await;
    }

    async fn send_raw(&mut self, msg: WsMessage) {
        let _ = self.sink.send(msg).await;
    }

    /// Wait until `pred` holds for the frames received so far.
    async fn wait_for(&mut self, mut pred: impl FnMut(&[Frame]) -> bool, ms: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
        while !pred(&self.frames) {
            match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                Ok(Some(Incoming::Frames(f))) => self.frames.extend(f),
                Ok(Some(Incoming::Closed(code, reason))) => {
                    self.closed = Some((code, reason));
                    assert!(pred(&self.frames), "WebSocket closed ({code}) first");
                    return;
                }
                Ok(None) | Err(_) => panic!("timed out; frames: {:?}", self.frames),
            }
        }
    }

    /// The first frame for `conn` that is not `data`, waiting for it.
    async fn outcome(&mut self, conn: u16) -> Frame {
        let is = move |f: &Frame| f.conn() == conn && !matches!(f, Frame::Data { .. });
        self.wait_for(|fs| fs.iter().any(is), 10_000).await;
        self.frames.iter().find(|f| is(f)).unwrap().clone()
    }

    async fn closed(&mut self, ms: u64) -> (u16, String) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
        while self.closed.is_none() {
            match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                Ok(Some(Incoming::Frames(f))) => self.frames.extend(f),
                Ok(Some(Incoming::Closed(code, reason))) => self.closed = Some((code, reason)),
                Ok(None) | Err(_) => panic!("WebSocket not closed in time"),
            }
        }
        self.closed.clone().unwrap()
    }

    /// Bytes received as `data` on `conn`.
    fn data(&self, conn: u16) -> Vec<u8> {
        self.frames
            .iter()
            .filter_map(|f| match f {
                Frame::Data { conn: c, payload } if *c == conn => Some(payload.to_vec()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    async fn close(mut self) {
        let _ = self
            .sink
            .send(WsMessage::Close(Some(CloseFrame {
                code: 1000.into(),
                reason: "done".into(),
            })))
            .await;
    }
}

fn open(conn: u16, port: u16) -> Frame {
    open_to(conn, SocketAddr::from(([127, 0, 0, 1], port)))
}

fn open_to(conn: u16, addr: SocketAddr) -> Frame {
    Frame::Open {
        conn,
        peer: PeerFields {
            info_hash: IH,
            reserved: RESERVED,
            peer_id: PEER_ID,
            addr,
            transport: transport::TCP,
        },
    }
}

fn data(conn: u16, bytes: &[u8]) -> Frame {
    Frame::Data {
        conn,
        payload: Bytes::copy_from_slice(bytes),
    }
}

async fn wait_until(mut cond: impl FnMut() -> bool, ms: u64) {
    let end = Instant::now() + Duration::from_millis(ms);
    while !cond() {
        assert!(Instant::now() < end, "condition not met in time");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn session_id() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}

// ---------------------------------------------------------------- tests

#[tokio::test]
async fn opens_a_tunnel_and_relays_bytes_both_ways_after_the_gate() {
    let echo = peer(Behaviour::Echo).await;
    let server = serve(true, |_| {}).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[open(1, echo.addr.port())]).await;
    let opened = t.outcome(1).await;
    assert_eq!(
        opened,
        Frame::Opened {
            conn: 1,
            reserved: RESERVED,
            peer_id: *b"-TP0001-abcdefghijkl"
        }
    );
    // The gateway wrote the handshake with the client's reserved bytes and peer id.
    let hs = echo.handshakes.lock().unwrap()[0].clone();
    assert_eq!(hs, encode_handshake_with(&RESERVED, &IH, &PEER_ID).to_vec());
    // The bytes that came with the remote's handshake are relayed, not the handshake.
    let mut want = HAVE_ALL.to_vec();
    t.wait_for(
        |fs| fs.iter().any(|f| matches!(f, Frame::Data { .. })),
        5_000,
    )
    .await;
    assert_eq!(t.data(1), want);
    // Client → peer → client.
    let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    t.send(&[data(1, &payload[..100_000]), data(1, &payload[100_000..])])
        .await;
    want.extend_from_slice(&payload);
    let len = want.len();
    let wait = |fs: &[Frame]| {
        fs.iter()
            .filter_map(|f| match f {
                Frame::Data { payload, .. } => Some(payload.len()),
                _ => None,
            })
            .sum::<usize>()
            >= len
    };
    t.wait_for(wait, 5_000).await;
    assert_eq!(t.data(1), want);
    // Our close reaches the peer.
    t.send(&[Frame::Close {
        conn: 1,
        reason: close_reason::NORMAL,
    }])
    .await;
    wait_until(|| echo.closed.load(Ordering::SeqCst) == 1, 5_000).await;
    t.close().await;
}

#[tokio::test]
async fn reports_a_peer_that_hangs_up() {
    let p = peer(Behaviour::HangUp).await;
    let server = serve(true, |_| {}).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[open(3, p.addr.port())]).await;
    assert!(matches!(t.outcome(3).await, Frame::Opened { .. }));
    t.send(&[data(3, &[0, 0, 0, 1, 2])]).await;
    t.wait_for(
        |fs| fs.iter().any(|f| matches!(f, Frame::Close { .. })),
        5_000,
    )
    .await;
    let close = t.frames.iter().find(|f| matches!(f, Frame::Close { .. }));
    assert_eq!(
        close,
        Some(&Frame::Close {
            conn: 3,
            reason: close_reason::REMOTE_CLOSED
        })
    );
}

#[tokio::test]
async fn fails_refused_timeout_not_bittorrent_and_mismatch_without_relaying() {
    let refused = closed_port().await;
    let silent = peer(Behaviour::Silent).await;
    let http = peer(Behaviour::Http).await;
    let wrong = peer(Behaviour::WrongHash).await;
    let server = serve(true, |_| {}).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[
        open(1, refused),
        open(2, silent.addr.port()),
        open(3, http.addr.port()),
        open(4, wrong.addr.port()),
    ])
    .await;
    let mut reasons = Vec::new();
    for conn in 1..=4 {
        match t.outcome(conn).await {
            Frame::Failed { reason, .. } => reasons.push(reason),
            other => panic!("conn {conn}: {other:?}"),
        }
    }
    assert_eq!(
        reasons,
        [
            fail_reason::REFUSED,
            fail_reason::TIMEOUT,
            fail_reason::NOT_BITTORRENT,
            fail_reason::INFOHASH_MISMATCH
        ]
    );
    assert!(t.frames.iter().all(|f| matches!(f, Frame::Failed { .. })));
}

#[tokio::test]
async fn data_before_opened_closes_that_connection_and_reaches_no_one() {
    let silent = peer(Behaviour::Silent).await;
    let server = serve(true, |_| {}).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[
        open(1, silent.addr.port()),
        data(1, b"GET / HTTP/1.1\r\n\r\n"),
    ])
    .await;
    assert_eq!(
        t.outcome(1).await,
        Frame::Close {
            conn: 1,
            reason: close_reason::PROTOCOL_ERROR
        }
    );
    // The id is free again at once.
    let echo = peer(Behaviour::Echo).await;
    t.send(&[open(1, echo.addr.port())]).await;
    t.wait_for(
        |fs| {
            fs.iter()
                .any(|f| matches!(f, Frame::Opened { conn: 1, .. }))
        },
        5_000,
    )
    .await;
}

#[tokio::test]
async fn closes_a_peer_that_does_not_read_and_only_that_connection() {
    let stuck = peer(Behaviour::NoRead).await;
    let echo = peer(Behaviour::Echo).await;
    let server = serve(true, |s| s.max_pending_write = 256 * 1024).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[open(1, stuck.addr.port()), open(2, echo.addr.port())])
        .await;
    for conn in [1, 2] {
        assert!(matches!(t.outcome(conn).await, Frame::Opened { .. }));
    }
    // Far more than the kernel's socket buffers plus the bound.
    let chunk = vec![7u8; 256 * 1024];
    let overflowed = |fs: &[Frame]| fs.iter().any(|f| matches!(f, Frame::Close { conn: 1, .. }));
    for i in 0..512 {
        if overflowed(&t.frames) {
            break;
        }
        t.send(&[data(1, &chunk), data(1, &chunk)]).await;
        if i % 8 == 7 {
            while let Ok(Incoming::Frames(f)) = t.rx.try_recv() {
                t.frames.extend(f);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    t.wait_for(overflowed, 20_000).await;
    assert!(t.frames.contains(&Frame::Close {
        conn: 1,
        reason: close_reason::WRITE_OVERFLOW
    }));
    // The other connection of the socket still works.
    let before = t.data(2).len();
    t.send(&[data(2, b"ping")]).await;
    let mut want = t.data(2);
    want.extend_from_slice(b"ping");
    t.wait_for(
        |fs| {
            fs.iter()
                .filter_map(|f| match f {
                    Frame::Data { conn: 2, payload } => Some(payload.len()),
                    _ => None,
                })
                .sum::<usize>()
                >= before + 4
        },
        5_000,
    )
    .await;
    assert_eq!(t.data(2), want);
    assert!(!t
        .frames
        .iter()
        .any(|f| matches!(f, Frame::Close { conn: 2, .. })));
}

#[tokio::test]
async fn refuses_the_seventh_connection_and_a_reused_id() {
    let echo = peer(Behaviour::Echo).await;
    let server = serve(true, |_| {}).await;
    let mut t = Client::connect(&server, None).await;
    let opens: Vec<Frame> = (1..=6).map(|c| open(c, echo.addr.port())).collect();
    t.send(&opens).await;
    for conn in 1..=6 {
        assert!(matches!(t.outcome(conn).await, Frame::Opened { .. }));
    }
    t.send(&[open(7, echo.addr.port())]).await;
    assert_eq!(
        t.outcome(7).await,
        Frame::Failed {
            conn: 7,
            reason: fail_reason::TOO_MANY_CONNECTIONS
        }
    );
    // A slot frees once a connection is closed.
    t.send(&[
        Frame::Close {
            conn: 6,
            reason: close_reason::NORMAL,
        },
        open(1, echo.addr.port()),
    ])
    .await;
    t.wait_for(
        |fs| {
            fs.iter().any(|f| {
                *f == Frame::Failed {
                    conn: 1,
                    reason: fail_reason::CONN_IN_USE,
                }
            })
        },
        5_000,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    t.send(&[open(8, echo.addr.port())]).await;
    t.wait_for(
        |fs| {
            fs.iter()
                .any(|f| matches!(f, Frame::Opened { conn: 8, .. }))
        },
        5_000,
    )
    .await;
}

#[tokio::test]
async fn a_malformed_frame_a_gateway_frame_or_text_closes_the_websocket() {
    let server = serve(true, |_| {}).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[Frame::Opened {
        conn: 1,
        reserved: RESERVED,
        peer_id: PEER_ID,
    }])
    .await;
    assert_eq!(t.closed(5_000).await.0, close_code::PROTOCOL_ERROR);

    let mut t = Client::connect(&server, None).await;
    t.send_raw(WsMessage::Binary(vec![0x09, 0, 0, 0, 0, 0, 0].into()))
        .await;
    let (code, reason) = t.closed(5_000).await;
    assert_eq!(code, close_code::PROTOCOL_ERROR);
    assert_eq!(reason, "protocol_error in=1 up=0 down=0");

    let mut t = Client::connect(&server, None).await;
    t.send_raw(WsMessage::Text("hello".into())).await;
    assert_eq!(t.closed(5_000).await.0, close_code::UNSUPPORTED_DATA);
}

#[tokio::test]
async fn forbids_private_addresses_and_low_ports_without_connecting() {
    let echo = peer(Behaviour::Echo).await;
    let strict = serve(false, |_| {}).await;
    let mut t = Client::connect(&strict, None).await;
    t.send(&[
        open(1, echo.addr.port()),
        open_to(2, "10.0.0.1:6881".parse().unwrap()),
        open_to(3, "[::1]:6881".parse().unwrap()),
        open_to(4, "8.8.8.8:80".parse().unwrap()),
    ])
    .await;
    for conn in 1..=4 {
        assert_eq!(
            t.outcome(conn).await,
            Frame::Failed {
                conn,
                reason: fail_reason::FORBIDDEN_ADDRESS
            }
        );
    }
    // Even where private peers are allowed, ports below 1024 are not.
    let local = serve(true, |_| {}).await;
    let mut t = Client::connect(&local, None).await;
    t.send(&[open(1, 1023)]).await;
    assert_eq!(
        t.outcome(1).await,
        Frame::Failed {
            conn: 1,
            reason: fail_reason::FORBIDDEN_ADDRESS
        }
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(echo.connections.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn origin_subprotocol_upgrade_and_session_are_checked_before_the_upgrade() {
    let server = serve(true, |_| {}).await;
    let proto = ("sec-websocket-protocol", SUBPROTOCOL);
    assert_eq!(
        upgrade_status(&server, "", &[proto, ("origin", "https://evil.example")]).await,
        403
    );
    assert_eq!(
        upgrade_status(&server, "", &[proto, ("origin", ORIGIN)]).await,
        101
    );
    // Not from a page (no Origin): allowed, like the Cloudflare gateway.
    assert_eq!(upgrade_status(&server, "", &[proto]).await, 101);
    assert_eq!(upgrade_status(&server, "", &[]).await, 400);
    assert_eq!(
        upgrade_status(
            &server,
            "",
            &[("sec-websocket-protocol", "magnet-tunnel.v9")]
        )
        .await,
        400
    );
    assert_eq!(
        upgrade_status(&server, "?session=NOT-HEX", &[proto]).await,
        400
    );
    let plain = reqwest::get(format!("http://{}/v1/tunnel", server.addr))
        .await
        .unwrap();
    assert_eq!(plain.status().as_u16(), 426);
    let foreign = reqwest::Client::new()
        .get(format!("http://{}/v1/tunnel", server.addr))
        .header("origin", "https://evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(foreign.status().as_u16(), 403);
}

#[tokio::test]
async fn closing_the_websocket_closes_every_tcp_socket() {
    let echo = peer(Behaviour::Echo).await;
    let server = serve(true, |_| {}).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[open(1, echo.addr.port()), open(2, echo.addr.port())])
        .await;
    for conn in [1, 2] {
        assert!(matches!(t.outcome(conn).await, Frame::Opened { .. }));
    }
    t.close().await;
    wait_until(|| echo.closed.load(Ordering::SeqCst) == 2, 5_000).await;
}

#[tokio::test]
async fn a_session_holds_three_websockets_with_six_connections_each() {
    let echo = peer(Behaviour::Echo).await;
    let server = serve(true, |_| {}).await;
    let session = session_id();
    let mut a = Client::connect(&server, Some(&session)).await;
    let mut b = Client::connect(&server, Some(&session)).await;
    let c = Client::connect(&server, Some(&session)).await;
    let opens: Vec<Frame> = (1..=6).map(|c| open(c, echo.addr.port())).collect();
    a.send(&opens).await;
    b.send(&opens).await;
    for t in [&mut a, &mut b] {
        for conn in 1..=6 {
            assert!(matches!(t.outcome(conn).await, Frame::Opened { .. }));
        }
    }
    b.send(&[open(7, echo.addr.port())]).await;
    assert_eq!(
        b.outcome(7).await,
        Frame::Failed {
            conn: 7,
            reason: fail_reason::TOO_MANY_CONNECTIONS
        }
    );
    let proto = ("sec-websocket-protocol", SUBPROTOCOL);
    let q = format!("?session={session}");
    assert_eq!(upgrade_status(&server, &q, &[proto]).await, 429);
    // Another session is unaffected.
    let other = format!("?session={}", session_id());
    assert_eq!(upgrade_status(&server, &other, &[proto]).await, 101);
    // A seat frees when its WebSocket closes.
    c.close().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut d = Client::connect(&server, Some(&session)).await;
    d.send(&[Frame::Ping {
        payload: Bytes::new(),
    }])
    .await;
}

#[tokio::test]
async fn over_the_per_ip_upgrade_limit_answers_429() {
    let server = serve(true, |s| s.upgrades_per_minute = 3).await;
    let proto = ("sec-websocket-protocol", SUBPROTOCOL);
    let mut statuses = Vec::new();
    for _ in 0..5 {
        statuses.push(upgrade_status(&server, "", &[proto]).await);
    }
    assert_eq!(statuses, [101, 101, 101, 429, 429]);
}

#[tokio::test]
async fn concurrent_sockets_per_ip_and_in_total_are_capped() {
    let server = serve(true, |s| {
        s.sockets_per_ip = 2;
    })
    .await;
    let _a = Client::connect(&server, None).await;
    let _b = Client::connect(&server, None).await;
    let proto = ("sec-websocket-protocol", SUBPROTOCOL);
    assert_eq!(upgrade_status(&server, "", &[proto]).await, 429);
    let server = serve(true, |s| s.max_sockets = 1).await;
    let _a = Client::connect(&server, None).await;
    assert_eq!(upgrade_status(&server, "", &[proto]).await, 503);
}

#[tokio::test]
async fn an_idle_connection_closes_and_a_busy_one_stays() {
    let echo = peer(Behaviour::Echo).await;
    let server = serve(true, |s| s.idle_timeout = Duration::from_millis(1500)).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[open(1, echo.addr.port()), open(2, echo.addr.port())])
        .await;
    for conn in [1, 2] {
        assert!(matches!(t.outcome(conn).await, Frame::Opened { .. }));
    }
    let started = Instant::now();
    loop {
        t.send(&[data(2, &[0, 0, 0, 0])]).await;
        while let Ok(Incoming::Frames(f)) = t.rx.try_recv() {
            t.frames.extend(f);
        }
        if t.frames.iter().any(|f| matches!(f, Frame::Close { .. })) {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(6), "no idle close");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    while let Ok(Incoming::Frames(f)) = t.rx.try_recv() {
        t.frames.extend(f);
    }
    let closes: Vec<&Frame> = t
        .frames
        .iter()
        .filter(|f| matches!(f, Frame::Close { .. }))
        .collect();
    assert_eq!(
        closes,
        [&Frame::Close {
            conn: 1,
            reason: close_reason::IDLE
        }]
    );
}

#[tokio::test]
async fn a_websocket_without_client_messages_closes_idle() {
    let server = serve(true, |s| s.idle_timeout = Duration::from_millis(1000)).await;
    let mut t = Client::connect(&server, None).await;
    let (code, reason) = t.closed(5_000).await;
    assert_eq!(code, close_code::IDLE);
    assert_eq!(reason, "idle in=0 up=0 down=0");
}

#[tokio::test]
async fn the_session_ends_at_its_maximum_length() {
    let echo = peer(Behaviour::Echo).await;
    let server = serve(true, |s| s.session_timeout = Duration::from_secs(2)).await;
    let session = session_id();
    let mut t = Client::connect(&server, Some(&session)).await;
    t.send(&[open(1, echo.addr.port())]).await;
    assert!(matches!(t.outcome(1).await, Frame::Opened { .. }));
    let (code, reason) = t.closed(6_000).await;
    assert_eq!(code, close_code::SESSION_TIMEOUT);
    assert!(reason.starts_with("session_timeout in=1 "), "{reason}");
    assert!(t.frames.contains(&Frame::Close {
        conn: 1,
        reason: close_reason::SESSION_TIMEOUT
    }));
    wait_until(|| echo.closed.load(Ordering::SeqCst) == 1, 5_000).await;
}

#[tokio::test]
async fn a_shutdown_closes_every_websocket_with_4002() {
    let echo = peer(Behaviour::Echo).await;
    let server = serve(true, |_| {}).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[open(1, echo.addr.port())]).await;
    assert!(matches!(t.outcome(1).await, Frame::Opened { .. }));
    server.shutdown.send(true).unwrap();
    assert_eq!(t.closed(5_000).await.0, close_code::SHUTDOWN);
    assert!(t.frames.contains(&Frame::Close {
        conn: 1,
        reason: close_reason::SESSION_ENDING
    }));
}

// ------------------------------------------------------------ unit tests

#[test]
fn session_names_are_32_lowercase_hex() {
    assert_eq!(parse_session(None), Ok(None));
    assert_eq!(parse_session(Some("x=1")), Ok(None));
    let id = "0123456789abcdef0123456789abcdef";
    assert_eq!(
        parse_session(Some(&format!("session={id}"))),
        Ok(Some(id.into()))
    );
    for bad in [
        "session=",
        "session=0123456789ABCDEF0123456789abcdef",
        "session=0123",
        &format!("session={id}&session={id}"),
    ] {
        assert!(parse_session(Some(bad)).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn a_session_past_its_length_refuses_new_websockets() {
    let registry = Arc::new(Registry {
        sessions: Mutex::new(Default::default()),
        per_session: 3,
        timeout: Duration::from_millis(50),
    });
    let first = registry.join(Some("a".into())).ok().unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(matches!(
        registry.join(Some("a".into())),
        Err(super::JoinError::Expired)
    ));
    drop(first);
    // Once its last WebSocket is gone the name is free for a new session.
    assert!(registry.join(Some("a".into())).is_ok());
}

#[test]
fn private_peers_only_for_direct_loopback_requests() {
    let (_tx, rx) = watch::channel(false);
    let tunnel = Tunnel::new(
        settings(),
        AllowedOrigins::None,
        true,
        "x-real-ip".into(),
        rx,
    );
    let headers = |host: &str, proxied: bool| {
        let mut h = HeaderMap::new();
        h.insert("host", host.parse().unwrap());
        if proxied {
            h.insert("x-real-ip", "203.0.113.5".parse().unwrap());
        }
        h
    };
    let local: Option<SocketAddr> = Some("127.0.0.1:50000".parse().unwrap());
    let remote: Option<SocketAddr> = Some("198.51.100.1:50000".parse().unwrap());
    assert!(tunnel.allow_private(local, &headers("127.0.0.1:8080", false)));
    assert!(tunnel.allow_private(local, &headers("localhost:8080", false)));
    assert!(tunnel.allow_private(local, &headers("[::1]:8080", false)));
    assert!(!tunnel.allow_private(local, &headers("vps.example.com", false)));
    assert!(!tunnel.allow_private(local, &headers("127.0.0.1:8080", true)));
    assert!(!tunnel.allow_private(remote, &headers("127.0.0.1:8080", false)));
    let (_tx, rx) = watch::channel(false);
    let off = Tunnel::new(
        settings(),
        AllowedOrigins::None,
        false,
        "x-real-ip".into(),
        rx,
    );
    assert!(!off.allow_private(local, &headers("127.0.0.1:8080", false)));
}

#[test]
fn origins_follow_allowed_origins() {
    let make = |origins| {
        let (_tx, rx) = watch::channel(false);
        Tunnel::new(settings(), origins, false, "x-real-ip".into(), rx)
    };
    let list = make(AllowedOrigins::List(vec![ORIGIN.into()]));
    assert!(list.origin_allowed(Some(ORIGIN)));
    assert!(list.origin_allowed(None));
    assert!(!list.origin_allowed(Some("https://evil.example")));
    let none = make(AllowedOrigins::None);
    assert!(!none.origin_allowed(Some(ORIGIN)));
    assert!(none.origin_allowed(None));
    assert!(make(AllowedOrigins::Any).origin_allowed(Some("https://any.example")));
}

// ------------------------------------------------ announce and inbound

/// A BEP 3 tracker on loopback recording each announce's query string,
/// answering with an empty peer list.
async fn recording_tracker() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let mut req = Vec::new();
                let mut buf = [0u8; 2048];
                while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => req.extend_from_slice(&buf[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&req).to_string();
                let line = text.lines().next().unwrap_or("").to_string();
                log.lock().unwrap().push(line);
                let body = b"d8:intervali1800e5:peers0:e";
                let mut r = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                r.extend_from_slice(body);
                let _ = sock.write_all(&r).await;
            });
        }
    });
    (url, seen)
}

/// The value of `key` in the last recorded announce with `event`, if any.
fn announced(seen: &Mutex<Vec<String>>, event: &str) -> Option<String> {
    seen.lock()
        .unwrap()
        .iter()
        .find(|l| l.contains(&format!("event={event}")))
        .cloned()
}

fn query_value(line: &str, key: &str) -> Option<String> {
    let query = line.split_whitespace().nth(1)?.split_once('?')?.1;
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(&format!("{key}=")).map(str::to_string))
}

fn announce_frame(trackers: Vec<String>) -> Frame {
    Frame::Announce {
        info_hash: IH,
        reserved: RESERVED,
        peer_id: PEER_ID,
        trackers,
    }
}

async fn inbound_server(tune: impl FnOnce(&mut TunnelSettings)) -> Server {
    serve(true, |s| {
        s.inbound_addr = Some("127.0.0.1:0".parse().unwrap());
        s.handshake_timeout = Duration::from_millis(800);
        tune(s);
    })
    .await
}

/// Announce on `t` through `tracker`: the inbound port the gateway announced.
async fn announce_and_port(t: &mut Client, tracker: &str, seen: &Mutex<Vec<String>>) -> u16 {
    t.send(&[announce_frame(vec![tracker.to_string()])]).await;
    t.wait_for(
        |fs| fs.iter().any(|f| matches!(f, Frame::Announced { .. })),
        5_000,
    )
    .await;
    assert!(t.frames.contains(&Frame::Announced {
        info_hash: IH,
        status: super::codec::announce_status::ACTIVE
    }));
    wait_until(|| announced(seen, "started").is_some(), 5_000).await;
    let line = announced(seen, "started").unwrap();
    assert_eq!(
        query_value(&line, "left").as_deref(),
        Some("1"),
        "a leecher"
    );
    query_value(&line, "port").unwrap().parse().unwrap()
}

/// Connect to the gateway's inbound port and send a handshake for `ih`.
async fn dial_in(port: u16, ih: [u8; 20]) -> TcpStream {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let hs = encode_handshake_with(
        &[0, 0, 0, 0, 0, 0x10, 0, 0x05],
        &ih,
        b"-IN0001-inboundpeer0",
    );
    s.write_all(&hs).await.unwrap();
    s
}

/// Bytes until EOF or `ms` (the connection's fate).
async fn read_until_closed(s: &mut TcpStream, ms: u64) -> Option<Vec<u8>> {
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    loop {
        match tokio::time::timeout_at(deadline, s.read(&mut buf)).await {
            Err(_) => return None, // still open
            Ok(Ok(0)) | Ok(Err(_)) => return Some(got),
            Ok(Ok(n)) => got.extend_from_slice(&buf[..n]),
        }
    }
}

async fn refused(port: u16) -> bool {
    TcpStream::connect(("127.0.0.1", port)).await.is_err()
}

#[tokio::test]
async fn nothing_is_announced_or_listened_without_a_request() {
    let (tracker, seen) = recording_tracker().await;
    let echo = peer(Behaviour::Echo).await;
    let server = inbound_server(|_| {}).await;
    let mut t = Client::connect(&server, None).await;
    t.send(&[open(1, echo.addr.port())]).await;
    assert!(matches!(t.outcome(1).await, Frame::Opened { .. }));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(seen.lock().unwrap().is_empty());
    // A gateway without an inbound address answers UNSUPPORTED.
    let plain = serve(true, |_| {}).await;
    let mut u = Client::connect(&plain, None).await;
    u.send(&[announce_frame(vec![tracker])]).await;
    u.wait_for(
        |fs| fs.iter().any(|f| matches!(f, Frame::Announced { .. })),
        5_000,
    )
    .await;
    assert_eq!(
        u.frames[0],
        Frame::Announced {
            info_hash: IH,
            status: super::codec::announce_status::UNSUPPORTED
        }
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_inbound_peer_is_accepted_and_relayed_both_ways() {
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|_| {}).await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    let mut remote = dial_in(port, IH).await;
    remote.write_all(&HAVE_ALL).await.unwrap();
    // The gateway answers with the session's handshake.
    let mut theirs = [0u8; HANDSHAKE_LEN];
    remote.read_exact(&mut theirs).await.unwrap();
    assert_eq!(theirs, encode_handshake_with(&RESERVED, &IH, &PEER_ID));
    t.wait_for(
        |fs| fs.iter().any(|f| matches!(f, Frame::Accept { .. })),
        5_000,
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
    assert_eq!(peer.info_hash, IH);
    assert_eq!(peer.peer_id, *b"-IN0001-inboundpeer0");
    assert_eq!(peer.reserved, [0, 0, 0, 0, 0, 0x10, 0, 0x05]);
    assert_eq!(peer.addr.ip().to_string(), "127.0.0.1");
    // Bytes that came with its handshake, then both directions.
    t.wait_for(
        |fs| fs.iter().any(|f| matches!(f, Frame::Data { .. })),
        5_000,
    )
    .await;
    assert_eq!(t.data(conn), HAVE_ALL);
    t.send(&[data(conn, b"hello peer")]).await;
    let mut got = [0u8; 10];
    remote.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"hello peer");
    remote.write_all(b"hello browser").await.unwrap();
    let mut want = HAVE_ALL.to_vec();
    want.extend_from_slice(b"hello browser");
    let len = want.len();
    t.wait_for(|fs| data_len(fs, conn) >= len, 5_000).await;
    assert_eq!(t.data(conn), want);
    // The browser's close reaches the peer.
    t.send(&[Frame::Close {
        conn,
        reason: close_reason::NORMAL,
    }])
    .await;
    assert_eq!(read_until_closed(&mut remote, 5_000).await, Some(vec![]));
}

fn data_len(fs: &[Frame], conn: u16) -> usize {
    fs.iter()
        .filter_map(|f| match f {
            Frame::Data { conn: c, payload } if *c == conn => Some(payload.len()),
            _ => None,
        })
        .sum()
}

#[tokio::test]
async fn an_inbound_peer_for_another_torrent_or_protocol_gets_no_reply() {
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|_| {}).await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    // Another info-hash: closed without a byte.
    let mut other = dial_in(port, [0x5a; 20]).await;
    assert_eq!(read_until_closed(&mut other, 5_000).await, Some(vec![]));
    // Not BitTorrent: closed without a byte.
    let mut http = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    http.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    assert_eq!(read_until_closed(&mut http, 5_000).await, Some(vec![]));
    // Silent: closed at the handshake timeout, without a byte.
    let mut silent = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(read_until_closed(&mut silent, 5_000).await, Some(vec![]));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!t.frames.iter().any(|f| matches!(f, Frame::Accept { .. })));
}

#[tokio::test]
async fn the_end_of_the_session_stops_the_announce_and_closes_the_listener() {
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|_| {}).await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    let mut remote = dial_in(port, IH).await;
    t.wait_for(
        |fs| fs.iter().any(|f| matches!(f, Frame::Accept { .. })),
        5_000,
    )
    .await;
    t.close().await;
    wait_until(|| announced(&seen, "stopped").is_some(), 5_000).await;
    let stopped = announced(&seen, "stopped").unwrap();
    assert_eq!(query_value(&stopped, "port"), Some(port.to_string()));
    // The accepted connection closes with the session, and the port no longer listens.
    let mut buf = [0u8; HANDSHAKE_LEN];
    remote.read_exact(&mut buf).await.unwrap();
    assert_eq!(read_until_closed(&mut remote, 5_000).await, Some(vec![]));
    let mut closed = false;
    for _ in 0..50 {
        if refused(port).await {
            closed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(closed, "listener still open");
}

#[tokio::test]
async fn withdraw_stops_the_announce_and_later_peers_are_refused() {
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|_| {}).await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    t.send(&[Frame::Withdraw { info_hash: IH }]).await;
    t.wait_for(
        |fs| {
            fs.contains(&Frame::Announced {
                info_hash: IH,
                status: super::codec::announce_status::STOPPED,
            })
        },
        5_000,
    )
    .await;
    wait_until(|| announced(&seen, "stopped").is_some(), 5_000).await;
    for _ in 0..50 {
        if refused(port).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("listener still open after withdraw");
}

#[tokio::test]
async fn inbound_connections_are_capped_per_session() {
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|s| s.inbound_per_session = 1).await;
    let mut t = Client::connect(&server, None).await;
    let port = announce_and_port(&mut t, &tracker, &seen).await;
    let mut first = dial_in(port, IH).await;
    let mut buf = [0u8; HANDSHAKE_LEN];
    first.read_exact(&mut buf).await.unwrap();
    let mut second = dial_in(port, IH).await;
    assert_eq!(read_until_closed(&mut second, 5_000).await, Some(vec![]));
    let accepts = t
        .frames
        .iter()
        .filter(|f| matches!(f, Frame::Accept { .. }))
        .count();
    assert!(accepts <= 1);
    // Once the first one is gone, there is room again.
    drop(first);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut third = dial_in(port, IH).await;
    third.read_exact(&mut buf).await.unwrap();
}

#[tokio::test]
async fn announced_info_hashes_are_capped_per_websocket() {
    let (tracker, seen) = recording_tracker().await;
    let server = inbound_server(|s| s.announces_per_socket = 1).await;
    let mut t = Client::connect(&server, None).await;
    announce_and_port(&mut t, &tracker, &seen).await;
    t.send(&[Frame::Announce {
        info_hash: [0x77; 20],
        reserved: RESERVED,
        peer_id: PEER_ID,
        trackers: vec![],
    }])
    .await;
    t.wait_for(
        |fs| {
            fs.contains(&Frame::Announced {
                info_hash: [0x77; 20],
                status: super::codec::announce_status::LIMIT_REACHED,
            })
        },
        5_000,
    )
    .await;
}

#[tokio::test]
async fn announce_withdraw_cycles_run_out_of_rounds() {
    let server = inbound_server(|s| s.announce_rounds_per_ip = 2).await;
    let mut t = Client::connect(&server, None).await;
    let announced = |status| Frame::Announced {
        info_hash: IH,
        status,
    };
    for _ in 0..2 {
        t.frames.clear();
        t.send(&[announce_frame(vec![])]).await;
        t.wait_for(
            |fs| fs.contains(&announced(super::codec::announce_status::ACTIVE)),
            5_000,
        )
        .await;
        t.send(&[Frame::Withdraw { info_hash: IH }]).await;
        t.wait_for(
            |fs| fs.contains(&announced(super::codec::announce_status::STOPPED)),
            5_000,
        )
        .await;
    }
    // The third new swarm within the minute, even from a new WebSocket of
    // the same client: refused.
    let mut u = Client::connect(&server, None).await;
    u.send(&[announce_frame(vec![])]).await;
    u.wait_for(
        |fs| fs.contains(&announced(super::codec::announce_status::LIMIT_REACHED)),
        5_000,
    )
    .await;
}
