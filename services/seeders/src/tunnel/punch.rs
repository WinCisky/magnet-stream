//! The dial of a uTP `open` (M4-04e), made fit for hole punching (M4-05,
//! BEP 55; docs/architecture.md › Chunk recovery › Swarms with few seeders,
//! step 4; docs/decisions/utp.md).
//!
//! In a hole punch the engine asks a relay (a peer connected to the gateway's
//! announced port) to introduce it to a peer behind NAT; the relay tells
//! both sides to connect, and both send a uTP SYN at about the same time.
//! Our first SYN usually dies at the remote's NAT, whose mapping opens only
//! once the remote's own SYN has left towards our port: the connection that
//! comes up may be ours (a retransmitted SYN gets through) or theirs (their
//! SYN reaches our port), and with some clients both do.
//!
//! So a uTP `open` dials from the announced port's socket and, while the
//! connect is pending, holds the multiplexer's acceptor for the remote's
//! exact address (`UtpSocket::expect`, M4-04d): a SYN from that address is
//! this `open`'s, not an inbound peer for the listener. Every connection to
//! the address that completes its uTP handshake gets our BitTorrent
//! handshake; the first one on which the remote's valid handshake arrives
//! (the same gate as TCP, `gate.rs`) is kept and the others are closed
//! (`ST_FIN`). Libtorrent keeps one of two duplicate connections to a peer
//! at the BitTorrent layer, and our own multiplexer keeps the first to
//! complete (M4-04b): either way the remote answers on one connection only,
//! and that is the one we keep, so the two sides cannot end up holding
//! different connections.
//!
//! Bounds: the acceptor lives at most the connect timeout and takes at most
//! [`MAX_ALTERNATES`] connections (the multiplexer's per-address and
//! half-open limits apply too); each connection has the handshake timeout.
//! Nothing else changes for a uTP `open`: the address rules, the session's
//! announce of the info-hash, the slots, the timeouts and the gate are
//! checked as before (`session.rs`). When another `open` already holds the
//! acceptor for the address, the connect alone answers its SYNs while it is
//! pending (the multiplexer's simultaneous open).

use super::codec::fail_reason;
use super::gate::{self, Gate};
use super::stream::PeerStream;
use crate::peerwire::HANDSHAKE_LEN;
use crate::utp::socket::{UtpAcceptor, UtpSocket, UtpStream};
use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;

/// Inbound connections from the remote's address one `open` takes, at most.
pub const MAX_ALTERNATES: usize = 2;

/// A connection that passed the gate: the stream, the peer-wire bytes read
/// past the remote's handshake, its reserved bytes and peer id.
pub type Passed = (PeerStream, Bytes, [u8; 8], [u8; 20]);

/// Write `ours`, then wait (at most `timeout`) for the remote's handshake
/// and check it against `info_hash`; the `failed` reason otherwise.
pub async fn exchange(
    mut stream: PeerStream,
    ours: [u8; HANDSHAKE_LEN],
    info_hash: [u8; 20],
    timeout: Duration,
) -> Result<Passed, u8> {
    let run = async {
        stream
            .write_all(&ours)
            .await
            .map_err(|_| fail_reason::REMOTE_CLOSED)?;
        let mut buf = BytesMut::with_capacity(HANDSHAKE_LEN + 4096);
        loop {
            match stream.read_buf(&mut buf).await {
                Ok(0) | Err(_) => return Err(fail_reason::REMOTE_CLOSED),
                Ok(_) => match gate::check(&buf, &info_hash) {
                    Gate::NeedMore => {
                        if buf.capacity() == buf.len() {
                            buf.reserve(4096);
                        }
                    }
                    Gate::Rejected(reason) => return Err(reason),
                    Gate::Accepted { reserved, peer_id } => {
                        let extra = buf.split_off(HANDSHAKE_LEN).freeze();
                        return Ok((extra, reserved, peer_id));
                    }
                },
            }
        }
    };
    match tokio::time::timeout(timeout, run).await {
        Err(_) => Err(fail_reason::TIMEOUT),
        Ok(Err(reason)) => Err(reason),
        Ok(Ok((extra, reserved, peer_id))) => Ok((stream, extra, reserved, peer_id)),
    }
}

/// The `failed` reason of a uTP connect error (`crate::utp::socket`).
pub fn connect_failure(e: &std::io::Error) -> u8 {
    match e.kind() {
        std::io::ErrorKind::TimedOut => fail_reason::TIMEOUT,
        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionRefused => {
            fail_reason::REFUSED
        }
        // The socket's connection limits.
        _ => fail_reason::INTERNAL_ERROR,
    }
}

type Connecting = Pin<Box<dyn Future<Output = std::io::Result<UtpStream>> + Send>>;

/// Dial `addr` over uTP from `socket` and pass the gate on the first
/// connection to it that answers (see the module docs).
pub async fn dial(
    socket: UtpSocket,
    addr: SocketAddr,
    ours: [u8; HANDSHAKE_LEN],
    info_hash: [u8; 20],
    connect_timeout: Duration,
    handshake_timeout: Duration,
) -> Result<Passed, u8> {
    // Taken before the connect, so no SYN of the remote's slips to the listener.
    let mut gate: Option<UtpAcceptor> = socket.expect(addr).ok();
    let mut connect: Option<Connecting> = Some(Box::pin({
        let socket = socket.clone();
        async move { socket.connect(addr).await }
    }));
    let window = tokio::time::sleep(connect_timeout);
    tokio::pin!(window);
    let mut tries: JoinSet<Result<Passed, u8>> = JoinSet::new();
    let mut alternates = 0;
    // The connect's failure, then the gate's last verdict: the `failed` reason.
    let mut reason = fail_reason::TIMEOUT;
    let mut gated = false;
    let start = |tries: &mut JoinSet<Result<Passed, u8>>, stream: UtpStream| {
        tries.spawn(exchange(
            PeerStream::Utp(stream),
            ours,
            info_hash,
            handshake_timeout,
        ));
    };
    loop {
        if connect.is_none() && gate.is_none() && tries.is_empty() {
            return Err(reason);
        }
        tokio::select! {
            r = async { connect.as_mut().expect("polled only while some").await }, if connect.is_some() => {
                connect = None;
                match r {
                    Ok(stream) => start(&mut tries, stream),
                    Err(e) if !gated => reason = connect_failure(&e),
                    Err(_) => {}
                }
            }
            r = async { gate.as_ref().expect("polled only while some").accept().await }, if gate.is_some() => {
                match r {
                    Ok(stream) => {
                        alternates += 1;
                        start(&mut tries, stream);
                        if alternates >= MAX_ALTERNATES {
                            gate = None;
                        }
                    }
                    Err(_) => gate = None,
                }
            }
            () = &mut window, if connect.is_some() || gate.is_some() => {
                // The SYN exchange is over: a connect still pending times out
                // (dropping it forgets its half-open connection) and the
                // acceptor closes; connections already up keep their gate.
                connect = None;
                gate = None;
            }
            Some(done) = tries.join_next() => {
                match done {
                    // The others are dropped with the set: each sends its ST_FIN.
                    Ok(Ok(passed)) => return Ok(passed),
                    Ok(Err(r)) => {
                        reason = r;
                        gated = true;
                    }
                    Err(_) => {}
                }
            }
        }
    }
}
