//! UDP tracker protocol (BEP 15): a connect handshake followed by an
//! announce, both request/response pairs over a single UDP "connection".
//!
//! We deliberately simplify BEP 15's exponential-backoff retry schedule
//! (15s, 30s, 60s, ...) since our whole lookup has a budget of a few seconds
//! total: each round trip gets a couple of quick, growing-timeout attempts,
//! and the caller wraps the whole call in its own hard deadline anyway.

use super::{AnnounceParams, TrackerError};
use crate::netguard;
use rand::random;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout as tokio_timeout;

const PROTOCOL_ID: u64 = 0x0000_0417_2710_1980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_ERROR: u32 = 3;

pub async fn announce(
    url: &url::Url,
    params: &AnnounceParams,
    deadline: Duration,
) -> Result<Vec<SocketAddr>, TrackerError> {
    let host = url
        .host_str()
        .ok_or_else(|| TrackerError::InvalidUrl("missing host".into()))?;
    let port = url
        .port()
        .ok_or_else(|| TrackerError::InvalidUrl("missing port".into()))?;

    let remote = resolve_public(host, port).await?;

    let bind_addr = match remote {
        SocketAddr::V4(_) => "0.0.0.0:0",
        SocketAddr::V6(_) => "[::]:0",
    };
    let socket = UdpSocket::bind(bind_addr)
        .await
        .map_err(|e| TrackerError::Io(e.to_string()))?;
    socket
        .connect(remote)
        .await
        .map_err(|e| TrackerError::Io(e.to_string()))?;

    // Split the budget: connect handshake, then announce.
    let per_attempt = (deadline / 4).max(Duration::from_millis(200));

    let connection_id = do_connect(&socket, per_attempt).await?;
    do_announce(&socket, connection_id, params, per_attempt).await
}

async fn resolve_public(host: &str, port: u16) -> Result<SocketAddr, TrackerError> {
    let mut addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| TrackerError::Dns(e.to_string()))?;
    addrs
        .find(|a| netguard::is_public(a.ip()))
        .ok_or(TrackerError::NoPublicAddress)
}

async fn do_connect(socket: &UdpSocket, per_attempt: Duration) -> Result<u64, TrackerError> {
    let transaction_id: u32 = random();
    let mut req = [0u8; 16];
    req[0..8].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
    req[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    req[12..16].copy_from_slice(&transaction_id.to_be_bytes());

    let mut buf = [0u8; 16];
    for attempt in 0u32..2 {
        socket
            .send(&req)
            .await
            .map_err(|e| TrackerError::Io(e.to_string()))?;
        let attempt_timeout = per_attempt * (attempt + 1);
        match tokio_timeout(attempt_timeout, socket.recv(&mut buf)).await {
            Ok(Ok(n)) if n >= 16 => {
                let action = u32::from_be_bytes(buf[0..4].try_into().unwrap());
                let recv_txn = u32::from_be_bytes(buf[4..8].try_into().unwrap());
                if recv_txn != transaction_id {
                    continue;
                }
                if action == ACTION_ERROR {
                    return Err(TrackerError::TrackerFailure(
                        String::from_utf8_lossy(&buf[8..n]).into_owned(),
                    ));
                }
                if action != ACTION_CONNECT {
                    return Err(TrackerError::Malformed(
                        "unexpected action in connect response".into(),
                    ));
                }
                return Ok(u64::from_be_bytes(buf[8..16].try_into().unwrap()));
            }
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => return Err(TrackerError::Io(e.to_string())),
            Err(_) => continue,
        }
    }
    Err(TrackerError::Timeout)
}

async fn do_announce(
    socket: &UdpSocket,
    connection_id: u64,
    params: &AnnounceParams,
    per_attempt: Duration,
) -> Result<Vec<SocketAddr>, TrackerError> {
    let transaction_id: u32 = random();
    let mut req = [0u8; 98];
    req[0..8].copy_from_slice(&connection_id.to_be_bytes());
    req[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    req[12..16].copy_from_slice(&transaction_id.to_be_bytes());
    req[16..36].copy_from_slice(&params.info_hash);
    req[36..56].copy_from_slice(&params.peer_id);
    // downloaded(56..64)=0, left(64..72)=0, uploaded(72..80)=0, event(80..84)=0=none,
    // ip(84..88)=0 (let the tracker use the packet's source address) are all left zeroed.
    req[88..92].copy_from_slice(&params.key.to_be_bytes());
    req[92..96].copy_from_slice(&params.numwant.to_be_bytes());
    req[96..98].copy_from_slice(&params.port.to_be_bytes());

    // Enough room for a couple hundred compact peers; UDP truncates silently
    // beyond this, which is an acceptable trade-off for a "peer list" API.
    let mut buf = vec![0u8; 4096];
    for attempt in 0u32..2 {
        socket
            .send(&req)
            .await
            .map_err(|e| TrackerError::Io(e.to_string()))?;
        let attempt_timeout = per_attempt * (attempt + 1);
        match tokio_timeout(attempt_timeout, socket.recv(&mut buf)).await {
            Ok(Ok(n)) if n >= 20 => {
                let action = u32::from_be_bytes(buf[0..4].try_into().unwrap());
                let recv_txn = u32::from_be_bytes(buf[4..8].try_into().unwrap());
                if recv_txn != transaction_id {
                    continue;
                }
                if action == ACTION_ERROR {
                    return Err(TrackerError::TrackerFailure(
                        String::from_utf8_lossy(&buf[8..n]).into_owned(),
                    ));
                }
                if action != ACTION_ANNOUNCE {
                    return Err(TrackerError::Malformed(
                        "unexpected action in announce response".into(),
                    ));
                }
                return Ok(parse_compact_peers(&buf[20..n]));
            }
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => return Err(TrackerError::Io(e.to_string())),
            Err(_) => continue,
        }
    }
    Err(TrackerError::Timeout)
}

fn parse_compact_peers(data: &[u8]) -> Vec<SocketAddr> {
    data.chunks_exact(6)
        .map(|c| {
            let ip = std::net::Ipv4Addr::new(c[0], c[1], c[2], c[3]);
            let port = u16::from_be_bytes([c[4], c[5]]);
            SocketAddr::from((ip, port))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compact_peers() {
        let mut data = vec![1, 2, 3, 4, 0x1A, 0xE1];
        data.extend_from_slice(&[5, 6, 7, 8, 0x1A, 0xE2]);
        let peers = parse_compact_peers(&data);
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].to_string(), "1.2.3.4:6881");
        assert_eq!(peers[1].to_string(), "5.6.7.8:6882");
    }

    #[test]
    fn parses_compact_peers_ignores_trailing_partial_entry() {
        let data = vec![1, 2, 3, 4, 0x1A, 0xE1, 9, 9];
        let peers = parse_compact_peers(&data);
        assert_eq!(peers.len(), 1);
    }
}
