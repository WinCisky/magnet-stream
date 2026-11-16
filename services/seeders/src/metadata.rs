//! Fetching a torrent's info dict from peers with the ut_metadata extension
//! (BEP 9). A result is only accepted if its SHA-1 equals the info-hash, so
//! a lying peer can't poison the cache (and the browser checks again).

use crate::bencode::{self, Value};
use crate::peerwire::{self, ExtensionHandshake, Message, PeerConn, WireError};
use bytes::Bytes;
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::task::JoinSet;

const BLOCK: usize = 16 * 1024;
const MAX_METADATA_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Error, Clone)]
pub enum MetadataError {
    #[error("no peers to fetch metadata from")]
    NoPeers,
    #[error("peer does not offer ut_metadata")]
    Unsupported,
    #[error("peer rejected the metadata request")]
    Rejected,
    #[error("peer announced an implausible metadata size ({0} bytes)")]
    BadSize(u32),
    #[error("malformed ut_metadata message")]
    Malformed,
    #[error("metadata does not match the info-hash")]
    HashMismatch,
    #[error("not a valid info dict")]
    InvalidInfo,
    #[error("timed out fetching metadata")]
    Timeout,
    #[error("every candidate peer failed ({0} tried)")]
    AllFailed(usize),
    #[error(transparent)]
    Wire(#[from] WireError),
}

/// A verified info dict, as served by `/metadata`.
#[derive(Debug)]
pub struct InfoDict {
    pub bytes: Bytes,
    pub num_pieces: u32,
}

impl InfoDict {
    /// `bytes` must already be verified against the info-hash; this only
    /// checks it's a dict with a sane `pieces` string.
    pub fn parse(bytes: Vec<u8>) -> Result<Self, MetadataError> {
        let value = bencode::parse(&bytes).map_err(|_| MetadataError::InvalidInfo)?;
        let pieces = value
            .get("pieces")
            .and_then(Value::as_bytes)
            .ok_or(MetadataError::InvalidInfo)?;
        let piece_length = value
            .get("piece length")
            .and_then(Value::as_int)
            .unwrap_or(0);
        if pieces.is_empty() || pieces.len() % 20 != 0 || piece_length <= 0 {
            return Err(MetadataError::InvalidInfo);
        }
        let num_pieces =
            u32::try_from(pieces.len() / 20).map_err(|_| MetadataError::InvalidInfo)?;
        Ok(Self {
            bytes: Bytes::from(bytes),
            num_pieces,
        })
    }
}

/// Fetch the metadata from one peer; it must be complete before `deadline`.
pub async fn fetch_from_peer(
    addr: SocketAddr,
    info_hash: [u8; 20],
    connect_timeout: Duration,
    deadline: Instant,
) -> Result<Vec<u8>, MetadataError> {
    let mut conn = PeerConn::open(addr, info_hash, connect_timeout, deadline).await?;
    if !conn.handshake.supports_extensions() {
        return Err(MetadataError::Unsupported);
    }
    let mut hello = Vec::new();
    if conn.handshake.supports_fast() {
        hello.extend(peerwire::have_none());
    }
    hello.extend(peerwire::extended(0, &peerwire::our_extension_handshake()));
    conn.send(&hello).await?;

    let extensions = loop {
        if let Message::Extended { id: 0, payload } = next(&mut conn, deadline).await? {
            break ExtensionHandshake::parse(&payload).ok_or(MetadataError::Malformed)?;
        }
    };
    let (Some(remote_id), Some(size)) = (extensions.ut_metadata, extensions.metadata_size) else {
        return Err(MetadataError::Unsupported);
    };
    if size as usize > MAX_METADATA_SIZE {
        return Err(MetadataError::BadSize(size));
    }
    let size = size as usize;
    let blocks = size.div_ceil(BLOCK);

    let mut requests = Vec::new();
    for piece in 0..blocks {
        requests.extend(peerwire::extended(remote_id, &request_message(piece)));
    }
    conn.send(&requests).await?;

    let mut data = vec![0u8; size];
    let mut received = vec![false; blocks];
    let mut missing = blocks;
    while missing > 0 {
        let Message::Extended { id, payload } = next(&mut conn, deadline).await? else {
            continue;
        };
        if id != peerwire::UT_METADATA_LOCAL_ID {
            continue;
        }
        let (header, consumed) =
            bencode::parse_prefix(&payload).map_err(|_| MetadataError::Malformed)?;
        match header.get("msg_type").and_then(Value::as_int) {
            Some(1) => {
                let piece = header
                    .get("piece")
                    .and_then(Value::as_int)
                    .and_then(|p| usize::try_from(p).ok())
                    .filter(|&p| p < blocks)
                    .ok_or(MetadataError::Malformed)?;
                let start = piece * BLOCK;
                let block = &payload[consumed..];
                if block.len() != BLOCK.min(size - start) {
                    return Err(MetadataError::Malformed);
                }
                if !received[piece] {
                    data[start..start + block.len()].copy_from_slice(block);
                    received[piece] = true;
                    missing -= 1;
                }
            }
            Some(2) => return Err(MetadataError::Rejected),
            // A request from the peer (msg_type 0): we have nothing to give.
            _ => {}
        }
    }

    if Sha1::digest(&data).as_slice() != info_hash {
        return Err(MetadataError::HashMismatch);
    }
    Ok(data)
}

/// Try `candidates` in order, `parallel` at a time; the first verified
/// result wins and the other attempts are cancelled.
pub async fn fetch_from_any(
    candidates: Vec<SocketAddr>,
    info_hash: [u8; 20],
    parallel: usize,
    connect_timeout: Duration,
    deadline: Instant,
) -> Result<Vec<u8>, MetadataError> {
    if candidates.is_empty() {
        return Err(MetadataError::NoPeers);
    }
    let mut pending = candidates.into_iter();
    let mut attempts = JoinSet::new();
    let mut spawn_next = |attempts: &mut JoinSet<_>| {
        if let Some(addr) = pending.next() {
            attempts.spawn(fetch_from_peer(addr, info_hash, connect_timeout, deadline));
        }
    };
    for _ in 0..parallel.max(1) {
        spawn_next(&mut attempts);
    }

    let mut failed = 0;
    loop {
        let joined = tokio::time::timeout_at(deadline.into(), attempts.join_next()).await;
        match joined {
            Err(_) => return Err(MetadataError::Timeout),
            Ok(None) => return Err(MetadataError::AllFailed(failed)),
            Ok(Some(Ok(Ok(data)))) => return Ok(data),
            Ok(Some(_)) => {
                failed += 1;
                spawn_next(&mut attempts);
            }
        }
    }
}

fn request_message(piece: usize) -> Vec<u8> {
    bencode::encode(&Value::Dict(vec![
        (b"msg_type".to_vec(), Value::Int(0)),
        (b"piece".to_vec(), Value::Int(piece as i64)),
    ]))
}

async fn next(conn: &mut PeerConn, deadline: Instant) -> Result<Message, MetadataError> {
    conn.next_message(deadline).await.map_err(|e| match e {
        WireError::Timeout => MetadataError::Timeout,
        other => MetadataError::Wire(other),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testpeer::{self, FakePeer};

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(3)
    }

    #[tokio::test]
    async fn fetches_and_verifies_multi_block_metadata() {
        let info = testpeer::fake_info_dict(3000);
        assert!(info.len() > 2 * BLOCK, "test should span several blocks");
        let info_hash: [u8; 20] = Sha1::digest(&info).into();
        let addr = testpeer::spawn(FakePeer::seed_with_metadata(info_hash, info.clone())).await;

        let fetched = fetch_from_peer(addr, info_hash, Duration::from_millis(500), deadline())
            .await
            .unwrap();
        assert_eq!(fetched, info);

        let parsed = InfoDict::parse(fetched).unwrap();
        assert_eq!(parsed.num_pieces, 3000);
    }

    #[tokio::test]
    async fn rejects_metadata_that_does_not_hash_to_the_info_hash() {
        let info = testpeer::fake_info_dict(10);
        let info_hash: [u8; 20] = Sha1::digest(&info).into();
        let peer = FakePeer {
            corrupt_metadata: true,
            ..FakePeer::seed_with_metadata(info_hash, info)
        };
        let addr = testpeer::spawn(peer).await;
        let err = fetch_from_peer(addr, info_hash, Duration::from_millis(500), deadline())
            .await
            .unwrap_err();
        assert!(matches!(err, MetadataError::HashMismatch), "{err:?}");
    }

    #[tokio::test]
    async fn fetch_from_any_skips_bad_peers() {
        let info = testpeer::fake_info_dict(50);
        let info_hash: [u8; 20] = Sha1::digest(&info).into();
        let without_metadata = testpeer::spawn(FakePeer::seed(info_hash)).await;
        let corrupt = testpeer::spawn(FakePeer {
            corrupt_metadata: true,
            ..FakePeer::seed_with_metadata(info_hash, info.clone())
        })
        .await;
        let good = testpeer::spawn(FakePeer::seed_with_metadata(info_hash, info.clone())).await;

        let fetched = fetch_from_any(
            vec![without_metadata, corrupt, good],
            info_hash,
            1,
            Duration::from_millis(500),
            deadline(),
        )
        .await
        .unwrap();
        assert_eq!(fetched, info);
    }

    #[tokio::test]
    async fn fetch_from_any_with_no_candidates() {
        let err = fetch_from_any(vec![], [0u8; 20], 3, Duration::from_millis(100), deadline())
            .await
            .unwrap_err();
        assert!(matches!(err, MetadataError::NoPeers));
    }

    #[test]
    fn info_dict_requires_piece_hashes() {
        assert!(InfoDict::parse(b"d4:name1:xe".to_vec()).is_err());
        assert!(InfoDict::parse(b"d12:piece lengthi16384e6:pieces3:abce".to_vec()).is_err());
    }
}
