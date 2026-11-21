//! Fetching a torrent's info dict from peers with the ut_metadata extension
//! (BEP 9). A result is only accepted if it hashes to the info-hash (SHA-1
//! for v1 and hybrid torrents, SHA-256 truncated to 20 bytes for v2-only
//! ones), so a lying peer can't poison the cache (and the browser checks
//! again). The size a peer announces is checked before anything is
//! allocated, and allocations can be drawn from a shared memory budget.

use crate::bencode::{self, Value};
use crate::peerwire::{self, ExtensionHandshake, Message, PeerConn, WireError};
use bytes::Bytes;
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const BLOCK: usize = 16 * 1024;
/// Largest info dict accepted from a peer.
pub const MAX_METADATA_SIZE: usize = 8 * 1024 * 1024;
/// Unit of the memory budget's permits.
pub const MEMORY_UNIT: usize = 1024;

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
    #[error("too many metadata fetches in flight")]
    Busy,
    #[error("every candidate peer failed ({0} tried)")]
    AllFailed(usize),
    #[error(transparent)]
    Wire(#[from] WireError),
}

/// Whether `bytes` is the info dict of `info_hash`: its SHA-1 (v1, hybrid)
/// or its SHA-256 truncated to 20 bytes (v2-only, as in the handshake).
pub fn matches_info_hash(bytes: &[u8], info_hash: &[u8; 20]) -> bool {
    Sha1::digest(bytes).as_slice() == info_hash || Sha256::digest(bytes)[..20] == info_hash[..]
}

/// A verified info dict, as served by `/metadata`.
#[derive(Debug)]
pub struct InfoDict {
    pub bytes: Bytes,
    /// v1 piece hashes (0 for a v2-only torrent).
    pub num_pieces: u32,
}

impl InfoDict {
    /// `bytes` must already be verified against the info-hash; this only
    /// checks it's a dict with a sane `pieces` string (v1, hybrid) or a
    /// `file tree` with `meta version` 2 (v2-only).
    pub fn parse(bytes: Vec<u8>) -> Result<Self, MetadataError> {
        let value = bencode::parse(&bytes).map_err(|_| MetadataError::InvalidInfo)?;
        let piece_length = value
            .get("piece length")
            .and_then(Value::as_int)
            .unwrap_or(0);
        if piece_length <= 0 {
            return Err(MetadataError::InvalidInfo);
        }
        let num_pieces = match value.get("pieces").and_then(Value::as_bytes) {
            Some(pieces) => {
                if pieces.is_empty() || pieces.len() % 20 != 0 {
                    return Err(MetadataError::InvalidInfo);
                }
                u32::try_from(pieces.len() / 20).map_err(|_| MetadataError::InvalidInfo)?
            }
            None => {
                let v2 = value.get("meta version").and_then(Value::as_int) == Some(2);
                let tree = matches!(value.get("file tree"), Some(Value::Dict(_)));
                if !v2 || !tree {
                    return Err(MetadataError::InvalidInfo);
                }
                0
            }
        };
        Ok(Self {
            bytes: Bytes::from(bytes),
            num_pieces,
        })
    }
}

/// Fetch the metadata from one peer; it must be complete before `deadline`.
/// With `memory`, the buffer is only allocated once its size (in
/// [`MEMORY_UNIT`]s) is granted from that budget, and given back after.
pub async fn fetch_from_peer(
    addr: SocketAddr,
    info_hash: [u8; 20],
    connect_timeout: Duration,
    deadline: Instant,
    memory: Option<Arc<Semaphore>>,
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
    if size == 0 {
        return Err(MetadataError::BadSize(0));
    }
    let blocks = size.div_ceil(BLOCK);
    let _reserved = match memory {
        Some(budget) => {
            let units = u32::try_from(size.div_ceil(MEMORY_UNIT))
                .map_err(|_| MetadataError::BadSize(size as u32))?;
            match tokio::time::timeout_at(deadline.into(), budget.acquire_many_owned(units)).await {
                Ok(Ok(permit)) => Some(permit),
                Ok(Err(_)) => return Err(MetadataError::Busy),
                Err(_) => return Err(MetadataError::Timeout),
            }
        }
        None => None,
    };

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

    if !matches_info_hash(&data, &info_hash) {
        return Err(MetadataError::HashMismatch);
    }
    Ok(data)
}

/// Try `candidates` in order, `parallel` at a time; the first verified
/// result wins and the other attempts are cancelled. (The service in
/// `info.rs` does this with single-flight and shared limits.)
#[cfg_attr(not(test), allow(dead_code))]
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
            attempts.spawn(fetch_from_peer(
                addr,
                info_hash,
                connect_timeout,
                deadline,
                None,
            ));
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

        let fetched = fetch_from_peer(
            addr,
            info_hash,
            Duration::from_millis(500),
            deadline(),
            None,
        )
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
        let err = fetch_from_peer(
            addr,
            info_hash,
            Duration::from_millis(500),
            deadline(),
            None,
        )
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
    fn info_dict_requires_piece_hashes_or_a_v2_file_tree() {
        assert!(InfoDict::parse(b"d4:name1:xe".to_vec()).is_err());
        assert!(InfoDict::parse(b"d12:piece lengthi16384e6:pieces3:abce".to_vec()).is_err());
        let v2 = testpeer::fake_v2_info_dict();
        assert_eq!(InfoDict::parse(v2).unwrap().num_pieces, 0);
        // No file tree, or not version 2: rejected.
        assert!(InfoDict::parse(b"d12:meta versioni2e12:piece lengthi16384ee".to_vec()).is_err());
        assert!(InfoDict::parse(
            b"d9:file treede12:meta versioni1e12:piece lengthi16384ee".to_vec()
        )
        .is_err());
    }

    #[test]
    fn verifies_v1_and_v2_info_hashes() {
        let v1 = testpeer::fake_info_dict(3);
        let sha1: [u8; 20] = Sha1::digest(&v1).into();
        assert!(matches_info_hash(&v1, &sha1));
        let v2 = testpeer::fake_v2_info_dict();
        let sha256 = Sha256::digest(&v2);
        let truncated: [u8; 20] = sha256[..20].try_into().unwrap();
        assert!(matches_info_hash(&v2, &truncated));
        let mut wrong = truncated;
        wrong[0] ^= 1;
        assert!(!matches_info_hash(&v2, &wrong));
        assert!(!matches_info_hash(&v1, &truncated));
    }

    #[tokio::test]
    async fn a_v2_only_info_dict_is_fetched_against_its_truncated_sha256() {
        let info = testpeer::fake_v2_info_dict();
        let ih: [u8; 20] = Sha256::digest(&info)[..20].try_into().unwrap();
        let addr = testpeer::spawn(FakePeer::seed_with_metadata(ih, info.clone())).await;
        let got = fetch_from_peer(addr, ih, Duration::from_millis(500), deadline(), None)
            .await
            .unwrap();
        assert_eq!(got, info);
    }

    #[tokio::test]
    async fn waits_for_its_share_of_the_memory_budget() {
        let info = testpeer::fake_info_dict(1000); // ~20 KiB
        let ih: [u8; 20] = Sha1::digest(&info).into();
        let addr = testpeer::spawn(FakePeer::seed_with_metadata(ih, info.clone())).await;
        // 8 KiB of budget: never enough, the fetch gives up at the deadline.
        let small = Arc::new(Semaphore::new(8));
        let soon = Instant::now() + Duration::from_millis(300);
        let err = fetch_from_peer(
            addr,
            ih,
            Duration::from_millis(500),
            soon,
            Some(small.clone()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, MetadataError::Timeout), "{err:?}");
        let enough = Arc::new(Semaphore::new(64));
        let got = fetch_from_peer(
            addr,
            ih,
            Duration::from_millis(500),
            deadline(),
            Some(enough.clone()),
        )
        .await
        .unwrap();
        assert_eq!(got, info);
        // Given back after.
        assert_eq!(enough.available_permits(), 64);
    }
}
