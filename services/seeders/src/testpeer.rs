//! A scriptable fake BitTorrent peer on 127.0.0.1, for tests.

use crate::bencode::{self, Value};
use crate::peerwire::{self, HANDSHAKE_LEN};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const FAKE_UT_METADATA_ID: u8 = 3;

#[derive(Clone)]
pub enum FakeAvailability {
    Everything,
    Bitfield(Vec<u8>),
    Silent,
}

#[derive(Clone)]
pub struct FakePeer {
    pub info_hash: [u8; 20],
    pub fast: bool,
    pub extensions: bool,
    pub availability: FakeAvailability,
    pub haves: Vec<u32>,
    pub unchoke: bool,
    pub reqq: Option<u32>,
    pub metadata: Option<Vec<u8>>,
    pub corrupt_metadata: bool,
    /// ut_metadata requests for the first block (one per fetch), shared by clones.
    pub metadata_fetches: Arc<AtomicUsize>,
}

impl FakePeer {
    pub fn seed(info_hash: [u8; 20]) -> Self {
        Self {
            info_hash,
            fast: true,
            extensions: true,
            availability: FakeAvailability::Everything,
            haves: Vec::new(),
            unchoke: true,
            reqq: Some(500),
            metadata: None,
            corrupt_metadata: false,
            metadata_fetches: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn seed_with_metadata(info_hash: [u8; 20], metadata: Vec<u8>) -> Self {
        Self {
            metadata: Some(metadata),
            ..Self::seed(info_hash)
        }
    }
}

pub async fn spawn(peer: FakePeer) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let peer = peer.clone();
            tokio::spawn(async move {
                let _ = serve(stream, peer).await;
            });
        }
    });
    addr
}

async fn serve(mut stream: TcpStream, peer: FakePeer) -> std::io::Result<()> {
    let mut theirs = [0u8; HANDSHAKE_LEN];
    stream.read_exact(&mut theirs).await?;

    let mut ours = peerwire::encode_handshake(&peer.info_hash, &[9u8; 20]);
    ours[25] = if peer.extensions { 0x10 } else { 0 };
    ours[27] = if peer.fast { 0x04 } else { 0 };
    stream.write_all(&ours).await?;

    if peer.extensions {
        let mut m = Vec::new();
        let mut dict = Vec::new();
        if peer.metadata.is_some() {
            m.push((
                b"ut_metadata".to_vec(),
                Value::Int(i64::from(FAKE_UT_METADATA_ID)),
            ));
        }
        dict.push((b"m".to_vec(), Value::Dict(m)));
        if let Some(metadata) = &peer.metadata {
            dict.push((b"metadata_size".to_vec(), Value::Int(metadata.len() as i64)));
        }
        if let Some(reqq) = peer.reqq {
            dict.push((b"reqq".to_vec(), Value::Int(i64::from(reqq))));
        }
        dict.push((b"v".to_vec(), Value::Bytes(b"FakePeer/1.0".to_vec())));
        stream
            .write_all(&peerwire::extended(0, &bencode::encode(&Value::Dict(dict))))
            .await?;
    }

    match &peer.availability {
        FakeAvailability::Everything if peer.fast => {
            stream
                .write_all(&peerwire::frame(peerwire::id::HAVE_ALL, &[]))
                .await?
        }
        FakeAvailability::Everything => {
            stream
                .write_all(&peerwire::frame(peerwire::id::BITFIELD, &[0xFF; 4]))
                .await?
        }
        FakeAvailability::Bitfield(bits) => {
            stream
                .write_all(&peerwire::frame(peerwire::id::BITFIELD, bits))
                .await?
        }
        FakeAvailability::Silent => {}
    }
    for have in &peer.haves {
        stream
            .write_all(&peerwire::frame(peerwire::id::HAVE, &have.to_be_bytes()))
            .await?;
    }

    loop {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len == 0 {
            continue;
        }
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await?;
        match body[0] {
            peerwire::id::INTERESTED if peer.unchoke => {
                stream
                    .write_all(&peerwire::frame(peerwire::id::UNCHOKE, &[]))
                    .await?
            }
            peerwire::id::EXTENDED if body.get(1) == Some(&FAKE_UT_METADATA_ID) => {
                let Some(metadata) = &peer.metadata else {
                    continue;
                };
                let (request, _) = bencode::parse_prefix(&body[2..]).unwrap();
                let piece = request.get("piece").and_then(Value::as_int).unwrap() as usize;
                if piece == 0 {
                    peer.metadata_fetches.fetch_add(1, Ordering::SeqCst);
                }
                let start = piece * 16 * 1024;
                let end = (start + 16 * 1024).min(metadata.len());
                let mut block = metadata[start..end].to_vec();
                if peer.corrupt_metadata {
                    block[0] ^= 0xFF;
                }
                let mut payload = bencode::encode(&Value::Dict(vec![
                    (b"msg_type".to_vec(), Value::Int(1)),
                    (b"piece".to_vec(), Value::Int(piece as i64)),
                    (b"total_size".to_vec(), Value::Int(metadata.len() as i64)),
                ]));
                payload.extend(block);
                stream
                    .write_all(&peerwire::extended(
                        peerwire::UT_METADATA_LOCAL_ID,
                        &payload,
                    ))
                    .await?;
            }
            _ => {}
        }
    }
}

/// A syntactically valid single-file info dict with `num_pieces` pieces.
pub fn fake_info_dict(num_pieces: usize) -> Vec<u8> {
    let pieces: Vec<u8> = (0..num_pieces * 20).map(|i| (i % 251) as u8).collect();
    bencode::encode(&Value::Dict(vec![
        (b"length".to_vec(), Value::Int(num_pieces as i64 * 16384)),
        (b"name".to_vec(), Value::Bytes(b"fake.bin".to_vec())),
        (b"piece length".to_vec(), Value::Int(16384)),
        (b"pieces".to_vec(), Value::Bytes(pieces)),
    ]))
}

/// A minimal v2-only info dict (BEP 52): one file in a `file tree`.
pub fn fake_v2_info_dict() -> Vec<u8> {
    let root = vec![7u8; 32];
    let file = Value::Dict(vec![(
        b"".to_vec(),
        Value::Dict(vec![
            (b"length".to_vec(), Value::Int(16384)),
            (b"pieces root".to_vec(), Value::Bytes(root)),
        ]),
    )]);
    bencode::encode(&Value::Dict(vec![
        (
            b"file tree".to_vec(),
            Value::Dict(vec![(b"v2.bin".to_vec(), file)]),
        ),
        (b"meta version".to_vec(), Value::Int(2)),
        (b"name".to_vec(), Value::Bytes(b"v2.bin".to_vec())),
        (b"piece length".to_vec(), Value::Int(16384)),
    ]))
}
