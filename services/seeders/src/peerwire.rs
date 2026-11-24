//! Minimal BitTorrent peer-wire client (BEP 3), with just enough of the
//! extension protocol (BEP 10) and the fast extension (BEP 6) to probe a peer
//! and fetch metadata (BEP 9). It never requests or serves piece data.
//!
//! Peers are untrusted: every read has a deadline and every message has a
//! size cap. After a `Timeout` the connection may be mid-message, so callers
//! must drop it rather than keep reading.

use crate::bencode::{self, Value};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

pub const HANDSHAKE_LEN: usize = 68;
pub const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";

/// Fits the bitfield of any sane torrent and a 16 KiB ut_metadata block;
/// anything bigger is a broken or hostile peer.
pub const MAX_MESSAGE_LEN: usize = 1 << 20;

/// The message id we ask peers to use for ut_metadata (our extension
/// handshake's `m` dict).
pub const UT_METADATA_LOCAL_ID: u8 = 1;

pub mod id {
    pub const CHOKE: u8 = 0;
    pub const UNCHOKE: u8 = 1;
    pub const INTERESTED: u8 = 2;
    pub const HAVE: u8 = 4;
    pub const BITFIELD: u8 = 5;
    pub const HAVE_ALL: u8 = 0x0E;
    pub const HAVE_NONE: u8 = 0x0F;
    pub const EXTENDED: u8 = 20;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WireError {
    #[error("connect timed out")]
    ConnectTimeout,
    #[error("connect failed: {0}")]
    ConnectFailed(String),
    #[error("timed out")]
    Timeout,
    #[error("peer closed the connection")]
    Closed,
    #[error("not a BitTorrent handshake")]
    BadHandshake,
    #[error("peer is not serving this info-hash")]
    InfoHashMismatch,
    #[error("message of {0} bytes exceeds the limit")]
    TooLarge(usize),
    #[error("i/o error: {0}")]
    Io(String),
}

impl WireError {
    /// Short, stable code for API responses.
    pub fn code(&self) -> &'static str {
        match self {
            WireError::ConnectTimeout => "connect_timeout",
            WireError::ConnectFailed(_) => "connect_failed",
            WireError::Timeout => "timeout",
            WireError::Closed => "closed",
            WireError::BadHandshake => "bad_handshake",
            WireError::InfoHashMismatch => "infohash_mismatch",
            WireError::TooLarge(_) => "protocol",
            WireError::Io(_) => "io",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
}

impl Handshake {
    pub fn supports_extensions(&self) -> bool {
        self.reserved[5] & 0x10 != 0
    }

    pub fn supports_fast(&self) -> bool {
        self.reserved[7] & 0x04 != 0
    }
}

pub fn encode_handshake(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> [u8; HANDSHAKE_LEN] {
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10; // extension protocol (BEP 10)
    reserved[7] |= 0x04; // fast extension (BEP 6)
    encode_handshake_with(&reserved, info_hash, peer_id)
}

/// A handshake with the caller's reserved bytes (the tunnel gateway writes
/// the browser's, which knows which extensions it speaks).
pub fn encode_handshake_with(
    reserved: &[u8; 8],
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> [u8; HANDSHAKE_LEN] {
    let mut buf = [0u8; HANDSHAKE_LEN];
    buf[0] = PROTOCOL.len() as u8;
    buf[1..20].copy_from_slice(PROTOCOL);
    buf[20..28].copy_from_slice(reserved);
    buf[28..48].copy_from_slice(info_hash);
    buf[48..68].copy_from_slice(peer_id);
    buf
}

pub fn parse_handshake(buf: &[u8; HANDSHAKE_LEN]) -> Result<Handshake, WireError> {
    if buf[0] as usize != PROTOCOL.len() || &buf[1..20] != PROTOCOL {
        return Err(WireError::BadHandshake);
    }
    let mut reserved = [0u8; 8];
    reserved.copy_from_slice(&buf[20..28]);
    let mut info_hash = [0u8; 20];
    info_hash.copy_from_slice(&buf[28..48]);
    Ok(Handshake {
        reserved,
        info_hash,
    })
}

pub fn random_peer_id() -> [u8; 20] {
    let mut peer_id = [0u8; 20];
    peer_id[..8].copy_from_slice(b"-MS0020-");
    rand::Rng::fill(&mut rand::thread_rng(), &mut peer_id[8..]);
    peer_id
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Have(u32),
    Bitfield(Vec<u8>),
    HaveAll,
    HaveNone,
    Extended {
        id: u8,
        payload: Vec<u8>,
    },
    /// Anything else (requests, pieces, reject, allowed-fast, ...): nothing
    /// a probe or metadata fetch needs.
    Other(u8),
}

impl Message {
    fn decode(id: u8, mut payload: Vec<u8>) -> Message {
        match id {
            id::CHOKE => Message::Choke,
            id::UNCHOKE => Message::Unchoke,
            id::HAVE if payload.len() == 4 => Message::Have(u32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ])),
            id::BITFIELD => Message::Bitfield(payload),
            id::HAVE_ALL => Message::HaveAll,
            id::HAVE_NONE => Message::HaveNone,
            id::EXTENDED if !payload.is_empty() => {
                let rest = payload.split_off(1);
                Message::Extended {
                    id: payload[0],
                    payload: rest,
                }
            }
            other => Message::Other(other),
        }
    }
}

/// A length-prefixed message: `<len><id><payload>`.
pub fn frame(id: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.extend_from_slice(&((payload.len() + 1) as u32).to_be_bytes());
    out.push(id);
    out.extend_from_slice(payload);
    out
}

pub fn interested() -> Vec<u8> {
    frame(id::INTERESTED, &[])
}

/// With the fast extension on both sides, BEP 6 requires the first message
/// after the handshake to be bitfield, have-all or have-none.
pub fn have_none() -> Vec<u8> {
    frame(id::HAVE_NONE, &[])
}

pub fn extended(ext_id: u8, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(1 + payload.len());
    body.push(ext_id);
    body.extend_from_slice(payload);
    frame(id::EXTENDED, &body)
}

/// Our BEP 10 handshake: the only extension we speak is ut_metadata.
pub fn our_extension_handshake() -> Vec<u8> {
    let client = concat!("magnet-seeders/", env!("CARGO_PKG_VERSION"));
    bencode::encode(&Value::Dict(vec![
        (
            b"m".to_vec(),
            Value::Dict(vec![(
                b"ut_metadata".to_vec(),
                Value::Int(i64::from(UT_METADATA_LOCAL_ID)),
            )]),
        ),
        (b"v".to_vec(), Value::Bytes(client.as_bytes().to_vec())),
    ]))
}

/// The fields we care about from a peer's BEP 10 handshake.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtensionHandshake {
    pub client: Option<String>,
    pub reqq: Option<u32>,
    pub ut_metadata: Option<u8>,
    pub metadata_size: Option<u32>,
}

impl ExtensionHandshake {
    pub fn parse(payload: &[u8]) -> Option<Self> {
        let value = bencode::parse(payload).ok()?;
        value.as_dict()?;
        let positive_u32 = |key: &str| {
            value
                .get(key)
                .and_then(Value::as_int)
                .and_then(|n| u32::try_from(n).ok())
                .filter(|&n| n > 0)
        };
        Some(Self {
            client: value.get("v").and_then(Value::as_bytes).map(|b| {
                String::from_utf8_lossy(b)
                    .chars()
                    .filter(|c| !c.is_control())
                    .take(64)
                    .collect()
            }),
            reqq: positive_u32("reqq"),
            // An id of 0 means the peer disabled the extension.
            ut_metadata: value
                .get("m")
                .and_then(|m| m.get("ut_metadata"))
                .and_then(Value::as_int)
                .and_then(|n| u8::try_from(n).ok())
                .filter(|&n| n != 0),
            metadata_size: positive_u32("metadata_size"),
        })
    }
}

/// An open, handshaken connection to one peer.
pub struct PeerConn {
    stream: BufReader<TcpStream>,
    pub handshake: Handshake,
    pub connect_ms: u32,
}

impl PeerConn {
    /// Connect, send our handshake, and read and check the peer's, which
    /// must arrive before `deadline`.
    pub async fn open(
        addr: SocketAddr,
        info_hash: [u8; 20],
        connect_timeout: Duration,
        deadline: Instant,
    ) -> Result<Self, WireError> {
        let started = Instant::now();
        let connect_budget = connect_timeout.min(deadline.saturating_duration_since(started));
        let stream = match timeout(connect_budget, TcpStream::connect(addr)).await {
            Err(_) => return Err(WireError::ConnectTimeout),
            Ok(Err(e)) => return Err(WireError::ConnectFailed(e.kind().to_string())),
            Ok(Ok(stream)) => stream,
        };
        let connect_ms = elapsed_ms(started);
        let _ = stream.set_nodelay(true);
        let mut stream = BufReader::new(stream);

        let handshake = encode_handshake(&info_hash, &random_peer_id());
        let mut reply = [0u8; HANDSHAKE_LEN];
        let exchange = async {
            stream
                .get_mut()
                .write_all(&handshake)
                .await
                .map_err(io_error)?;
            stream.read_exact(&mut reply).await.map_err(read_error)?;
            Ok::<_, WireError>(())
        };
        match timeout(deadline.saturating_duration_since(Instant::now()), exchange).await {
            Err(_) => return Err(WireError::Timeout),
            Ok(result) => result?,
        }

        let handshake = parse_handshake(&reply)?;
        if handshake.info_hash != info_hash {
            return Err(WireError::InfoHashMismatch);
        }
        Ok(Self {
            stream,
            handshake,
            connect_ms,
        })
    }

    pub async fn send(&mut self, bytes: &[u8]) -> Result<(), WireError> {
        self.stream
            .get_mut()
            .write_all(bytes)
            .await
            .map_err(io_error)
    }

    /// The next message, or `Timeout` once `deadline` passes.
    pub async fn next_message(&mut self, deadline: Instant) -> Result<Message, WireError> {
        match timeout(
            deadline.saturating_duration_since(Instant::now()),
            self.read_message(),
        )
        .await
        {
            Err(_) => Err(WireError::Timeout),
            Ok(result) => result,
        }
    }

    async fn read_message(&mut self) -> Result<Message, WireError> {
        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len).await.map_err(read_error)?;
        let len = u32::from_be_bytes(len) as usize;
        if len == 0 {
            return Ok(Message::KeepAlive);
        }
        if len > MAX_MESSAGE_LEN {
            return Err(WireError::TooLarge(len));
        }
        let mut body = vec![0u8; len];
        self.stream
            .read_exact(&mut body)
            .await
            .map_err(read_error)?;
        let payload = body.split_off(1);
        Ok(Message::decode(body[0], payload))
    }
}

pub fn elapsed_ms(since: Instant) -> u32 {
    u32::try_from(since.elapsed().as_millis()).unwrap_or(u32::MAX)
}

fn io_error(e: std::io::Error) -> WireError {
    WireError::Io(e.kind().to_string())
}

fn read_error(e: std::io::Error) -> WireError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        WireError::Closed
    } else {
        io_error(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_round_trips_with_our_extension_bits() {
        let info_hash = [7u8; 20];
        let encoded = encode_handshake(&info_hash, &random_peer_id());
        let parsed = parse_handshake(&encoded).unwrap();
        assert_eq!(parsed.info_hash, info_hash);
        assert!(parsed.supports_extensions());
        assert!(parsed.supports_fast());
        assert_eq!(&encoded[48..56], b"-MS0020-");
    }

    #[test]
    fn rejects_non_bittorrent_handshake() {
        let mut buf = [0u8; HANDSHAKE_LEN];
        buf[0] = 19;
        buf[1..20].copy_from_slice(b"HTTP/1.1 200 OK\r\n\r\n");
        assert_eq!(parse_handshake(&buf).unwrap_err(), WireError::BadHandshake);
    }

    #[test]
    fn frames_messages() {
        assert_eq!(interested(), vec![0, 0, 0, 1, 2]);
        assert_eq!(have_none(), vec![0, 0, 0, 1, 0x0F]);
        assert_eq!(extended(3, b"xy"), vec![0, 0, 0, 4, 20, 3, b'x', b'y']);
    }

    #[test]
    fn decodes_messages() {
        assert_eq!(Message::decode(4, vec![0, 0, 1, 2]), Message::Have(258));
        assert_eq!(
            Message::decode(5, vec![0xF0]),
            Message::Bitfield(vec![0xF0])
        );
        assert_eq!(
            Message::decode(20, vec![0, b'd', b'e']),
            Message::Extended {
                id: 0,
                payload: b"de".to_vec()
            }
        );
        assert_eq!(Message::decode(7, vec![0; 20]), Message::Other(7));
        // A malformed have is not trusted as one.
        assert_eq!(Message::decode(4, vec![1]), Message::Other(4));
    }

    #[test]
    fn parses_extension_handshake() {
        let payload = b"d1:md11:ut_metadatai3e6:ut_pexi1ee13:metadata_sizei31235e4:reqqi500e1:v13:qBittorrent/5e";
        let ext = ExtensionHandshake::parse(payload).unwrap();
        assert_eq!(ext.ut_metadata, Some(3));
        assert_eq!(ext.metadata_size, Some(31235));
        assert_eq!(ext.reqq, Some(500));
        assert_eq!(ext.client.as_deref(), Some("qBittorrent/5"));
    }

    #[test]
    fn disabled_or_bogus_extension_fields_are_ignored() {
        let payload = b"d1:md11:ut_metadatai0ee13:metadata_sizei-5e4:reqqi0ee";
        let ext = ExtensionHandshake::parse(payload).unwrap();
        assert_eq!(ext, ExtensionHandshake::default());
        assert!(ExtensionHandshake::parse(b"i1e").is_none());
    }

    #[test]
    fn our_extension_handshake_advertises_ut_metadata() {
        let ext = ExtensionHandshake::parse(&our_extension_handshake()).unwrap();
        assert_eq!(ext.ut_metadata, Some(UT_METADATA_LOCAL_ID));
        assert!(ext.client.unwrap().starts_with("magnet-seeders/"));
    }
}
