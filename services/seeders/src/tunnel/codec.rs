//! Frames of the browser ↔ gateway tunnel (docs/tunnel-protocol.md), the
//! Rust twin of `packages/tunnel-protocol/src/codec.ts`:
//!
//! ```text
//! [type u8][conn u16][length u32][payload: length bytes]   (big-endian)
//! ```
//!
//! One binary WebSocket message carries one or more whole frames. Decoding
//! is explicit binary parsing with bounded work: every length is checked
//! against the bytes present and the limits, and malformed input yields a
//! typed error (the same codes, in the same order of checks, as the
//! TypeScript decoder; the shared vectors in
//! `packages/tunnel-protocol/fixtures` are the contract).

use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// `Sec-WebSocket-Protocol` both sides must agree on.
pub const SUBPROTOCOL: &str = "magnet-tunnel.v1";
pub const HEADER_LEN: usize = 7;
pub const DEFAULT_MAX_PAYLOAD: usize = 1 << 20;
pub const DEFAULT_MAX_FRAMES: usize = 1024;
pub const MAX_PING_PAYLOAD: usize = 8;
/// Client connections (`open`) use 1..=0x7fff; gateway ones (`accept`) 0x8000..=0xffff.
pub const CLIENT_CONN_MAX: u16 = 0x7fff;
pub const GATEWAY_CONN_MIN: u16 = 0x8000;
/// Tracker URLs an `announce` may carry, and the longest one (printable ASCII).
pub const MAX_ANNOUNCE_TRACKERS: usize = 16;
pub const MAX_TRACKER_URL_LEN: usize = 512;

pub mod frame_type {
    pub const OPEN: u8 = 0x01;
    pub const OPENED: u8 = 0x02;
    pub const FAILED: u8 = 0x03;
    pub const DATA: u8 = 0x04;
    pub const CLOSE: u8 = 0x05;
    pub const ACCEPT: u8 = 0x06;
    pub const PING: u8 = 0x07;
    pub const ANNOUNCE: u8 = 0x08;
    pub const ANNOUNCED: u8 = 0x09;
    pub const WITHDRAW: u8 = 0x0a;
}

/// State of a session's announce for one info-hash (`announced`).
pub mod announce_status {
    pub const ACTIVE: u8 = 0;
    pub const STOPPED: u8 = 1;
    pub const UNSUPPORTED: u8 = 2;
    pub const LIMIT_REACHED: u8 = 3;
    pub const INTERNAL_ERROR: u8 = 4;

    pub fn name(status: u8) -> &'static str {
        match status {
            ACTIVE => "active",
            STOPPED => "stopped",
            UNSUPPORTED => "unsupported",
            LIMIT_REACHED => "limit_reached",
            _ => "internal_error",
        }
    }
}

/// Why an `open` did not produce an `opened`.
pub mod fail_reason {
    pub const REFUSED: u8 = 1;
    pub const TIMEOUT: u8 = 2;
    pub const NOT_BITTORRENT: u8 = 3;
    pub const INFOHASH_MISMATCH: u8 = 4;
    pub const FORBIDDEN_ADDRESS: u8 = 5;
    pub const TOO_MANY_CONNECTIONS: u8 = 6;
    pub const CONN_IN_USE: u8 = 7;
    pub const REMOTE_CLOSED: u8 = 8;
    /// Anything else (a uTP connection limit of the gateway).
    pub const INTERNAL_ERROR: u8 = 9;
    /// The `open`'s transport is not available: uTP while the session does
    /// not announce the info-hash, or an unknown transport.
    pub const UNSUPPORTED_TRANSPORT: u8 = 10;

    pub fn name(reason: u8) -> &'static str {
        match reason {
            REFUSED => "refused",
            TIMEOUT => "timeout",
            NOT_BITTORRENT => "not_bittorrent",
            INFOHASH_MISMATCH => "infohash_mismatch",
            FORBIDDEN_ADDRESS => "forbidden_address",
            TOO_MANY_CONNECTIONS => "too_many_connections",
            CONN_IN_USE => "conn_in_use",
            REMOTE_CLOSED => "remote_closed",
            UNSUPPORTED_TRANSPORT => "unsupported_transport",
            _ => "internal_error",
        }
    }
}

/// Transport of an `open` or an `accept`: TCP is encoded by leaving the
/// byte out (the frames of protocol v1 before uTP), any other one as a
/// trailing byte.
pub mod transport {
    pub const TCP: u8 = 0;
    pub const UTP: u8 = 1;

    pub fn name(transport: u8) -> &'static str {
        match transport {
            TCP => "tcp",
            UTP => "utp",
            _ => "unknown",
        }
    }
}

/// Why a connection ended.
pub mod close_reason {
    pub const NORMAL: u8 = 0;
    pub const REMOTE_CLOSED: u8 = 1;
    pub const REMOTE_ERROR: u8 = 2;
    pub const PROTOCOL_ERROR: u8 = 3;
    pub const SESSION_ENDING: u8 = 4;
    pub const INTERNAL_ERROR: u8 = 5;
    pub const IDLE: u8 = 6;
    pub const SESSION_TIMEOUT: u8 = 7;
    pub const WRITE_OVERFLOW: u8 = 8;

    pub fn name(reason: u8) -> &'static str {
        match reason {
            NORMAL => "normal",
            REMOTE_CLOSED => "remote_closed",
            REMOTE_ERROR => "remote_error",
            PROTOCOL_ERROR => "protocol_error",
            SESSION_ENDING => "session_ending",
            IDLE => "idle",
            SESSION_TIMEOUT => "session_timeout",
            WRITE_OVERFLOW => "write_overflow",
            _ => "internal_error",
        }
    }
}

/// WebSocket close codes of a gateway.
pub mod close_code {
    pub const NORMAL: u16 = 1000;
    pub const PROTOCOL_ERROR: u16 = 1002;
    pub const UNSUPPORTED_DATA: u16 = 1003;
    pub const IDLE: u16 = 4000;
    pub const SESSION_TIMEOUT: u16 = 4001;
    pub const SHUTDOWN: u16 = 4002;
}

/// The handshake fields and address of `open` and `accept`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerFields {
    pub info_hash: [u8; 20],
    pub reserved: [u8; 8],
    pub peer_id: [u8; 20],
    /// An IPv4 or IPv6 literal (IPv4-mapped IPv6 stays IPv6) and a port 1..=65535.
    pub addr: SocketAddr,
    /// [`transport`]: TCP unless the frame carries a transport byte.
    pub transport: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Open {
        conn: u16,
        peer: PeerFields,
    },
    Opened {
        conn: u16,
        reserved: [u8; 8],
        peer_id: [u8; 20],
    },
    Failed {
        conn: u16,
        reason: u8,
    },
    Data {
        conn: u16,
        payload: Bytes,
    },
    Close {
        conn: u16,
        reason: u8,
    },
    Accept {
        conn: u16,
        peer: PeerFields,
    },
    Ping {
        payload: Bytes,
    },
    /// Join the swarm of `info_hash` (VPS gateway): announce, accept inbound peers.
    Announce {
        info_hash: [u8; 20],
        reserved: [u8; 8],
        peer_id: [u8; 20],
        /// 0..=16 URLs of 1..=512 printable ASCII bytes (not checked further here).
        trackers: Vec<String>,
    },
    Announced {
        info_hash: [u8; 20],
        status: u8,
    },
    Withdraw {
        info_hash: [u8; 20],
    },
}

impl Frame {
    pub fn type_code(&self) -> u8 {
        match self {
            Frame::Open { .. } => frame_type::OPEN,
            Frame::Opened { .. } => frame_type::OPENED,
            Frame::Failed { .. } => frame_type::FAILED,
            Frame::Data { .. } => frame_type::DATA,
            Frame::Close { .. } => frame_type::CLOSE,
            Frame::Accept { .. } => frame_type::ACCEPT,
            Frame::Ping { .. } => frame_type::PING,
            Frame::Announce { .. } => frame_type::ANNOUNCE,
            Frame::Announced { .. } => frame_type::ANNOUNCED,
            Frame::Withdraw { .. } => frame_type::WITHDRAW,
        }
    }

    pub fn conn(&self) -> u16 {
        match self {
            Frame::Open { conn, .. }
            | Frame::Opened { conn, .. }
            | Frame::Failed { conn, .. }
            | Frame::Data { conn, .. }
            | Frame::Close { conn, .. }
            | Frame::Accept { conn, .. } => *conn,
            Frame::Ping { .. }
            | Frame::Announce { .. }
            | Frame::Announced { .. }
            | Frame::Withdraw { .. } => 0,
        }
    }

    /// Bytes this frame takes in a message.
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.payload_len()
    }

    fn payload_len(&self) -> usize {
        match self {
            Frame::Open { peer, .. } | Frame::Accept { peer, .. } => peer_fields_len(peer),
            Frame::Opened { .. } => 28,
            Frame::Failed { .. } | Frame::Close { .. } => 1,
            Frame::Data { payload, .. } | Frame::Ping { payload } => payload.len(),
            Frame::Announce { trackers, .. } => {
                PEER_FIELDS_LEN + 1 + trackers.iter().map(|t| 2 + t.len()).sum::<usize>()
            }
            Frame::Announced { .. } => 21,
            Frame::Withdraw { .. } => 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameErrorCode {
    UnknownType,
    TooLong,
    Truncated,
    BadPayload,
    BadConn,
    TooManyFrames,
    Empty,
}

impl FrameErrorCode {
    /// The code's name in docs/tunnel-protocol.md (and the TS decoder).
    pub fn as_str(self) -> &'static str {
        match self {
            FrameErrorCode::UnknownType => "unknown_type",
            FrameErrorCode::TooLong => "too_long",
            FrameErrorCode::Truncated => "truncated",
            FrameErrorCode::BadPayload => "bad_payload",
            FrameErrorCode::BadConn => "bad_conn",
            FrameErrorCode::TooManyFrames => "too_many_frames",
            FrameErrorCode::Empty => "empty",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameError {
    pub code: FrameErrorCode,
    /// Byte offset of the offending frame within the message.
    pub offset: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    pub max_payload: usize,
    pub max_frames: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_payload: DEFAULT_MAX_PAYLOAD,
            max_frames: DEFAULT_MAX_FRAMES,
        }
    }
}

/// Which `conn` values each frame type may carry.
pub fn conn_allowed(frame_type: u8, conn: u16) -> bool {
    match frame_type {
        frame_type::PING | frame_type::ANNOUNCE | frame_type::ANNOUNCED | frame_type::WITHDRAW => {
            conn == 0
        }
        frame_type::OPEN => (1..=CLIENT_CONN_MAX).contains(&conn),
        frame_type::ACCEPT => conn >= GATEWAY_CONN_MIN,
        _ => conn != 0,
    }
}

const PEER_FIELDS_LEN: usize = 48;

fn peer_fields_len(peer: &PeerFields) -> usize {
    let transport = usize::from(peer.transport != transport::TCP);
    PEER_FIELDS_LEN + 1 + if peer.addr.is_ipv4() { 4 } else { 16 } + 2 + transport
}

// ------------------------------------------------------------- encoding

/// Encode frames into one WebSocket message. Panics on a frame the protocol
/// forbids (a `conn` outside its range, an empty `data`, a ping over 8 bytes,
/// port 0): those are gateway bugs, never peer input.
pub fn encode_frames(frames: &[Frame]) -> Vec<u8> {
    assert!(!frames.is_empty(), "no frames to encode");
    let mut out = Vec::with_capacity(frames.iter().map(Frame::encoded_len).sum());
    for frame in frames {
        encode_into(frame, &mut out);
    }
    out
}

fn encode_into(frame: &Frame, out: &mut Vec<u8>) {
    let ty = frame.type_code();
    assert!(conn_allowed(ty, frame.conn()), "conn not allowed for type");
    let len = frame.payload_len();
    out.push(ty);
    out.extend_from_slice(&frame.conn().to_be_bytes());
    out.extend_from_slice(&(len as u32).to_be_bytes());
    match frame {
        Frame::Open { peer, .. } | Frame::Accept { peer, .. } => {
            assert!(peer.addr.port() != 0, "port 0");
            out.extend_from_slice(&peer.info_hash);
            out.extend_from_slice(&peer.reserved);
            out.extend_from_slice(&peer.peer_id);
            match peer.addr.ip() {
                IpAddr::V4(v4) => {
                    out.push(4);
                    out.extend_from_slice(&v4.octets());
                }
                IpAddr::V6(v6) => {
                    out.push(6);
                    out.extend_from_slice(&v6.octets());
                }
            }
            out.extend_from_slice(&peer.addr.port().to_be_bytes());
            if peer.transport != transport::TCP {
                out.push(peer.transport);
            }
        }
        Frame::Opened {
            reserved, peer_id, ..
        } => {
            out.extend_from_slice(reserved);
            out.extend_from_slice(peer_id);
        }
        Frame::Failed { reason, .. } | Frame::Close { reason, .. } => out.push(*reason),
        Frame::Data { payload, .. } => {
            assert!(!payload.is_empty(), "empty data frame");
            out.extend_from_slice(payload);
        }
        Frame::Ping { payload } => {
            assert!(payload.len() <= MAX_PING_PAYLOAD, "ping payload too long");
            out.extend_from_slice(payload);
        }
        Frame::Announce {
            info_hash,
            reserved,
            peer_id,
            trackers,
        } => {
            assert!(trackers.len() <= MAX_ANNOUNCE_TRACKERS, "too many trackers");
            out.extend_from_slice(info_hash);
            out.extend_from_slice(reserved);
            out.extend_from_slice(peer_id);
            out.push(trackers.len() as u8);
            for t in trackers {
                assert!(is_tracker_url_text(t.as_bytes()), "bad tracker url");
                out.extend_from_slice(&(t.len() as u16).to_be_bytes());
                out.extend_from_slice(t.as_bytes());
            }
        }
        Frame::Announced { info_hash, status } => {
            out.extend_from_slice(info_hash);
            out.push(*status);
        }
        Frame::Withdraw { info_hash } => out.extend_from_slice(info_hash),
    }
}

/// 1..=512 printable ASCII bytes (0x21..=0x7e): no spaces, no controls.
pub fn is_tracker_url_text(url: &[u8]) -> bool {
    (1..=MAX_TRACKER_URL_LEN).contains(&url.len()) && url.iter().all(|b| (0x21..=0x7e).contains(b))
}

// ------------------------------------------------------------- decoding

/// Decode one WebSocket message: whole frames only. `data` and `ping`
/// payloads are slices of `message` (no copy).
pub fn decode_message(message: &Bytes, limits: DecodeLimits) -> Result<Vec<Frame>, FrameError> {
    if message.is_empty() {
        return Err(FrameError {
            code: FrameErrorCode::Empty,
            offset: 0,
        });
    }
    let err = |code, offset| Err(FrameError { code, offset });
    let buf = &message[..];
    let mut frames = Vec::new();
    let mut at = 0;
    while at < buf.len() {
        let ty = buf[at];
        if !(frame_type::OPEN..=frame_type::WITHDRAW).contains(&ty) {
            return err(FrameErrorCode::UnknownType, at);
        }
        if buf.len() - at < HEADER_LEN {
            return err(FrameErrorCode::Truncated, at);
        }
        let conn = u16::from_be_bytes([buf[at + 1], buf[at + 2]]);
        let len = u32::from_be_bytes([buf[at + 3], buf[at + 4], buf[at + 5], buf[at + 6]]) as usize;
        if len > limits.max_payload {
            return err(FrameErrorCode::TooLong, at);
        }
        if !conn_allowed(ty, conn) {
            return err(FrameErrorCode::BadConn, at);
        }
        if buf.len() - at - HEADER_LEN < len {
            return err(FrameErrorCode::Truncated, at);
        }
        if frames.len() >= limits.max_frames {
            return err(FrameErrorCode::TooManyFrames, at);
        }
        let start = at + HEADER_LEN;
        let payload = message.slice(start..start + len);
        match decode_payload(ty, conn, payload) {
            Some(frame) => frames.push(frame),
            None => return err(FrameErrorCode::BadPayload, at),
        }
        at = start + len;
    }
    Ok(frames)
}

/// The frame, or `None` if its payload has the wrong layout.
fn decode_payload(ty: u8, conn: u16, p: Bytes) -> Option<Frame> {
    match ty {
        frame_type::OPEN => Some(Frame::Open {
            conn,
            peer: read_peer_fields(&p)?,
        }),
        frame_type::ACCEPT => Some(Frame::Accept {
            conn,
            peer: read_peer_fields(&p)?,
        }),
        frame_type::OPENED if p.len() == 28 => Some(Frame::Opened {
            conn,
            reserved: p[..8].try_into().ok()?,
            peer_id: p[8..].try_into().ok()?,
        }),
        frame_type::FAILED if p.len() == 1 => Some(Frame::Failed { conn, reason: p[0] }),
        frame_type::CLOSE if p.len() == 1 => Some(Frame::Close { conn, reason: p[0] }),
        frame_type::DATA if !p.is_empty() => Some(Frame::Data { conn, payload: p }),
        frame_type::PING if p.len() <= MAX_PING_PAYLOAD => Some(Frame::Ping { payload: p }),
        frame_type::ANNOUNCE => read_announce(&p),
        frame_type::ANNOUNCED if p.len() == 21 => Some(Frame::Announced {
            info_hash: p[..20].try_into().ok()?,
            status: p[20],
        }),
        frame_type::WITHDRAW if p.len() == 20 => Some(Frame::Withdraw {
            info_hash: p[..].try_into().ok()?,
        }),
        _ => None,
    }
}

/// `info_hash[20] reserved[8] peer_id[20] count u8 (count × [len u16][url])`.
fn read_announce(p: &[u8]) -> Option<Frame> {
    if p.len() < PEER_FIELDS_LEN + 1 {
        return None;
    }
    let count = p[PEER_FIELDS_LEN] as usize;
    if count > MAX_ANNOUNCE_TRACKERS {
        return None;
    }
    let mut trackers = Vec::with_capacity(count);
    let mut at = PEER_FIELDS_LEN + 1;
    for _ in 0..count {
        if p.len() - at < 2 {
            return None;
        }
        let len = u16::from_be_bytes([p[at], p[at + 1]]) as usize;
        let url = p.get(at + 2..at + 2 + len)?;
        if !is_tracker_url_text(url) {
            return None;
        }
        trackers.push(String::from_utf8(url.to_vec()).ok()?);
        at += 2 + len;
    }
    if at != p.len() {
        return None;
    }
    Some(Frame::Announce {
        info_hash: p[..20].try_into().ok()?,
        reserved: p[20..28].try_into().ok()?,
        peer_id: p[28..48].try_into().ok()?,
        trackers,
    })
}

fn read_peer_fields(p: &[u8]) -> Option<PeerFields> {
    if p.len() < PEER_FIELDS_LEN + 1 {
        return None;
    }
    let family = p[PEER_FIELDS_LEN];
    let ip_len = match family {
        4 => 4,
        6 => 16,
        _ => return None,
    };
    let base = PEER_FIELDS_LEN + 1 + ip_len + 2;
    let transport = match p.len() {
        n if n == base => transport::TCP,
        // One encoding per frame: TCP is the byte left out, never an explicit 0.
        n if n == base + 1 && p[base] != transport::TCP => p[base],
        _ => return None,
    };
    let ip_at = PEER_FIELDS_LEN + 1;
    let port = u16::from_be_bytes([p[ip_at + ip_len], p[ip_at + ip_len + 1]]);
    if port == 0 {
        return None;
    }
    let ip = if family == 4 {
        let o: [u8; 4] = p[ip_at..ip_at + 4].try_into().ok()?;
        IpAddr::V4(Ipv4Addr::from(o))
    } else {
        let o: [u8; 16] = p[ip_at..ip_at + 16].try_into().ok()?;
        IpAddr::V6(Ipv6Addr::from(o))
    };
    Some(PeerFields {
        info_hash: p[..20].try_into().ok()?,
        reserved: p[20..28].try_into().ok()?,
        peer_id: p[28..48].try_into().ok()?,
        addr: SocketAddr::new(ip, port),
        transport,
    })
}

/// `"<reason> in=<n> up=<n> down=<n>"`: the WebSocket close reason with the
/// session's counters (always within the 123-byte limit).
pub fn format_session_end(
    reason: &str,
    messages_in: u64,
    bytes_up: u64,
    bytes_down: u64,
) -> String {
    let ok = !reason.is_empty()
        && reason.len() <= 32
        && reason
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    let reason = if ok { reason } else { "internal_error" };
    // u64 counters fit 20 digits: 32 + 3·(5 + 20) < 123.
    format!("{reason} in={messages_in} up={bytes_up} down={bytes_down}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::path::PathBuf;

    fn fixtures() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../packages/tunnel-protocol/fixtures")
    }

    fn vectors() -> Value {
        let text = std::fs::read_to_string(fixtures().join("vectors.json")).expect("vectors.json");
        serde_json::from_str(&text).unwrap()
    }

    fn bin(v: &Value) -> Bytes {
        let file = v["file"].as_str().unwrap();
        let bytes = std::fs::read(fixtures().join(file)).unwrap();
        assert_eq!(
            hex::encode(&bytes),
            v["hex"].as_str().unwrap(),
            "{file} matches its hex"
        );
        Bytes::from(bytes)
    }

    fn limits(v: &Value) -> DecodeLimits {
        let mut l = DecodeLimits::default();
        if let Some(n) = v["limits"]["maxPayload"].as_u64() {
            l.max_payload = n as usize;
        }
        if let Some(n) = v["limits"]["maxFrames"].as_u64() {
            l.max_frames = n as usize;
        }
        l
    }

    /// The JSON description of a frame in vectors.json.
    fn describe(frame: &Frame) -> Value {
        let peer = |p: &PeerFields| {
            let ip = match p.addr.ip() {
                IpAddr::V4(v4) => hex::encode(v4.octets()),
                IpAddr::V6(v6) => hex::encode(v6.octets()),
            };
            let mut fields = serde_json::json!({
                "infoHash": hex::encode(p.info_hash),
                "reserved": hex::encode(p.reserved),
                "peerId": hex::encode(p.peer_id),
                "address": {"family": if p.addr.is_ipv4() { 4 } else { 6 }, "ip": ip, "port": p.addr.port()},
            });
            if p.transport != transport::TCP {
                fields["transport"] = p.transport.into();
            }
            fields
        };
        let with = |ty: &str, conn: u16, mut rest: Value| {
            rest["type"] = ty.into();
            rest["conn"] = conn.into();
            rest
        };
        match frame {
            Frame::Open { conn, peer: p } => with("open", *conn, peer(p)),
            Frame::Accept { conn, peer: p } => with("accept", *conn, peer(p)),
            Frame::Opened {
                conn,
                reserved,
                peer_id,
            } => with(
                "opened",
                *conn,
                serde_json::json!({"reserved": hex::encode(reserved), "peerId": hex::encode(peer_id)}),
            ),
            Frame::Failed { conn, reason } => {
                with("failed", *conn, serde_json::json!({"reason": reason}))
            }
            Frame::Close { conn, reason } => {
                with("close", *conn, serde_json::json!({"reason": reason}))
            }
            Frame::Data { conn, payload } => with(
                "data",
                *conn,
                serde_json::json!({"payload": hex::encode(payload)}),
            ),
            Frame::Ping { payload } => with(
                "ping",
                0,
                serde_json::json!({"payload": hex::encode(payload)}),
            ),
            Frame::Announce {
                info_hash,
                reserved,
                peer_id,
                trackers,
            } => with(
                "announce",
                0,
                serde_json::json!({
                    "infoHash": hex::encode(info_hash),
                    "reserved": hex::encode(reserved),
                    "peerId": hex::encode(peer_id),
                    "trackers": trackers,
                }),
            ),
            Frame::Announced { info_hash, status } => with(
                "announced",
                0,
                serde_json::json!({"infoHash": hex::encode(info_hash), "status": status}),
            ),
            Frame::Withdraw { info_hash } => with(
                "withdraw",
                0,
                serde_json::json!({"infoHash": hex::encode(info_hash)}),
            ),
        }
    }

    #[test]
    fn shared_valid_vectors_decode_and_reencode_identically() {
        let v = vectors();
        let valid = v["valid"].as_array().unwrap();
        assert!(valid.len() >= 10);
        for vector in valid {
            let name = vector["name"].as_str().unwrap();
            let bytes = bin(vector);
            let frames =
                decode_message(&bytes, limits(vector)).unwrap_or_else(|e| panic!("{name}: {e:?}"));
            let described: Vec<Value> = frames.iter().map(describe).collect();
            assert_eq!(
                &Value::Array(described),
                &vector["frames"],
                "{name}: decoded fields"
            );
            assert_eq!(
                encode_frames(&frames),
                bytes.to_vec(),
                "{name}: re-encoded bytes"
            );
        }
    }

    #[test]
    fn shared_invalid_vectors_fail_with_their_error() {
        let v = vectors();
        let invalid = v["invalid"].as_array().unwrap();
        assert!(invalid.len() >= 15);
        for vector in invalid {
            let name = vector["name"].as_str().unwrap();
            let got = decode_message(&bin(vector), limits(vector));
            let code = got.map(|_| "ok").unwrap_or_else(|e| e.code.as_str());
            assert_eq!(code, vector["error"].as_str().unwrap(), "{name}");
        }
    }

    #[test]
    fn tcp_is_the_transport_byte_left_out() {
        let mut peer = PeerFields {
            info_hash: [1; 20],
            reserved: [0; 8],
            peer_id: [2; 20],
            addr: "203.0.113.7:6881".parse().unwrap(),
            transport: transport::TCP,
        };
        let tcp = encode_frames(&[Frame::Open {
            conn: 1,
            peer: peer.clone(),
        }]);
        assert_eq!(tcp.len(), HEADER_LEN + 55);
        peer.transport = transport::UTP;
        let utp = encode_frames(&[Frame::Open {
            conn: 1,
            peer: peer.clone(),
        }]);
        assert_eq!(utp.len(), HEADER_LEN + 56);
        assert_eq!(utp[utp.len() - 1], transport::UTP);
        let back = decode_message(&Bytes::from(utp.clone()), DecodeLimits::default()).unwrap();
        assert_eq!(back, vec![Frame::Open { conn: 1, peer }]);
        // An unknown transport decodes (the session fails it); an explicit 0 does not.
        let mut odd = utp.clone();
        *odd.last_mut().unwrap() = 9;
        assert!(decode_message(&Bytes::from(odd), DecodeLimits::default()).is_ok());
        let mut zero = utp;
        *zero.last_mut().unwrap() = 0;
        let err = decode_message(&Bytes::from(zero), DecodeLimits::default()).unwrap_err();
        assert_eq!(err.code, FrameErrorCode::BadPayload);
    }

    #[test]
    fn data_payloads_are_slices_of_the_message() {
        let msg = Bytes::from(encode_frames(&[Frame::Data {
            conn: 5,
            payload: Bytes::from_static(b"abc"),
        }]));
        let frames = decode_message(&msg, DecodeLimits::default()).unwrap();
        let Frame::Data { payload, .. } = &frames[0] else {
            panic!()
        };
        assert_eq!(payload.as_ptr(), msg[HEADER_LEN..].as_ptr());
    }

    #[test]
    fn session_end_reason_fits_and_sanitises() {
        let r = format_session_end("normal", 2, 10, 20);
        assert_eq!(r, "normal in=2 up=10 down=20");
        let max = format_session_end(&"a".repeat(32), u64::MAX, u64::MAX, u64::MAX);
        assert!(max.len() <= 123);
        assert!(format_session_end("Bad Reason", 0, 0, 0).starts_with("internal_error "));
    }
}
