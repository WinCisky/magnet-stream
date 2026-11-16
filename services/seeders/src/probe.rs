//! Probing a peer: connect, handshake, say we're interested, and watch the
//! first few messages. That tells us whether it is reachable over TCP, which
//! pieces it has, whether it unchokes a fresh downloader, and how many
//! requests it queues. The browser's scheduler uses all of this to decide
//! which peers are worth a magnet-worker request.

use crate::peerwire::{self, elapsed_ms, ExtensionHandshake, Message, PeerConn, WireError};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Upper bound on buffered `have` messages (a peer can't make us grow
/// memory without bound within the probe window).
const MAX_HAVES: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    Unknown,
    Nothing,
    Everything,
    Bitfield(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct PeerProbe {
    /// Why the peer is unusable; `None` if it handshook and told us
    /// something.
    pub error: Option<WireError>,
    pub connect_ms: Option<u32>,
    pub fast: bool,
    pub extensions: ExtensionHandshake,
    pub availability: Availability,
    /// Time from our `interested` to the peer's unchoke, if it came in time.
    pub unchoke_ms: Option<u32>,
}

impl PeerProbe {
    pub fn failed(error: WireError) -> Self {
        Self {
            error: Some(error),
            connect_ms: None,
            fast: false,
            extensions: ExtensionHandshake::default(),
            availability: Availability::Unknown,
            unchoke_ms: None,
        }
    }

    pub fn ok(&self) -> bool {
        self.error.is_none()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProbeTimeouts {
    pub connect: Duration,
    pub total: Duration,
    pub unchoke_wait: Duration,
}

pub async fn probe(addr: SocketAddr, info_hash: [u8; 20], timeouts: ProbeTimeouts) -> PeerProbe {
    let deadline = Instant::now() + timeouts.total;
    let mut conn = match PeerConn::open(addr, info_hash, timeouts.connect, deadline).await {
        Ok(conn) => conn,
        Err(e) => return PeerProbe::failed(e),
    };

    let fast = conn.handshake.supports_fast();
    let extensions_supported = conn.handshake.supports_extensions();
    let mut hello = Vec::new();
    if fast {
        hello.extend(peerwire::have_none());
    }
    if extensions_supported {
        hello.extend(peerwire::extended(0, &peerwire::our_extension_handshake()));
    }
    hello.extend(peerwire::interested());
    if let Err(e) = conn.send(&hello).await {
        return PeerProbe {
            connect_ms: Some(conn.connect_ms),
            ..PeerProbe::failed(e)
        };
    }

    let interested_at = Instant::now();
    let until = deadline.min(interested_at + timeouts.unchoke_wait);
    let mut availability = Availability::Unknown;
    let mut haves: Vec<u32> = Vec::new();
    let mut extensions = ExtensionHandshake::default();
    let mut extensions_seen = false;
    let mut unchoke_ms = None;
    let mut end_error = None;

    loop {
        let knows_pieces = availability != Availability::Unknown || !haves.is_empty();
        if knows_pieces && unchoke_ms.is_some() && (extensions_seen || !extensions_supported) {
            break;
        }
        match conn.next_message(until).await {
            Ok(Message::Bitfield(bits)) => availability = Availability::Bitfield(bits),
            Ok(Message::HaveAll) => availability = Availability::Everything,
            Ok(Message::HaveNone) => availability = Availability::Nothing,
            Ok(Message::Have(index)) if haves.len() < MAX_HAVES => haves.push(index),
            Ok(Message::Unchoke) => {
                unchoke_ms.get_or_insert(elapsed_ms(interested_at));
            }
            Ok(Message::Extended { id: 0, payload }) => {
                extensions = ExtensionHandshake::parse(&payload).unwrap_or_default();
                extensions_seen = true;
            }
            Ok(_) => {}
            Err(WireError::Timeout) => break,
            Err(e) => {
                end_error = Some(e);
                break;
            }
        }
    }

    let availability = merge_haves(availability, &haves);
    let said_something =
        availability != Availability::Unknown || unchoke_ms.is_some() || extensions_seen;
    let error = if said_something {
        None
    } else {
        Some(end_error.unwrap_or(WireError::Timeout))
    };
    PeerProbe {
        error,
        connect_ms: Some(conn.connect_ms),
        fast,
        extensions,
        availability,
        unchoke_ms,
    }
}

/// Fold `have` messages into the bitfield. Peers may send a partial ("lazy")
/// bitfield and complete it with haves, or send only haves.
fn merge_haves(availability: Availability, haves: &[u32]) -> Availability {
    if haves.is_empty() {
        return availability;
    }
    let (mut bits, sized) = match availability {
        Availability::Everything => return Availability::Everything,
        Availability::Bitfield(bits) => (bits, true),
        Availability::Nothing | Availability::Unknown => (Vec::new(), false),
    };
    for &index in haves {
        let byte = (index / 8) as usize;
        if byte >= bits.len() {
            // A real bitfield already has the torrent's length, so a have
            // past it is bogus; without one, grow (within reason).
            if sized || byte >= peerwire::MAX_MESSAGE_LEN {
                continue;
            }
            bits.resize(byte + 1, 0);
        }
        bits[byte] |= 0x80 >> (index % 8);
    }
    Availability::Bitfield(bits)
}

/// Whether a bitfield covers every piece. Without the piece count, a peer
/// counts as a seed when every byte is full except a last byte of leading
/// ones (the spare bits of the final byte must be zero).
pub fn is_complete(bits: &[u8], num_pieces: Option<u32>) -> bool {
    match num_pieces {
        Some(0) => true,
        Some(n) => {
            let n = n as usize;
            if bits.len() < n.div_ceil(8) {
                return false;
            }
            let full_bytes = n / 8;
            if bits[..full_bytes].iter().any(|&b| b != 0xFF) {
                return false;
            }
            let rest = n % 8;
            rest == 0 || bits[full_bytes] & (0xFFu8 << (8 - rest)) == 0xFFu8 << (8 - rest)
        }
        None => match bits.split_last() {
            None => false,
            Some((&last, init)) => {
                init.iter().all(|&b| b == 0xFF)
                    && last != 0
                    && last.leading_ones() + last.trailing_zeros() == 8
            }
        },
    }
}

pub fn count_pieces(bits: &[u8], num_pieces: Option<u32>) -> u32 {
    let total: u32 = bits.iter().map(|b| b.count_ones()).sum();
    match num_pieces {
        Some(n) => total.min(n),
        None => total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testpeer::{self, FakeAvailability, FakePeer};

    fn timeouts() -> ProbeTimeouts {
        ProbeTimeouts {
            connect: Duration::from_millis(500),
            total: Duration::from_secs(3),
            unchoke_wait: Duration::from_millis(800),
        }
    }

    #[tokio::test]
    async fn probes_a_seed_that_unchokes() {
        let peer = FakePeer::seed([1u8; 20]);
        let addr = testpeer::spawn(peer).await;
        let result = probe(addr, [1u8; 20], timeouts()).await;
        assert!(result.ok(), "{:?}", result.error);
        assert_eq!(result.availability, Availability::Everything);
        assert!(result.unchoke_ms.is_some());
        assert!(result.fast);
        assert_eq!(result.extensions.reqq, Some(500));
        assert_eq!(result.extensions.client.as_deref(), Some("FakePeer/1.0"));
        assert!(result.connect_ms.is_some());
    }

    #[tokio::test]
    async fn merges_lazy_bitfield_with_haves_and_notes_choke() {
        let peer = FakePeer {
            fast: false,
            availability: FakeAvailability::Bitfield(vec![0b1010_0000]),
            haves: vec![1],
            unchoke: false,
            ..FakePeer::seed([2u8; 20])
        };
        let addr = testpeer::spawn(peer).await;
        let result = probe(addr, [2u8; 20], timeouts()).await;
        assert!(result.ok());
        assert_eq!(
            result.availability,
            Availability::Bitfield(vec![0b1110_0000])
        );
        assert_eq!(result.unchoke_ms, None);
        assert!(!result.fast);
    }

    #[tokio::test]
    async fn rejects_peer_with_other_info_hash() {
        let addr = testpeer::spawn(FakePeer::seed([3u8; 20])).await;
        let result = probe(addr, [4u8; 20], timeouts()).await;
        assert_eq!(result.error, Some(WireError::InfoHashMismatch));
        assert!(!result.ok());
    }

    #[tokio::test]
    async fn reports_refused_connections() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let result = probe(addr, [5u8; 20], timeouts()).await;
        assert_eq!(
            result.error.as_ref().map(WireError::code),
            Some("connect_failed")
        );
    }

    #[tokio::test]
    async fn silent_peer_is_not_ok() {
        let peer = FakePeer {
            fast: false,
            extensions: false,
            availability: FakeAvailability::Silent,
            unchoke: false,
            ..FakePeer::seed([6u8; 20])
        };
        let addr = testpeer::spawn(peer).await;
        let result = probe(addr, [6u8; 20], timeouts()).await;
        assert_eq!(result.error, Some(WireError::Timeout));
    }

    #[test]
    fn seed_detection_with_and_without_piece_count() {
        assert!(is_complete(&[0xFF, 0xE0], Some(11)));
        assert!(!is_complete(&[0xFF, 0xC0], Some(11)));
        assert!(!is_complete(&[0xFF], Some(11)));
        assert!(is_complete(&[0xFF, 0xFF], Some(16)));
        assert!(is_complete(&[0xFF, 0xE0], None));
        assert!(is_complete(&[0xFF, 0xFF], None));
        assert!(!is_complete(&[0xFF, 0xA0], None));
        assert!(!is_complete(&[0x7F, 0xFF], None));
        assert!(!is_complete(&[], None));
        assert_eq!(count_pieces(&[0xFF, 0xE0], Some(11)), 11);
    }

    #[test]
    fn haves_without_bitfield_build_one() {
        assert_eq!(
            merge_haves(Availability::Unknown, &[0, 9]),
            Availability::Bitfield(vec![0x80, 0x40])
        );
        assert_eq!(
            merge_haves(Availability::Everything, &[3]),
            Availability::Everything
        );
    }
}
