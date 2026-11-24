//! The handshake gate (docs/tunnel-protocol.md › How the info-hash is bound
//! to a connection): the gateway writes the 68-byte BitTorrent handshake on
//! the client's behalf and relays nothing, either way, until the remote's
//! bytes are a valid handshake for the same info-hash. Pure function over
//! the bytes read so far, so it is tested without sockets.

use super::codec::fail_reason;
use crate::peerwire::{HANDSHAKE_LEN, PROTOCOL};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Gate {
    /// A valid prefix so far: read more.
    NeedMore,
    /// A valid handshake for the info-hash; `buf[HANDSHAKE_LEN..]` is
    /// peer-wire stream that came with it.
    Accepted {
        reserved: [u8; 8],
        peer_id: [u8; 20],
    },
    /// Not a BitTorrent handshake for this torrent: the `failed` reason.
    Rejected(u8),
}

/// Judge the remote's first bytes. Fails as soon as a byte departs from the
/// protocol string (an HTTP or SSH banner fails on its first byte), and
/// checks the info-hash once all 68 bytes are in.
pub fn check(buf: &[u8], info_hash: &[u8; 20]) -> Gate {
    let prefix_len = 1 + PROTOCOL.len();
    let seen = buf.len().min(prefix_len);
    let mut expected = [0u8; 20];
    expected[0] = PROTOCOL.len() as u8;
    expected[1..].copy_from_slice(PROTOCOL);
    if buf[..seen] != expected[..seen] {
        return Gate::Rejected(fail_reason::NOT_BITTORRENT);
    }
    if buf.len() < HANDSHAKE_LEN {
        return Gate::NeedMore;
    }
    if &buf[28..48] != info_hash {
        return Gate::Rejected(fail_reason::INFOHASH_MISMATCH);
    }
    let mut reserved = [0u8; 8];
    reserved.copy_from_slice(&buf[20..28]);
    let mut peer_id = [0u8; 20];
    peer_id.copy_from_slice(&buf[48..68]);
    Gate::Accepted { reserved, peer_id }
}

/// The inbound side of the gate (an announced session, docs/tunnel-protocol.md
/// › Announce and inbound connections): the remote speaks first, and its
/// handshake must name an info-hash a session announces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inbound {
    NeedMore,
    /// `buf[HANDSHAKE_LEN..]` is peer-wire stream that came with it.
    Accepted {
        info_hash: [u8; 20],
        reserved: [u8; 8],
        peer_id: [u8; 20],
    },
    /// Not a BitTorrent handshake: closed without a reply.
    NotBitTorrent,
    /// A handshake for an info-hash no session announces: closed without a reply.
    NoSession,
}

/// Judge an inbound remote's first bytes: the protocol string byte by byte,
/// then the info-hash against `announced` as soon as its 20 bytes are in
/// (before the peer id), then the whole 68 bytes.
pub fn check_inbound(buf: &[u8], announced: impl Fn(&[u8; 20]) -> bool) -> Inbound {
    let prefix_len = 1 + PROTOCOL.len();
    let seen = buf.len().min(prefix_len);
    let mut expected = [0u8; 20];
    expected[0] = PROTOCOL.len() as u8;
    expected[1..].copy_from_slice(PROTOCOL);
    if buf[..seen] != expected[..seen] {
        return Inbound::NotBitTorrent;
    }
    if buf.len() < 48 {
        return Inbound::NeedMore;
    }
    let mut info_hash = [0u8; 20];
    info_hash.copy_from_slice(&buf[28..48]);
    if !announced(&info_hash) {
        return Inbound::NoSession;
    }
    if buf.len() < HANDSHAKE_LEN {
        return Inbound::NeedMore;
    }
    let mut reserved = [0u8; 8];
    reserved.copy_from_slice(&buf[20..28]);
    let mut peer_id = [0u8; 20];
    peer_id.copy_from_slice(&buf[48..68]);
    Inbound::Accepted {
        info_hash,
        reserved,
        peer_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peerwire::encode_handshake_with;

    const IH: [u8; 20] = [7; 20];

    fn remote() -> Vec<u8> {
        encode_handshake_with(
            &[0, 0, 0, 0, 0, 0x10, 0, 0x04],
            &IH,
            b"-FP0001-abcdefghijkl",
        )
        .to_vec()
    }

    #[test]
    fn needs_the_whole_handshake_byte_by_byte() {
        let hs = remote();
        for n in 0..HANDSHAKE_LEN {
            assert_eq!(check(&hs[..n], &IH), Gate::NeedMore, "after {n} bytes");
        }
        assert_eq!(
            check(&hs, &IH),
            Gate::Accepted {
                reserved: [0, 0, 0, 0, 0, 0x10, 0, 0x04],
                peer_id: *b"-FP0001-abcdefghijkl"
            }
        );
    }

    #[test]
    fn extra_bytes_after_the_handshake_are_left_to_the_caller() {
        let mut hs = remote();
        hs.extend_from_slice(&[0, 0, 0, 1, 0x0e]);
        assert!(matches!(check(&hs, &IH), Gate::Accepted { .. }));
    }

    #[test]
    fn rejects_other_protocols_on_the_first_wrong_byte() {
        assert_eq!(
            check(b"H", &IH),
            Gate::Rejected(fail_reason::NOT_BITTORRENT)
        );
        assert_eq!(
            check(b"SSH-2.0-OpenSSH", &IH),
            Gate::Rejected(fail_reason::NOT_BITTORRENT)
        );
        let mut hs = remote();
        hs[5] = b'X';
        assert_eq!(
            check(&hs[..6], &IH),
            Gate::Rejected(fail_reason::NOT_BITTORRENT)
        );
    }

    #[test]
    fn rejects_another_torrent() {
        let hs = encode_handshake_with(&[0; 8], &[8; 20], &[1; 20]);
        assert_eq!(check(&hs[..67], &IH), Gate::NeedMore);
        assert_eq!(
            check(&hs, &IH),
            Gate::Rejected(fail_reason::INFOHASH_MISMATCH)
        );
    }

    #[test]
    fn inbound_needs_an_announced_info_hash_and_the_whole_handshake() {
        let hs = remote();
        let announced = |ih: &[u8; 20]| *ih == IH;
        for n in 0..HANDSHAKE_LEN {
            assert_eq!(check_inbound(&hs[..n], announced), Inbound::NeedMore, "{n}");
        }
        let mut more = hs.clone();
        more.extend_from_slice(&[0, 0, 0, 1, 0x0e]);
        assert_eq!(
            check_inbound(&more, announced),
            Inbound::Accepted {
                info_hash: IH,
                reserved: [0, 0, 0, 0, 0, 0x10, 0, 0x04],
                peer_id: *b"-FP0001-abcdefghijkl"
            }
        );
    }

    #[test]
    fn inbound_rejects_an_unannounced_hash_before_the_peer_id() {
        let hs = encode_handshake_with(&[0; 8], &[8; 20], &[1; 20]);
        let announced = |ih: &[u8; 20]| *ih == IH;
        assert_eq!(check_inbound(&hs[..47], announced), Inbound::NeedMore);
        assert_eq!(check_inbound(&hs[..48], announced), Inbound::NoSession);
        assert_eq!(check_inbound(&hs, |_| false), Inbound::NoSession);
    }

    #[test]
    fn inbound_rejects_other_protocols_on_the_first_wrong_byte() {
        let yes = |_: &[u8; 20]| true;
        assert_eq!(check_inbound(b"G", yes), Inbound::NotBitTorrent);
        assert_eq!(
            check_inbound(b"\x13BitTorrent protocoX", yes),
            Inbound::NotBitTorrent
        );
        assert_eq!(check_inbound(b"", yes), Inbound::NeedMore);
    }
}
