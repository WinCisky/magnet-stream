//! uTP packet codec (BEP 29, <https://www.bittorrent.org/beps/bep_0029.html>).
//!
//! ```text
//! 0       4       8               16              24              32
//! +-------+-------+---------------+---------------+---------------+
//! | type  | ver   | extension     | connection_id                 |
//! +-------+-------+---------------+---------------+---------------+
//! | timestamp_microseconds                                        |
//! +---------------+---------------+---------------+---------------+
//! | timestamp_difference_microseconds                             |
//! +---------------+---------------+---------------+---------------+
//! | wnd_size                                                      |
//! +---------------+---------------+---------------+---------------+
//! | seq_nr                        | ack_nr                        |
//! +---------------+---------------+---------------+---------------+
//! ```
//!
//! All fields big-endian. `extension` names the first extension of a chain
//! of `[next extension u8][length u8][length bytes]` records, ended by a
//! next-extension of 0; the payload follows the chain.
//!
//! Parsing is strict (docs/decisions/utp.md › Split of M4-04): version 1,
//! type 0–4, at most [`MAX_EXTENSIONS`] extensions each fully present, the
//! chain terminated, a selective ACK at most once with a bitmask of a
//! non-zero multiple of 4 bytes, a payload only on `ST_DATA`. Unknown
//! extensions are skipped (their declared length is checked against the
//! bytes present); they are not kept, so re-encoding a packet drops them.
//! Decoding borrows the datagram: nothing is allocated, whatever lengths
//! the packet declares, and no input panics. A rejected packet is dropped
//! by the caller without a reply; [`DecodeError::code`] is the reason code
//! for counters and logs.
//!
//! Sequence and ACK numbers are 16 bits and wrap ([`SeqNr`]); timestamps
//! are microseconds truncated to 32 bits and wrap too ([`timestamp_micros`]).

use std::fmt;
use std::time::Instant;

/// Fixed header length.
pub const HEADER_LEN: usize = 20;
/// The only protocol version.
pub const VERSION: u8 = 1;
/// Extension type that ends the chain.
pub const EXT_NONE: u8 = 0;
/// Selective ACK extension.
pub const EXT_SELECTIVE_ACK: u8 = 1;
/// Longest chain accepted. Clients send at most two or three (selective
/// ACK, libutp's extension bits, libtorrent's close reason); the bound
/// keeps the work per packet small whatever the packet says.
pub const MAX_EXTENSIONS: usize = 8;
/// Longest selective ACK bitmask: the largest multiple of 4 an extension
/// length byte can express.
pub const MAX_SACK_LEN: usize = 252;

/// Packet type, the high nibble of the first byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PacketType {
    /// `ST_DATA`: carries payload.
    Data = 0,
    /// `ST_FIN`: the last packet of the sender.
    Fin = 1,
    /// `ST_STATE`: a bare ACK; does not advance `seq_nr`.
    State = 2,
    /// `ST_RESET`: forcibly ends the connection.
    Reset = 3,
    /// `ST_SYN`: opens a connection.
    Syn = 4,
}

impl PacketType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Data),
            1 => Some(Self::Fin),
            2 => Some(Self::State),
            3 => Some(Self::Reset),
            4 => Some(Self::Syn),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Data => "ST_DATA",
            Self::Fin => "ST_FIN",
            Self::State => "ST_STATE",
            Self::Reset => "ST_RESET",
            Self::Syn => "ST_SYN",
        }
    }
}

/// A 16-bit sequence or ACK number. Comparisons are modular (BEP 29, as
/// libutp's `wrapping_compare_less`): `a` is before `b` when going forward
/// from `a` reaches `b` in fewer steps than going backward. Two numbers
/// exactly 0x8000 apart are neither before nor after each other. There is
/// deliberately no `Ord`: the relation is not transitive across the ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SeqNr(pub u16);

impl SeqNr {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }

    /// `self + n` modulo 2^16.
    #[must_use]
    pub const fn add(self, n: u16) -> Self {
        Self(self.0.wrapping_add(n))
    }

    /// `self - n` modulo 2^16.
    #[must_use]
    pub const fn sub(self, n: u16) -> Self {
        Self(self.0.wrapping_sub(n))
    }

    /// Steps forward from `earlier` to `self`, modulo 2^16.
    pub const fn distance_from(self, earlier: SeqNr) -> u16 {
        self.0.wrapping_sub(earlier.0)
    }

    /// `self` comes strictly before `other` on the ring.
    pub const fn is_before(self, other: SeqNr) -> bool {
        let up = other.0.wrapping_sub(self.0);
        let down = self.0.wrapping_sub(other.0);
        up < down
    }

    /// `self` comes strictly after `other` on the ring.
    pub const fn is_after(self, other: SeqNr) -> bool {
        other.is_before(self)
    }
}

impl fmt::Display for SeqNr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Microseconds elapsed since `epoch`, truncated to 32 bits: the value of
/// `timestamp_microseconds`. It wraps every ~71.6 minutes; only
/// differences between timestamps of the same clock are meaningful.
pub fn timestamp_micros(epoch: Instant, now: Instant) -> u32 {
    // Truncation is the wire format (modulo 2^32).
    now.saturating_duration_since(epoch).as_micros() as u32
}

/// `timestamp_difference_microseconds` for a packet received at local time
/// `received_us` that carried `remote_us`: their difference modulo 2^32.
pub const fn timestamp_difference(received_us: u32, remote_us: u32) -> u32 {
    received_us.wrapping_sub(remote_us)
}

/// The 20-byte header, decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub packet_type: PacketType,
    pub connection_id: u16,
    pub timestamp_us: u32,
    pub timestamp_difference_us: u32,
    pub wnd_size: u32,
    pub seq_nr: SeqNr,
    pub ack_nr: SeqNr,
}

/// A selective ACK bitmask (BEP 29, extension 1). Bit `i` (byte `i / 8`,
/// least significant bit first) stands for `ack_nr + 2 + i`: `ack_nr + 1`
/// is implicitly missing, or it would have been acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectiveAck<'a> {
    bitmask: &'a [u8],
}

impl<'a> SelectiveAck<'a> {
    /// `None` unless the length is a non-zero multiple of 4 and at most
    /// [`MAX_SACK_LEN`].
    pub fn new(bitmask: &'a [u8]) -> Option<Self> {
        valid_sack_len(bitmask.len()).then_some(Self { bitmask })
    }

    pub fn bitmask(&self) -> &'a [u8] {
        self.bitmask
    }

    /// Number of packets the bitmask covers.
    pub fn len_bits(&self) -> usize {
        self.bitmask.len() * 8
    }

    /// Whether bit `i` (packet `ack_nr + 2 + i`) is set; false past the end.
    pub fn is_set(&self, i: usize) -> bool {
        self.bitmask
            .get(i / 8)
            .is_some_and(|byte| byte & (1 << (i % 8)) != 0)
    }

    /// The sequence numbers the bitmask acknowledges, in order, for a
    /// packet whose `ack_nr` is `ack_nr`.
    pub fn acked(&self, ack_nr: SeqNr) -> impl Iterator<Item = SeqNr> + 'a {
        let bitmask = self.bitmask;
        (0..bitmask.len() * 8)
            .filter(move |&i| bitmask[i / 8] & (1 << (i % 8)) != 0)
            .map(move |i| ack_nr.add(2).add(i as u16))
    }
}

fn valid_sack_len(len: usize) -> bool {
    len != 0 && len % 4 == 0 && len <= MAX_SACK_LEN
}

/// One uTP packet, borrowing the datagram it was decoded from (or the
/// buffers it is built from).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Packet<'a> {
    pub header: Header,
    pub selective_ack: Option<SelectiveAck<'a>>,
    pub payload: &'a [u8],
    /// Unknown extensions skipped while decoding (not re-encoded).
    pub skipped_extensions: u8,
}

/// Why a datagram is not a uTP packet. Each maps to a reason code for
/// counters; the datagram is dropped without a reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// Fewer than 20 bytes.
    TooShort,
    /// Version other than 1.
    BadVersion,
    /// Type above 4.
    BadType,
    /// The chain names another extension but its 2-byte record header is
    /// missing.
    ExtensionUnterminated,
    /// An extension declares more bytes than the datagram holds.
    ExtensionTruncated,
    /// More than [`MAX_EXTENSIONS`] extensions.
    TooManyExtensions,
    /// Selective ACK bitmask empty or not a multiple of 4 bytes.
    BadSackLength,
    /// Two selective ACK extensions.
    DuplicateSack,
    /// Payload on a type other than `ST_DATA`.
    UnexpectedPayload,
}

impl DecodeError {
    pub fn code(self) -> &'static str {
        match self {
            Self::TooShort => "too_short",
            Self::BadVersion => "bad_version",
            Self::BadType => "bad_type",
            Self::ExtensionUnterminated => "extension_unterminated",
            Self::ExtensionTruncated => "extension_truncated",
            Self::TooManyExtensions => "too_many_extensions",
            Self::BadSackLength => "bad_sack_length",
            Self::DuplicateSack => "duplicate_sack",
            Self::UnexpectedPayload => "unexpected_payload",
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for DecodeError {}

/// Why a packet cannot be encoded (a bug of the caller, never peer input).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeError {
    /// Payload on a type other than `ST_DATA`.
    UnexpectedPayload,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedPayload => f.write_str("unexpected_payload"),
        }
    }
}

impl std::error::Error for EncodeError {}

fn be_u16(b: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([b[at], b[at + 1]])
}

fn be_u32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

impl<'a> Packet<'a> {
    /// A packet without extensions or payload.
    pub fn new(header: Header) -> Self {
        Self {
            header,
            selective_ack: None,
            payload: &[],
            skipped_extensions: 0,
        }
    }

    /// Decodes one datagram. Checks run in a fixed order: length, version,
    /// type, extension chain (in chain order), payload.
    pub fn decode(datagram: &'a [u8]) -> Result<Self, DecodeError> {
        if datagram.len() < HEADER_LEN {
            return Err(DecodeError::TooShort);
        }
        if datagram[0] & 0x0f != VERSION {
            return Err(DecodeError::BadVersion);
        }
        let packet_type = PacketType::from_u8(datagram[0] >> 4).ok_or(DecodeError::BadType)?;
        let header = Header {
            packet_type,
            connection_id: be_u16(datagram, 2),
            timestamp_us: be_u32(datagram, 4),
            timestamp_difference_us: be_u32(datagram, 8),
            wnd_size: be_u32(datagram, 12),
            seq_nr: SeqNr(be_u16(datagram, 16)),
            ack_nr: SeqNr(be_u16(datagram, 18)),
        };

        let mut selective_ack = None;
        let mut skipped_extensions = 0u8;
        let mut extension = datagram[1];
        let mut at = HEADER_LEN;
        let mut count = 0usize;
        while extension != EXT_NONE {
            count += 1;
            if count > MAX_EXTENSIONS {
                return Err(DecodeError::TooManyExtensions);
            }
            let record = datagram
                .get(at..at + 2)
                .ok_or(DecodeError::ExtensionUnterminated)?;
            let (next, len) = (record[0], usize::from(record[1]));
            let body = datagram
                .get(at + 2..at + 2 + len)
                .ok_or(DecodeError::ExtensionTruncated)?;
            if extension == EXT_SELECTIVE_ACK {
                if selective_ack.is_some() {
                    return Err(DecodeError::DuplicateSack);
                }
                selective_ack = Some(SelectiveAck::new(body).ok_or(DecodeError::BadSackLength)?);
            } else {
                skipped_extensions += 1;
            }
            extension = next;
            at += 2 + len;
        }

        let payload = &datagram[at..];
        if !payload.is_empty() && packet_type != PacketType::Data {
            return Err(DecodeError::UnexpectedPayload);
        }
        Ok(Self {
            header,
            selective_ack,
            payload,
            skipped_extensions,
        })
    }

    /// Bytes [`Packet::encode`] appends.
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.selective_ack.map_or(0, |s| 2 + s.bitmask.len()) + self.payload.len()
    }

    /// Appends the packet to `out` (the selective ACK, if any, as the only
    /// extension).
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        if !self.payload.is_empty() && self.header.packet_type != PacketType::Data {
            return Err(EncodeError::UnexpectedPayload);
        }
        let h = &self.header;
        out.reserve(self.encoded_len());
        out.push(((h.packet_type as u8) << 4) | VERSION);
        out.push(if self.selective_ack.is_some() {
            EXT_SELECTIVE_ACK
        } else {
            EXT_NONE
        });
        out.extend_from_slice(&h.connection_id.to_be_bytes());
        out.extend_from_slice(&h.timestamp_us.to_be_bytes());
        out.extend_from_slice(&h.timestamp_difference_us.to_be_bytes());
        out.extend_from_slice(&h.wnd_size.to_be_bytes());
        out.extend_from_slice(&h.seq_nr.0.to_be_bytes());
        out.extend_from_slice(&h.ack_nr.0.to_be_bytes());
        if let Some(sack) = self.selective_ack {
            // `SelectiveAck::new` bounds the length to MAX_SACK_LEN (< 256).
            out.push(EXT_NONE);
            out.push(sack.bitmask.len() as u8);
            out.extend_from_slice(sack.bitmask);
        }
        out.extend_from_slice(self.payload);
        Ok(())
    }

    /// [`Packet::encode`] into a new buffer.
    pub fn encode_to_vec(&self) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.encode(&mut out)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::time::Duration;

    fn header(packet_type: PacketType) -> Header {
        Header {
            packet_type,
            connection_id: 0x1234,
            timestamp_us: 0x0102_0304,
            timestamp_difference_us: 0x0a0b_0c0d,
            wnd_size: 0x0010_0000,
            seq_nr: SeqNr(0xfffe),
            ack_nr: SeqNr(0x0001),
        }
    }

    /// The 20 header bytes, built field by field from BEP 29's layout.
    fn header_bytes(type_version: u8, extension: u8) -> Vec<u8> {
        let mut b = vec![type_version, extension];
        b.extend_from_slice(&[0x12, 0x34]); // connection_id
        b.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]); // timestamp_microseconds
        b.extend_from_slice(&[0x0a, 0x0b, 0x0c, 0x0d]); // timestamp_difference_microseconds
        b.extend_from_slice(&[0x00, 0x10, 0x00, 0x00]); // wnd_size
        b.extend_from_slice(&[0xff, 0xfe]); // seq_nr
        b.extend_from_slice(&[0x00, 0x01]); // ack_nr
        b
    }

    fn roundtrip(bytes: &[u8]) -> Packet<'_> {
        let packet = Packet::decode(bytes).expect("decodes");
        assert_eq!(packet.encoded_len(), bytes.len());
        assert_eq!(packet.encode_to_vec().unwrap(), bytes);
        packet
    }

    #[test]
    fn bep29_vectors_roundtrip() {
        // Every type, no extension, no payload: first byte is type << 4 | 1.
        for (byte, t) in [
            (0x01, PacketType::Data),
            (0x11, PacketType::Fin),
            (0x21, PacketType::State),
            (0x31, PacketType::Reset),
            (0x41, PacketType::Syn),
        ] {
            let bytes = header_bytes(byte, 0);
            let p = roundtrip(&bytes);
            assert_eq!(p.header, header(t));
            assert!(p.selective_ack.is_none());
            assert!(p.payload.is_empty());
        }

        // ST_DATA with payload.
        let mut bytes = header_bytes(0x01, 0);
        bytes.extend_from_slice(b"\x13BitTorrent protocol");
        let p = roundtrip(&bytes);
        assert_eq!(p.payload, b"\x13BitTorrent protocol");

        // ST_STATE with a 4-byte selective ACK.
        let mut bytes = header_bytes(0x21, 1);
        bytes.extend_from_slice(&[0, 4, 0b0000_0101, 0, 0, 0x80]);
        let p = roundtrip(&bytes);
        let sack = p.selective_ack.unwrap();
        assert_eq!(sack.len_bits(), 32);
        // ack_nr = 1: bit 0 → 3, bit 2 → 5, bit 31 → 34.
        let acked: Vec<u16> = sack.acked(p.header.ack_nr).map(SeqNr::get).collect();
        assert_eq!(acked, vec![3, 5, 34]);

        // ST_DATA with an 8-byte selective ACK and payload.
        let mut bytes = header_bytes(0x01, 1);
        bytes.extend_from_slice(&[0, 8, 1, 0, 0, 0, 0, 0, 0, 1]);
        bytes.extend_from_slice(&[0xde, 0xad]);
        let p = roundtrip(&bytes);
        assert_eq!(p.selective_ack.unwrap().bitmask().len(), 8);
        assert_eq!(p.payload, &[0xde, 0xad]);

        // The largest selective ACK.
        let mut bytes = header_bytes(0x21, 1);
        bytes.extend_from_slice(&[0, MAX_SACK_LEN as u8]);
        bytes.extend(std::iter::repeat_n(0xff, MAX_SACK_LEN));
        let p = roundtrip(&bytes);
        assert_eq!(
            p.selective_ack.unwrap().acked(SeqNr(0xffff)).count(),
            252 * 8
        );
    }

    #[test]
    fn acked_wraps_around() {
        let sack = SelectiveAck::new(&[0b1000_0001, 0, 0, 0]).unwrap();
        let acked: Vec<u16> = sack.acked(SeqNr(0xfffd)).map(SeqNr::get).collect();
        assert_eq!(acked, vec![0xffff, 6]);
        assert!(sack.is_set(0) && sack.is_set(7) && !sack.is_set(1));
        assert!(!sack.is_set(32) && !sack.is_set(usize::MAX));
    }

    #[test]
    fn unknown_extensions_are_skipped() {
        // libutp's extension bits (type 2, 8 bytes) on a SYN, then a SACK on
        // a STATE behind an unknown extension 3 (libtorrent's close reason).
        let mut syn = header_bytes(0x41, 2);
        syn.extend_from_slice(&[0, 8, 0, 0, 0, 0, 0, 0, 0, 0]);
        let p = Packet::decode(&syn).unwrap();
        assert_eq!(p.skipped_extensions, 1);
        assert!(p.selective_ack.is_none());
        assert_eq!(p.encode_to_vec().unwrap(), header_bytes(0x41, 0));

        let mut state = header_bytes(0x21, 3);
        state.extend_from_slice(&[1, 4, 0, 0, 0, 7]); // close reason → SACK
        state.extend_from_slice(&[0, 4, 1, 0, 0, 0]); // SACK → end
        let p = Packet::decode(&state).unwrap();
        assert_eq!(p.skipped_extensions, 1);
        assert_eq!(p.selective_ack.unwrap().bitmask(), &[1, 0, 0, 0]);

        // An unknown extension of length 0.
        let mut bytes = header_bytes(0x21, 200);
        bytes.extend_from_slice(&[0, 0]);
        assert_eq!(Packet::decode(&bytes).unwrap().skipped_extensions, 1);

        // Exactly MAX_EXTENSIONS is fine.
        let mut bytes = header_bytes(0x21, 9);
        for _ in 0..MAX_EXTENSIONS - 1 {
            bytes.extend_from_slice(&[9, 0]);
        }
        bytes.extend_from_slice(&[0, 0]);
        assert_eq!(
            Packet::decode(&bytes).unwrap().skipped_extensions as usize,
            MAX_EXTENSIONS
        );
    }

    #[test]
    fn malformed_packets_are_rejected() {
        use DecodeError::*;
        let cases: Vec<(Vec<u8>, DecodeError)> = vec![
            (vec![], TooShort),
            (header_bytes(0x41, 0)[..19].to_vec(), TooShort),
            (header_bytes(0x40, 0), BadVersion),
            (header_bytes(0x42, 0), BadVersion),
            (header_bytes(0x4f, 0), BadVersion),
            (header_bytes(0x51, 0), BadType),
            (header_bytes(0xf1, 0), BadType),
            // Extension named, record header missing or half there.
            (header_bytes(0x21, 1), ExtensionUnterminated),
            (
                [header_bytes(0x21, 1), vec![0]].concat(),
                ExtensionUnterminated,
            ),
            // Chain continues past the end.
            (
                [header_bytes(0x21, 7), vec![1, 0]].concat(),
                ExtensionUnterminated,
            ),
            // Declared length beyond the datagram.
            (
                [header_bytes(0x21, 1), vec![0, 8, 0, 0, 0, 0]].concat(),
                ExtensionTruncated,
            ),
            (
                [header_bytes(0x21, 7), vec![0, 255, 1]].concat(),
                ExtensionTruncated,
            ),
            // SACK lengths that are not a non-zero multiple of 4.
            ([header_bytes(0x21, 1), vec![0, 0]].concat(), BadSackLength),
            (
                [header_bytes(0x21, 1), vec![0, 3, 1, 2, 3]].concat(),
                BadSackLength,
            ),
            (
                [header_bytes(0x21, 1), vec![0, 5, 1, 2, 3, 4, 5]].concat(),
                BadSackLength,
            ),
            (
                [
                    header_bytes(0x21, 1),
                    vec![1, 4, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0],
                ]
                .concat(),
                DuplicateSack,
            ),
            // Payload on every type but ST_DATA.
            ([header_bytes(0x11, 0), vec![1]].concat(), UnexpectedPayload),
            ([header_bytes(0x21, 0), vec![1]].concat(), UnexpectedPayload),
            ([header_bytes(0x31, 0), vec![1]].concat(), UnexpectedPayload),
            ([header_bytes(0x41, 0), vec![1]].concat(), UnexpectedPayload),
            (
                [header_bytes(0x21, 1), vec![0, 4, 0, 0, 0, 0, 9]].concat(),
                UnexpectedPayload,
            ),
        ];
        for (bytes, expected) in cases {
            assert_eq!(Packet::decode(&bytes), Err(expected), "{bytes:02x?}");
            assert!(!expected.code().is_empty());
        }

        // A chain longer than MAX_EXTENSIONS, and a self-referencing one.
        let mut bytes = header_bytes(0x21, 9);
        for _ in 0..MAX_EXTENSIONS {
            bytes.extend_from_slice(&[9, 0]);
        }
        bytes.extend_from_slice(&[0, 0]);
        assert_eq!(Packet::decode(&bytes), Err(TooManyExtensions));
        let mut bytes = header_bytes(0x21, 9);
        bytes.extend(std::iter::repeat_n([9u8, 0], 1000).flatten());
        assert_eq!(Packet::decode(&bytes), Err(TooManyExtensions));
    }

    #[test]
    fn encode_rejects_payload_on_control_packets() {
        for t in [
            PacketType::Fin,
            PacketType::State,
            PacketType::Reset,
            PacketType::Syn,
        ] {
            let mut p = Packet::new(header(t));
            p.payload = b"x";
            let mut out = Vec::new();
            assert_eq!(p.encode(&mut out), Err(EncodeError::UnexpectedPayload));
            assert!(out.is_empty());
        }
    }

    #[test]
    fn selective_ack_constructor_bounds() {
        assert!(SelectiveAck::new(&[]).is_none());
        assert!(SelectiveAck::new(&[0; 3]).is_none());
        assert!(SelectiveAck::new(&[0; 4]).is_some());
        assert!(SelectiveAck::new(&[0; MAX_SACK_LEN]).is_some());
        assert!(SelectiveAck::new(&[0; MAX_SACK_LEN + 4]).is_none());
    }

    #[test]
    fn sequence_comparisons_wrap() {
        let s = SeqNr;
        assert!(s(1).is_before(s(2)) && s(2).is_after(s(1)));
        assert!(s(65535).is_before(s(0)));
        assert!(s(0).is_after(s(65535)));
        assert!(s(65530).is_before(s(5)));
        assert!(s(5).is_after(s(65530)));
        assert!(!s(7).is_before(s(7)) && !s(7).is_after(s(7)));
        // Half the ring apart: neither.
        assert!(!s(0).is_before(s(0x8000)) && !s(0x8000).is_before(s(0)));
        assert!(s(0).is_before(s(0x7fff)));
        assert!(s(0).is_after(s(0x8001)));
        assert_eq!(s(65535).add(1), s(0));
        assert_eq!(s(65535).add(3), s(2));
        assert_eq!(s(0).sub(1), s(65535));
        assert_eq!(s(2).distance_from(s(65534)), 4);
        assert_eq!(s(65534).distance_from(s(2)), 65532);
        // Exhaustive around the wrap: a is before a + d for 1 ≤ d < 0x8000.
        for a in [0u16, 1, 0x7fff, 0x8000, 0xfff0, 0xffff] {
            for d in [1u16, 2, 100, 0x7ffe, 0x7fff] {
                let (x, y) = (s(a), s(a).add(d));
                assert!(x.is_before(y) && y.is_after(x), "{a} + {d}");
                assert!(!y.is_before(x) && !x.is_after(y), "{a} + {d}");
                assert_eq!(y.distance_from(x), d);
            }
        }
    }

    #[test]
    fn timestamps_are_wrapping_micros() {
        let epoch = Instant::now();
        assert_eq!(timestamp_micros(epoch, epoch), 0);
        assert_eq!(
            timestamp_micros(epoch, epoch + Duration::from_micros(1_234_567)),
            1_234_567
        );
        // 2^32 µs later the clock wraps.
        let wrap = Duration::from_micros(1 << 32);
        assert_eq!(
            timestamp_micros(epoch, epoch + wrap + Duration::from_micros(5)),
            5
        );
        // Before the epoch saturates at 0.
        assert_eq!(timestamp_micros(epoch + Duration::from_secs(1), epoch), 0);
        assert_eq!(timestamp_difference(1_000, 400), 600);
        assert_eq!(timestamp_difference(10, u32::MAX - 9), 20);
    }

    fn random_packet(
        rng: &mut StdRng,
        sack_buf: &mut [u8],
        payload_buf: &mut [u8],
    ) -> (Header, usize, usize) {
        let packet_type = PacketType::from_u8(rng.gen_range(0..=4)).unwrap();
        let header = Header {
            packet_type,
            connection_id: rng.gen(),
            timestamp_us: rng.gen(),
            timestamp_difference_us: rng.gen(),
            wnd_size: rng.gen(),
            seq_nr: SeqNr(rng.gen()),
            ack_nr: SeqNr(rng.gen()),
        };
        let sack_len = if rng.gen_bool(0.5) {
            4 * rng.gen_range(1..=MAX_SACK_LEN / 4)
        } else {
            0
        };
        rng.fill(&mut sack_buf[..sack_len]);
        let payload_len = if packet_type == PacketType::Data {
            rng.gen_range(0..=payload_buf.len())
        } else {
            0
        };
        rng.fill(&mut payload_buf[..payload_len]);
        (header, sack_len, payload_len)
    }

    #[test]
    fn property_valid_packets_roundtrip() {
        let mut rng = StdRng::seed_from_u64(0x5554_5029);
        let mut sack_buf = [0u8; MAX_SACK_LEN];
        let mut payload_buf = [0u8; 1500];
        for _ in 0..20_000 {
            let (header, sack_len, payload_len) =
                random_packet(&mut rng, &mut sack_buf, &mut payload_buf);
            let packet = Packet {
                header,
                selective_ack: (sack_len > 0)
                    .then(|| SelectiveAck::new(&sack_buf[..sack_len]).unwrap()),
                payload: &payload_buf[..payload_len],
                skipped_extensions: 0,
            };
            let bytes = packet.encode_to_vec().unwrap();
            assert_eq!(bytes.len(), packet.encoded_len());
            assert_eq!(Packet::decode(&bytes), Ok(packet));
        }
    }

    #[test]
    fn property_random_bytes_never_panic() {
        let mut rng = StdRng::seed_from_u64(0xbe90_0029);
        let mut buf = [0u8; 600];
        let mut accepted = 0;
        for i in 0..200_000 {
            let len = rng.gen_range(0..=buf.len());
            let bytes = &mut buf[..len];
            rng.fill(&mut *bytes);
            // Bias half the inputs towards plausible headers so the
            // extension walk and payload checks get exercised too.
            if i % 2 == 0 && len >= 2 {
                bytes[0] = (rng.gen_range(0..=5u8) << 4) | VERSION;
                bytes[1] = rng.gen_range(0..=3);
            }
            let bytes: &[u8] = bytes;
            let Ok(packet) = Packet::decode(bytes) else {
                continue;
            };
            accepted += 1;
            assert!(packet.header.packet_type as u8 <= 4);
            assert!(packet.payload.is_empty() || packet.header.packet_type == PacketType::Data);
            let encoded = packet.encode_to_vec().unwrap();
            if packet.skipped_extensions == 0 {
                // Without unknown extensions the bytes come back identical.
                assert_eq!(encoded, bytes);
            }
            // Re-encoding is stable either way.
            let again = Packet::decode(&encoded).unwrap();
            assert_eq!(again.header, packet.header);
            assert_eq!(again.selective_ack, packet.selective_ack);
            assert_eq!(again.payload, packet.payload);
            assert_eq!(again.skipped_extensions, 0);
        }
        assert!(accepted > 1000, "only {accepted} random inputs decoded");
    }
}
