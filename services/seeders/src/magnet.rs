//! Parsing of `magnet:` URIs (BEP 9) and bare info-hash strings.
//!
//! We only care about extracting the info-hash and the list of trackers
//! (`tr=`) — everything else in the magnet link (display name, exact
//! length, ...) is ignored. The info-hash is the v1 (SHA-1) `urn:btih:`
//! one, or for a v2-only torrent (BEP 52) the SHA-256 `urn:btmh:1220…`
//! truncated to its first 20 bytes, which is what DHT and trackers use.

use data_encoding::BASE32_NOPAD;
use thiserror::Error;

pub const INFO_HASH_LEN: usize = 20;
/// Longest magnet accepted (bytes); longer input is refused unparsed.
pub const MAX_MAGNET_LEN: usize = 8 * 1024;
/// `tr=` entries kept from one magnet; the rest are ignored.
pub const MAX_MAGNET_TRACKERS: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMagnet {
    pub info_hash: [u8; INFO_HASH_LEN],
    pub trackers: Vec<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MagnetError {
    #[error("empty input")]
    Empty,
    #[error("not a magnet: URI")]
    NotAMagnetUri,
    #[error("magnet URI has no usable xt=urn:btih or xt=urn:btmh info-hash")]
    NoInfoHash,
    #[error("magnet URI is too long")]
    TooLong,
    #[error("invalid info-hash: expected 40 hex, 32 base32 or 64 hex (v2) chars")]
    InvalidInfoHash,
}

/// Parse a full `magnet:?xt=urn:btih:...&tr=...` URI.
pub fn parse_magnet(input: &str) -> Result<ParsedMagnet, MagnetError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(MagnetError::Empty);
    }
    if input.len() > MAX_MAGNET_LEN {
        return Err(MagnetError::TooLong);
    }
    let rest = input
        .strip_prefix("magnet:")
        .ok_or(MagnetError::NotAMagnetUri)?;
    let query = rest.strip_prefix('?').unwrap_or(rest);

    let mut v1 = None;
    let mut v2 = None;
    let mut trackers = Vec::new();

    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "xt" => {
                // First valid hash of each kind wins; malformed ones are
                // ignored (a hybrid magnet carries one btih and one btmh).
                if let Some(hash_part) = value.strip_prefix("urn:btih:") {
                    if v1.is_none() {
                        v1 = decode_v1(hash_part).ok();
                    }
                } else if let Some(multihash) = value.strip_prefix("urn:btmh:") {
                    if v2.is_none() {
                        v2 = decode_btmh(multihash);
                    }
                }
            }
            "tr" if !value.is_empty() && trackers.len() < MAX_MAGNET_TRACKERS => {
                trackers.push(value.into_owned());
            }
            _ => {}
        }
    }

    // A hybrid's swarm is reachable under both; the v1 hash has more peers.
    let info_hash = v1.or(v2).ok_or(MagnetError::NoInfoHash)?;
    Ok(ParsedMagnet {
        info_hash,
        trackers,
    })
}

/// Decode a bare info-hash: 40 hex chars, 32 base32 chars (BEP 9 alt
/// form), or a v2 SHA-256 info-hash as 64 hex chars, truncated to its first
/// 20 bytes (BEP 52: what DHT, trackers and the handshake use).
pub fn decode_info_hash(s: &str) -> Result<[u8; INFO_HASH_LEN], MagnetError> {
    let s = s.trim();
    if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return truncate_v2(s);
    }
    decode_v1(s)
}

fn decode_v1(s: &str) -> Result<[u8; INFO_HASH_LEN], MagnetError> {
    let bytes = match s.len() {
        40 if s.bytes().all(|b| b.is_ascii_hexdigit()) => {
            hex::decode(s).map_err(|_| MagnetError::InvalidInfoHash)?
        }
        32 => BASE32_NOPAD
            .decode(s.to_ascii_uppercase().as_bytes())
            .map_err(|_| MagnetError::InvalidInfoHash)?,
        _ => return Err(MagnetError::InvalidInfoHash),
    };
    bytes.try_into().map_err(|_| MagnetError::InvalidInfoHash)
}

/// `1220` + 64 hex: the multihash of a SHA-256 v2 info-hash.
fn decode_btmh(multihash: &str) -> Option<[u8; INFO_HASH_LEN]> {
    let digest = multihash.strip_prefix("1220")?;
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    truncate_v2(digest).ok()
}

fn truncate_v2(hex64: &str) -> Result<[u8; INFO_HASH_LEN], MagnetError> {
    let bytes =
        hex::decode(&hex64[..2 * INFO_HASH_LEN]).map_err(|_| MagnetError::InvalidInfoHash)?;
    bytes.try_into().map_err(|_| MagnetError::InvalidInfoHash)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX_HASH: &str = "c9e15763f722f23e98a29decdfae341b98d53056"; // Ubuntu-style example
    const BASE32_HASH: &str = "ZHQVOY7XELZD5GFCTXWN7LRUDOMNKMCW"; // same 20 bytes as HEX_HASH

    #[test]
    fn parses_hex_hash_and_trackers() {
        let magnet = format!(
            "magnet:?xt=urn:btih:{HEX_HASH}&dn=example&tr=udp%3A%2F%2Ftracker.example.org%3A6969%2Fannounce&tr=https%3A%2F%2Ftracker2.example.org%2Fannounce"
        );
        let parsed = parse_magnet(&magnet).unwrap();
        assert_eq!(hex::encode(parsed.info_hash), HEX_HASH);
        assert_eq!(
            parsed.trackers,
            vec![
                "udp://tracker.example.org:6969/announce",
                "https://tracker2.example.org/announce",
            ]
        );
    }

    #[test]
    fn parses_base32_hash() {
        let magnet = format!("magnet:?xt=urn:btih:{BASE32_HASH}");
        let parsed = parse_magnet(&magnet).unwrap();
        // Base32 and hex encode the same 20 bytes; round-trip through hex for comparison.
        assert_eq!(parsed.info_hash.len(), 20);
        assert_eq!(decode_info_hash(BASE32_HASH).unwrap(), parsed.info_hash);
    }

    #[test]
    fn no_leading_question_mark_still_works() {
        let magnet = format!("magnet:xt=urn:btih:{HEX_HASH}");
        let parsed = parse_magnet(&magnet).unwrap();
        assert_eq!(hex::encode(parsed.info_hash), HEX_HASH);
    }

    #[test]
    fn missing_xt_is_an_error() {
        assert_eq!(parse_magnet("magnet:?dn=foo"), Err(MagnetError::NoInfoHash));
    }

    #[test]
    fn not_a_magnet_uri() {
        assert_eq!(
            parse_magnet("http://example.com"),
            Err(MagnetError::NotAMagnetUri)
        );
    }

    #[test]
    fn empty_input() {
        assert_eq!(parse_magnet(""), Err(MagnetError::Empty));
        assert_eq!(parse_magnet("   "), Err(MagnetError::Empty));
    }

    #[test]
    fn invalid_hash_length() {
        assert!(matches!(
            parse_magnet("magnet:?xt=urn:btih:deadbeef"),
            Err(MagnetError::NoInfoHash)
        ));
    }

    #[test]
    fn decode_info_hash_hex_and_base32_agree() {
        let via_hex = decode_info_hash(HEX_HASH).unwrap();
        let via_base32 = decode_info_hash(BASE32_HASH).unwrap();
        assert_eq!(via_hex, via_base32);
    }

    const V2_HEX: &str = "caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e";

    #[test]
    fn parses_v2_only_and_hybrid_magnets() {
        let v2 = parse_magnet(&format!("magnet:?xt=urn:btmh:1220{V2_HEX}")).unwrap();
        assert_eq!(hex::encode(v2.info_hash), &V2_HEX[..40]);
        // Hybrid: the v1 hash wins, in either order.
        for magnet in [
            format!("magnet:?xt=urn:btmh:1220{V2_HEX}&xt=urn:btih:{HEX_HASH}"),
            format!("magnet:?xt=urn:btih:{HEX_HASH}&xt=urn:btmh:1220{V2_HEX}"),
        ] {
            assert_eq!(
                hex::encode(parse_magnet(&magnet).unwrap().info_hash),
                HEX_HASH
            );
        }
        // Not SHA-256, or the wrong length: no usable hash.
        for bad in [
            format!("magnet:?xt=urn:btmh:1114{V2_HEX}"),
            format!("magnet:?xt=urn:btmh:1220{}", &V2_HEX[..62]),
        ] {
            assert_eq!(parse_magnet(&bad), Err(MagnetError::NoInfoHash));
        }
    }

    #[test]
    fn bare_v2_hash_is_truncated() {
        assert_eq!(
            hex::encode(decode_info_hash(V2_HEX).unwrap()),
            &V2_HEX[..40]
        );
    }

    #[test]
    fn caps_trackers_and_length() {
        let many: String = (0..80)
            .map(|i| format!("&tr=udp%3A%2F%2Ft{i}.example%3A1"))
            .collect();
        let parsed = parse_magnet(&format!("magnet:?xt=urn:btih:{HEX_HASH}{many}")).unwrap();
        assert_eq!(parsed.trackers.len(), MAX_MAGNET_TRACKERS);
        let huge = format!(
            "magnet:?xt=urn:btih:{HEX_HASH}&dn={}",
            "a".repeat(MAX_MAGNET_LEN)
        );
        assert_eq!(parse_magnet(&huge), Err(MagnetError::TooLong));
    }

    #[test]
    fn decode_info_hash_rejects_garbage() {
        assert!(decode_info_hash("not-a-hash").is_err());
        assert!(decode_info_hash("").is_err());
    }
}
