//! Parsing of `magnet:` URIs (BEP 9) and bare info-hash strings.
//!
//! We only care about extracting the v1 (SHA-1) info-hash and the list of
//! trackers (`tr=`) — everything else in the magnet link (display name,
//! exact length, ...) is ignored.

use data_encoding::BASE32_NOPAD;
use thiserror::Error;

pub const INFO_HASH_LEN: usize = 20;

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
    #[error("magnet URI has no usable xt=urn:btih (v1 BitTorrent) info-hash")]
    NoInfoHash,
    #[error("invalid info-hash: expected 40 hex chars or 32 base32 chars, got {0:?}")]
    InvalidInfoHash(String),
}

/// Parse a full `magnet:?xt=urn:btih:...&tr=...` URI.
pub fn parse_magnet(input: &str) -> Result<ParsedMagnet, MagnetError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(MagnetError::Empty);
    }
    let rest = input
        .strip_prefix("magnet:")
        .ok_or(MagnetError::NotAMagnetUri)?;
    let query = rest.strip_prefix('?').unwrap_or(rest);

    let mut info_hash = None;
    let mut trackers = Vec::new();

    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "xt" => {
                if info_hash.is_none() {
                    if let Some(hash_part) = value.strip_prefix("urn:btih:") {
                        // First valid v1 hash wins; ignore btmh (v2) and malformed ones,
                        // there may be several `xt` values (hybrid v1/v2 magnets).
                        if let Ok(hash) = decode_info_hash(hash_part) {
                            info_hash = Some(hash);
                        }
                    }
                }
            }
            "tr" if !value.is_empty() => {
                trackers.push(value.into_owned());
            }
            _ => {}
        }
    }

    let info_hash = info_hash.ok_or(MagnetError::NoInfoHash)?;
    Ok(ParsedMagnet {
        info_hash,
        trackers,
    })
}

/// Decode a bare info-hash: 40 hex chars, or 32 base32 chars (BEP 9 alt form).
pub fn decode_info_hash(s: &str) -> Result<[u8; INFO_HASH_LEN], MagnetError> {
    let s = s.trim();
    let bytes = match s.len() {
        40 if s.bytes().all(|b| b.is_ascii_hexdigit()) => {
            hex::decode(s).map_err(|_| MagnetError::InvalidInfoHash(s.to_string()))?
        }
        32 => BASE32_NOPAD
            .decode(s.to_ascii_uppercase().as_bytes())
            .map_err(|_| MagnetError::InvalidInfoHash(s.to_string()))?,
        _ => return Err(MagnetError::InvalidInfoHash(s.to_string())),
    };
    bytes
        .try_into()
        .map_err(|_| MagnetError::InvalidInfoHash(s.to_string()))
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

    #[test]
    fn decode_info_hash_rejects_garbage() {
        assert!(decode_info_hash("not-a-hash").is_err());
        assert!(decode_info_hash("").is_err());
    }
}
