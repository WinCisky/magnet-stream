//! Peer tokens for magnet-worker. The worker only opens a TCP connection to a
//! peer if the browser hands it a token for that exact (info-hash, peer
//! address), signed by us and not expired. That stops it from being an open
//! "connect anywhere" relay while costing it a single HMAC check.
//!
//! Format (must match magnet-worker's `src/token.ts`):
//! `base64url_nopad(HMAC-SHA256(secret, "mw1|<info-hash hex>|<addr>|<exp>"))`
//! where `<addr>` is `SocketAddr::to_string()` and `<exp>` is unix seconds.

use crate::config::Secret;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

pub fn sign(secret: &Secret, info_hash_hex: &str, addr: &str, exp: u64) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(format!("mw1|{info_hash_hex}|{addr}|{exp}").as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The same vectors are asserted in magnet-worker's test suite.
    #[test]
    fn matches_shared_vectors() {
        let secret = Secret::new("test-secret");
        let ih = "c9e15763f722f23e98a29decdfae341b98d53056";
        assert_eq!(
            sign(&secret, ih, "1.2.3.4:6881", 1_700_000_000),
            "sRR_H4vDqUIPeXLWwQoGjkaZxnZT1mEvVTqga86kR18"
        );
        assert_eq!(
            sign(&secret, ih, "[2001:db8::1]:51413", 1_700_000_000),
            "QVnEb2t6E7ja-qO9EejYkj76CkPpvAiRV1XvntzHUVY"
        );
    }
}
