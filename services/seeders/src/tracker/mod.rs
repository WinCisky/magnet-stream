pub mod http;
pub mod udp;

use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum TrackerError {
    #[error("unsupported tracker scheme: {0}")]
    UnsupportedScheme(String),
    #[error("invalid tracker url: {0}")]
    InvalidUrl(String),
    #[error("dns resolution failed: {0}")]
    Dns(String),
    #[error("no public address for tracker host")]
    NoPublicAddress,
    #[error("network error: {0}")]
    Io(String),
    #[error("tracker reported a failure: {0}")]
    TrackerFailure(String),
    #[error("malformed tracker response: {0}")]
    Malformed(String),
    #[error("timed out")]
    Timeout,
}

/// Parameters for a single BEP 3 / BEP 15 announce, shared across every
/// tracker queried for one incoming API request.
#[derive(Debug, Clone)]
pub struct AnnounceParams {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub key: u32,
    pub numwant: i32,
    /// Let tracker URLs reach loopback/private addresses. Tests only: the
    /// SSRF guard (netguard) is on in every real configuration.
    pub allow_private: bool,
}

impl AnnounceParams {
    pub fn new(info_hash: [u8; 20]) -> Self {
        let mut peer_id = [0u8; 20];
        peer_id[0..8].copy_from_slice(b"-MS0001-");
        rand::Rng::fill(&mut rand::thread_rng(), &mut peer_id[8..]);
        Self {
            info_hash,
            peer_id,
            port: 6881,
            key: rand::random(),
            numwant: 200,
            allow_private: false,
        }
    }
}

/// Dispatch a single tracker announce by URL scheme. The caller is expected
/// to enforce an overall deadline around this call; `per_call_timeout` is
/// only a best-effort internal budget for the (possibly multi-round-trip)
/// protocol underneath.
pub async fn announce(
    http_client: &reqwest::Client,
    tracker_url: &str,
    params: &AnnounceParams,
    per_call_timeout: Duration,
) -> Result<Vec<SocketAddr>, TrackerError> {
    let url = url::Url::parse(tracker_url).map_err(|e| TrackerError::InvalidUrl(e.to_string()))?;
    match url.scheme() {
        "udp" => udp::announce(&url, params, per_call_timeout).await,
        "http" | "https" => http::announce(http_client, &url, params, per_call_timeout).await,
        other => Err(TrackerError::UnsupportedScheme(other.to_string())),
    }
}
