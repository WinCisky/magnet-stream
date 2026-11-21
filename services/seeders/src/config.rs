//! Configuration, loaded entirely from environment variables. Every value has
//! a sane default and a bad value is logged and ignored (falls back to the
//! default) rather than crashing the process — this service should come up
//! even if it's misconfigured.

use std::env;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub cache_ttl: Duration,
    pub empty_cache_ttl: Duration,
    pub deadline: Duration,
    pub allowed_origins: AllowedOrigins,
    pub default_trackers: Vec<String>,
    pub max_trackers: usize,
    /// Byte budget of each discovery result cache (`PEERS_CACHE_MB`).
    pub peers_cache_bytes: u64,
    pub dht_port: u16,
    /// Max peer probes in flight across all swarms.
    pub probe_concurrency: usize,
    /// Max peers probed per swarm refresh, and per info-hash per
    /// `scout_reprobe` window by the scout.
    pub max_probe_peers: usize,
    pub probe_connect_timeout: Duration,
    /// Hard cap on one probe (connect + handshake + first messages).
    pub probe_timeout: Duration,
    /// How long a probe waits for an unchoke after saying it's interested.
    pub probe_unchoke_wait: Duration,
    /// Active swarms are re-announced and re-probed this often.
    pub refresh_interval: Duration,
    /// A swarm stays active this long after the last client request for it.
    pub active_ttl: Duration,
    /// A metadata fetch gives up after this long (`METADATA_DEADLINE_MS`).
    pub metadata_deadline: Duration,
    /// Byte budget of the info dict cache (`METADATA_CACHE_MB`).
    pub metadata_cache_bytes: u64,
    /// Larger info dicts are served but not cached (`METADATA_CACHE_MAX_ENTRY_MB`).
    pub metadata_cache_max_entry_bytes: usize,
    /// Metadata fetches in flight in total (`METADATA_FETCHES`).
    pub metadata_fetches: usize,
    /// Shared with magnet-worker, which only connects to peers carrying a
    /// token signed with it. Unset: no tokens are issued.
    pub worker_token_secret: Option<Secret>,
    pub token_ttl: Duration,
    /// Scout: probes in flight per `/peers` stream.
    pub scout_per_stream: usize,
    /// Scout: an address probed more recently is not probed again.
    pub scout_reprobe: Duration,
    /// Scout: scores fade toward "unknown" with this half-life.
    pub scout_half_life: Duration,
    /// Scout: an info-hash nobody asked about for this long is forgotten.
    pub scout_idle: Duration,
    /// Scout: byte budget of the scores in memory.
    pub scout_cache_bytes: u64,
    /// Scout: addresses remembered per info-hash.
    pub scout_max_addrs: usize,
    /// Allow probing loopback/private peers. Only for local testing.
    pub allow_private_peers: bool,
    /// Streamed `/peers`: the response closes after this long.
    pub peers_stream_deadline: Duration,
    /// Streamed `/peers`: requests per minute per client IP.
    pub peers_per_ip_per_minute: u32,
    /// Streamed `/peers`: concurrent streams per client IP.
    pub peers_streams_per_ip: u32,
    /// Streamed `/peers`: concurrent streams in total.
    pub peers_streams_total: u32,
    /// Header carrying the client IP, set by the reverse proxy (nginx's
    /// `X-Real-IP`); trusted only on connections from loopback.
    pub client_ip_header: String,
}

/// A config value that must never end up in logs (`Config` is logged at
/// startup).
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    #[cfg(test)]
    pub fn new(value: impl Into<String>) -> Self {
        Secret(value.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[derive(Debug, Clone)]
pub enum AllowedOrigins {
    /// No `ALLOWED_ORIGINS` configured: CORS headers are not sent, so only
    /// non-browser clients can use the API.
    None,
    /// `ALLOWED_ORIGINS=*`
    Any,
    /// `ALLOWED_ORIGINS=https://a.com,https://b.com`
    List(Vec<String>),
}

const DEFAULT_TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.dler.org:6969/announce",
    "http://tracker.openbittorrent.com:80/announce",
];

const MIB: u64 = 1024 * 1024;

impl Config {
    /// The scout's settings (docs/architecture.md › Scouting on the VPS).
    pub fn scout_settings(&self) -> crate::scout::ScoutSettings {
        crate::scout::ScoutSettings {
            timeouts: crate::probe::ProbeTimeouts {
                connect: self.probe_connect_timeout,
                total: self.probe_timeout,
                unchoke_wait: self.probe_unchoke_wait,
            },
            max_per_hash: self.max_probe_peers.max(1),
            reprobe_after: self.scout_reprobe,
            half_life: self.scout_half_life,
            idle: self.scout_idle,
            cache_bytes: self.scout_cache_bytes,
            max_addrs: self.scout_max_addrs,
        }
    }

    /// The info dict fetches and cache (docs/architecture.md › Scouting on the VPS, Metadata).
    pub fn info_settings(&self) -> crate::info::InfoSettings {
        crate::info::InfoSettings {
            deadline: self.metadata_deadline,
            cache_bytes: self.metadata_cache_bytes,
            max_entry_bytes: self.metadata_cache_max_entry_bytes,
            fetches: self.metadata_fetches,
            parallel: 3,
            connect_timeout: self.probe_connect_timeout,
        }
    }

    pub fn from_env() -> Self {
        Config {
            bind_addr: env_parse("BIND_ADDR", "0.0.0.0:8080".parse().unwrap()),
            cache_ttl: env_secs("CACHE_TTL_SECS", 300),
            empty_cache_ttl: env_secs("EMPTY_CACHE_TTL_SECS", 30),
            deadline: env_millis("DEADLINE_MS", 3000),
            allowed_origins: AllowedOrigins::from_env(),
            default_trackers: default_trackers_from_env(),
            max_trackers: env_parse("MAX_TRACKERS", 20usize),
            peers_cache_bytes: env_parse("PEERS_CACHE_MB", 16u64).max(1) * MIB,
            dht_port: env_parse("DHT_PORT", 6881u16),
            probe_concurrency: env_parse("PROBE_CONCURRENCY", 256usize).max(1),
            max_probe_peers: env_parse("MAX_PROBE_PEERS", 1000usize),
            probe_connect_timeout: env_millis("PROBE_CONNECT_TIMEOUT_MS", 2500),
            probe_timeout: env_millis("PROBE_TIMEOUT_MS", 5000),
            probe_unchoke_wait: env_millis("PROBE_UNCHOKE_WAIT_MS", 2500),
            refresh_interval: env_secs("REFRESH_INTERVAL_SECS", 300),
            active_ttl: env_secs("ACTIVE_TTL_SECS", 1800),
            metadata_deadline: env_millis("METADATA_DEADLINE_MS", 15_000),
            metadata_cache_bytes: env_parse("METADATA_CACHE_MB", 32u64).max(1) * MIB,
            metadata_cache_max_entry_bytes: (env_parse("METADATA_CACHE_MAX_ENTRY_MB", 4u64).max(1)
                * MIB) as usize,
            metadata_fetches: env_parse("METADATA_FETCHES", 4usize).max(1),
            worker_token_secret: env::var("WORKER_TOKEN_SECRET")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(Secret),
            token_ttl: env_secs("TOKEN_TTL_SECS", 900),
            scout_per_stream: env_parse("SCOUT_PER_STREAM", 64usize).max(1),
            scout_reprobe: env_secs("SCOUT_REPROBE_SECS", 600),
            scout_half_life: env_secs("SCOUT_HALF_LIFE_SECS", 1800).max(Duration::from_secs(1)),
            scout_idle: env_secs("SCOUT_IDLE_SECS", 7200).max(Duration::from_secs(1)),
            scout_cache_bytes: env_parse("SCOUT_CACHE_MB", 16u64).max(1) * MIB,
            scout_max_addrs: env_parse("SCOUT_MAX_ADDRS", 1000usize).max(1),
            allow_private_peers: env_parse("ALLOW_PRIVATE_PEERS", false),
            peers_stream_deadline: env_secs("PEERS_STREAM_SECS", 30)
                .clamp(Duration::from_secs(1), Duration::from_secs(120)),
            peers_per_ip_per_minute: env_parse("PEERS_PER_IP_PER_MINUTE", 30u32).max(1),
            peers_streams_per_ip: env_parse("PEERS_STREAMS_PER_IP", 4u32).max(1),
            peers_streams_total: env_parse("PEERS_STREAMS_TOTAL", 200u32).max(1),
            client_ip_header: env::var("CLIENT_IP_HEADER")
                .ok()
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "x-real-ip".to_string()),
        }
    }
}

impl AllowedOrigins {
    fn from_env() -> Self {
        match env::var("ALLOWED_ORIGINS") {
            Ok(raw) if raw.trim() == "*" => AllowedOrigins::Any,
            Ok(raw) if !raw.trim().is_empty() => {
                let list = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                if list.is_empty() {
                    AllowedOrigins::None
                } else {
                    AllowedOrigins::List(list)
                }
            }
            _ => AllowedOrigins::None,
        }
    }
}

fn default_trackers_from_env() -> Vec<String> {
    match env::var("DEFAULT_TRACKERS") {
        Ok(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        Err(_) => DEFAULT_TRACKERS.iter().map(|s| s.to_string()).collect(),
    }
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    match env::var(key) {
        Ok(raw) => raw.trim().parse().unwrap_or_else(|_| {
            tracing::warn!(key, value = %raw, "invalid config value, using default");
            default
        }),
        Err(_) => default,
    }
}

fn env_secs(key: &str, default_secs: u64) -> Duration {
    Duration::from_secs(env_parse(key, default_secs))
}

fn env_millis(key: &str, default_millis: u64) -> Duration {
    Duration::from_millis(env_parse(key, default_millis))
}
