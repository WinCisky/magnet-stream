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
    /// DHT bootstrap nodes (`DHT_BOOTSTRAP`): `None` the DHT library's
    /// defaults (public routers); an empty list (`none`) no bootstrap at all,
    /// so the node never contacts the public DHT (hermetic tests).
    pub dht_bootstrap: Option<Vec<String>>,
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
    /// The tunnel gateway `/v1/tunnel` (`TUNNEL_*`, docs/tunnel-protocol.md).
    pub tunnel: TunnelSettings,
}

/// Limits of the tunnel gateway. Same variable names and defaults as the
/// Cloudflare gateway (apps/gateway/src/tunnel/config.ts) where they exist;
/// a value that is not an integer within its range is logged and replaced
/// by the default.
#[derive(Debug, Clone)]
pub struct TunnelSettings {
    /// TCP connect timeout of an `open` (`TUNNEL_CONNECT_TIMEOUT_MS`, 5000, 100..=60000).
    pub connect_timeout: Duration,
    /// Wait for the remote's handshake (`TUNNEL_HANDSHAKE_TIMEOUT_MS`, 10000, 100..=60000).
    pub handshake_timeout: Duration,
    /// Connections per WebSocket (`TUNNEL_MAX_CONNECTIONS`, 6, 1..=64).
    pub max_connections: usize,
    /// A connection, or a WebSocket, without traffic is closed after this
    /// (`TUNNEL_IDLE_TIMEOUT_MS`, 90000, 1000..=600000).
    pub idle_timeout: Duration,
    /// Maximum session length (`TUNNEL_SESSION_TIMEOUT_S`, 7200, 5..=86400).
    pub session_timeout: Duration,
    /// WebSockets per session (`TUNNEL_SOCKETS_PER_SESSION`, 3, 1..=8).
    pub sockets_per_session: usize,
    /// Upgrades per minute per client IP (`TUNNEL_UPGRADES_PER_MINUTE`, 30, 1..=600).
    pub upgrades_per_minute: u32,
    /// Bytes held for a peer that does not read before its connection is
    /// closed (`TUNNEL_MAX_PENDING_WRITE_BYTES`, 4 MiB, 64 KiB..=256 MiB).
    pub max_pending_write: usize,
    /// Concurrent WebSockets per client IP (`TUNNEL_SOCKETS_PER_IP`, 12, 1..=1000).
    pub sockets_per_ip: u32,
    /// Concurrent WebSockets in total (`TUNNEL_MAX_SOCKETS`, 256, 1..=100000):
    /// with `TUNNEL_MAX_CONNECTIONS` it bounds the peer sockets (and file
    /// descriptors) the gateway holds.
    pub max_sockets: u32,
    /// Inbound peers (docs/tunnel-protocol.md › Announce and inbound
    /// connections): where the listener binds while a session announces
    /// (`TUNNEL_INBOUND_ADDR`, e.g. `0.0.0.0:6883`): TCP and, on the same
    /// port number, UDP for uTP, so the port must differ from `DHT_PORT`
    /// (a clash turns inbound peers off). Unset: no announce, no listener
    /// (`announced` UNSUPPORTED).
    pub inbound_addr: Option<SocketAddr>,
    /// Port announced to trackers and the DHT (`TUNNEL_ANNOUNCE_PORT`, 0 =
    /// the listener's own; else 1024..=65535, for a forwarded port).
    pub announce_port: u16,
    /// Re-announce period while a session announces (`TUNNEL_ANNOUNCE_INTERVAL_S`, 900, 60..=3600).
    pub announce_interval: Duration,
    /// Announce to the DHT too (`TUNNEL_ANNOUNCE_DHT`, on unless `0`/`false`).
    pub announce_dht: bool,
    /// Inbound connections per session (`TUNNEL_INBOUND_PER_SESSION`, 16, 1..=256).
    pub inbound_per_session: usize,
    /// Inbound connections in total (`TUNNEL_INBOUND_MAX`, 256, 1..=100000).
    pub inbound_max: usize,
    /// Inbound connections still handshaking, in total (`TUNNEL_INBOUND_PENDING`, 64, 1..=10000).
    pub inbound_pending: usize,
    /// Info-hashes one WebSocket may announce (`TUNNEL_ANNOUNCES_PER_SOCKET`, 2, 1..=16).
    pub announces_per_socket: usize,
    /// Info-hashes announced at once on the gateway (`TUNNEL_ANNOUNCES_MAX`, 64, 1..=10000).
    pub announces_max: usize,
    /// Info-hashes the WebSockets of one client IP (an IPv6 /64) may
    /// announce at once (`TUNNEL_ANNOUNCES_PER_IP`, 4, 1..=1000).
    pub announces_per_ip: usize,
    /// Announce rounds a client IP may start per minute: a new swarm, or
    /// trackers added to one (`TUNNEL_ANNOUNCE_ROUNDS_PER_IP_PER_MINUTE`, 6, 1..=600).
    pub announce_rounds_per_ip: u32,
    /// Announce rounds started per minute on the gateway, all clients
    /// together (`TUNNEL_ANNOUNCE_ROUNDS_PER_MINUTE`, 60, 1..=10000).
    pub announce_rounds_max: u32,
}

impl TunnelSettings {
    /// The inbound port takes uTP on UDP too, so it cannot be the DHT
    /// node's UDP port: on a clash inbound peers are turned off (logged).
    pub fn without_dht_clash(mut self, dht_port: u16) -> Self {
        if let Some(addr) = self.inbound_addr {
            if addr.port() != 0 && addr.port() == dht_port {
                tracing::warn!(
                    port = dht_port,
                    "TUNNEL_INBOUND_ADDR uses DHT_PORT (both need UDP): inbound peers are off"
                );
                self.inbound_addr = None;
            }
        }
        self
    }

    pub fn from_env() -> Self {
        Self {
            connect_timeout: Duration::from_millis(env_range(
                "TUNNEL_CONNECT_TIMEOUT_MS",
                5_000,
                100,
                60_000,
            )),
            handshake_timeout: Duration::from_millis(env_range(
                "TUNNEL_HANDSHAKE_TIMEOUT_MS",
                10_000,
                100,
                60_000,
            )),
            max_connections: env_range("TUNNEL_MAX_CONNECTIONS", 6, 1, 64) as usize,
            idle_timeout: Duration::from_millis(env_range(
                "TUNNEL_IDLE_TIMEOUT_MS",
                90_000,
                1_000,
                600_000,
            )),
            session_timeout: Duration::from_secs(env_range(
                "TUNNEL_SESSION_TIMEOUT_S",
                7_200,
                5,
                86_400,
            )),
            sockets_per_session: env_range("TUNNEL_SOCKETS_PER_SESSION", 3, 1, 8) as usize,
            upgrades_per_minute: env_range("TUNNEL_UPGRADES_PER_MINUTE", 30, 1, 600) as u32,
            max_pending_write: env_range(
                "TUNNEL_MAX_PENDING_WRITE_BYTES",
                4 * MIB,
                64 * 1024,
                256 * MIB,
            ) as usize,
            sockets_per_ip: env_range("TUNNEL_SOCKETS_PER_IP", 12, 1, 1_000) as u32,
            max_sockets: env_range("TUNNEL_MAX_SOCKETS", 256, 1, 100_000) as u32,
            inbound_addr: env_addr("TUNNEL_INBOUND_ADDR"),
            announce_port: match env_range("TUNNEL_ANNOUNCE_PORT", 0, 0, 65_535) {
                port @ (0 | 1024..) => port as u16,
                port => {
                    tracing::warn!(
                        port,
                        "TUNNEL_ANNOUNCE_PORT below 1024, using the listener's"
                    );
                    0
                }
            },
            announce_interval: Duration::from_secs(env_range(
                "TUNNEL_ANNOUNCE_INTERVAL_S",
                900,
                60,
                3_600,
            )),
            announce_dht: env_flag_or("TUNNEL_ANNOUNCE_DHT", true),
            inbound_per_session: env_range("TUNNEL_INBOUND_PER_SESSION", 16, 1, 256) as usize,
            inbound_max: env_range("TUNNEL_INBOUND_MAX", 256, 1, 100_000) as usize,
            inbound_pending: env_range("TUNNEL_INBOUND_PENDING", 64, 1, 10_000) as usize,
            announces_per_socket: env_range("TUNNEL_ANNOUNCES_PER_SOCKET", 2, 1, 16) as usize,
            announces_max: env_range("TUNNEL_ANNOUNCES_MAX", 64, 1, 10_000) as usize,
            announces_per_ip: env_range("TUNNEL_ANNOUNCES_PER_IP", 4, 1, 1_000) as usize,
            announce_rounds_per_ip: env_range("TUNNEL_ANNOUNCE_ROUNDS_PER_IP_PER_MINUTE", 6, 1, 600)
                as u32,
            announce_rounds_max: env_range("TUNNEL_ANNOUNCE_ROUNDS_PER_MINUTE", 60, 1, 10_000)
                as u32,
        }
    }
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
        let dht_port = env_parse("DHT_PORT", 6881u16);
        Config {
            bind_addr: env_parse("BIND_ADDR", "0.0.0.0:8080".parse().unwrap()),
            cache_ttl: env_secs("CACHE_TTL_SECS", 300),
            empty_cache_ttl: env_secs("EMPTY_CACHE_TTL_SECS", 30),
            deadline: env_millis("DEADLINE_MS", 3000),
            allowed_origins: AllowedOrigins::from_env(),
            default_trackers: default_trackers_from_env(),
            max_trackers: env_parse("MAX_TRACKERS", 20usize),
            peers_cache_bytes: env_parse("PEERS_CACHE_MB", 16u64).max(1) * MIB,
            dht_port,
            dht_bootstrap: dht_bootstrap_from_env(),
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
            allow_private_peers: env_flag("ALLOW_PRIVATE_PEERS"),
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
            tunnel: TunnelSettings::from_env().without_dht_clash(dht_port),
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

/// `DHT_BOOTSTRAP`: unset or empty = the library's defaults; `none` (or
/// `off`) = no bootstrap; else a comma-separated list of `host:port`.
fn dht_bootstrap_from_env() -> Option<Vec<String>> {
    parse_dht_bootstrap(env::var("DHT_BOOTSTRAP").ok().as_deref())
}

fn parse_dht_bootstrap(raw: Option<&str>) -> Option<Vec<String>> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.eq_ignore_ascii_case("none") || raw.eq_ignore_ascii_case("off") {
        return Some(Vec::new());
    }
    Some(
        raw.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
    )
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

/// A switch: `1` or `true` (any case) turns it on; unset, empty, `0` or
/// `false` leave it off; anything else is logged and leaves it off.
fn env_flag(key: &str) -> bool {
    let Ok(raw) = env::var(key) else {
        return false;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" => true,
        "" | "0" | "false" => false,
        _ => {
            tracing::warn!(key, value = %raw, "invalid switch, leaving it off");
            false
        }
    }
}

/// A switch with a default for unset or empty: `1`/`true` on, `0`/`false`
/// off; anything else is logged and gives the default.
fn env_flag_or(key: &str, default: bool) -> bool {
    let Ok(raw) = env::var(key) else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" => true,
        "0" | "false" => false,
        "" => default,
        _ => {
            tracing::warn!(key, value = %raw, "invalid switch, using the default");
            default
        }
    }
}

/// A socket address (`0.0.0.0:6881`, `[::]:6881`); unset or empty: None;
/// anything else is logged and gives None.
fn env_addr(key: &str) -> Option<SocketAddr> {
    let raw = env::var(key).ok()?;
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    match raw.parse() {
        Ok(addr) => Some(addr),
        Err(_) => {
            tracing::warn!(key, value = %raw, "invalid address, leaving it unset");
            None
        }
    }
}

/// An integer variable within `min..=max`; anything else is logged and
/// replaced by `default`.
fn env_range(key: &str, default: u64, min: u64, max: u64) -> u64 {
    let value = env_parse(key, default);
    if (min..=max).contains(&value) {
        value
    } else {
        tracing::warn!(
            key,
            value,
            min,
            max,
            "config value out of range, using default"
        );
        default
    }
}

fn env_secs(key: &str, default_secs: u64) -> Duration {
    Duration::from_secs(env_parse(key, default_secs))
}

fn env_millis(key: &str, default_millis: u64) -> Duration {
    Duration::from_millis(env_parse(key, default_millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dht_bootstrap_defaults_none_or_a_list() {
        assert_eq!(parse_dht_bootstrap(None), None);
        assert_eq!(parse_dht_bootstrap(Some(" ")), None);
        assert_eq!(parse_dht_bootstrap(Some("none")), Some(vec![]));
        assert_eq!(parse_dht_bootstrap(Some("OFF")), Some(vec![]));
        assert_eq!(
            parse_dht_bootstrap(Some("127.0.0.1:6881, router.example:6881,")),
            Some(vec![
                "127.0.0.1:6881".to_string(),
                "router.example:6881".to_string()
            ])
        );
    }

    #[test]
    fn the_inbound_port_cannot_be_the_dht_port() {
        let with = |addr: &str| TunnelSettings {
            inbound_addr: Some(addr.parse().unwrap()),
            ..TunnelSettings::from_env()
        };
        assert_eq!(
            with("0.0.0.0:6881").without_dht_clash(6881).inbound_addr,
            None
        );
        let other = with("0.0.0.0:6883").without_dht_clash(6881);
        assert_eq!(other.inbound_addr, Some("0.0.0.0:6883".parse().unwrap()));
        // Port 0 (tests) picks a free port, never the DHT's.
        let any = with("127.0.0.1:0").without_dht_clash(6881);
        assert!(any.inbound_addr.is_some());
    }
}
