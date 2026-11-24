//! The VPS tunnel gateway (M4-01): `GET /v1/tunnel`, the same protocol as
//! the Cloudflare gateway (docs/tunnel-protocol.md) for outbound TCP, so
//! the engine's `TunnelClient` uses either by changing only the URL. On top:
//! announce and inbound peers (M4-02, `inbound.rs`) and uTP, outbound and
//! inbound, on the announced port (M4-04e, `stream.rs`), whose dial is fit
//! for hole punching (M4-05, `punch.rs`).
//!
//! Checks, in the Cloudflare gateway's order, all before the upgrade:
//! `Origin` outside `ALLOWED_ORIGINS` → 403 (the WebSocket equivalent of
//! CORS, which browsers do not apply to upgrades; requests without `Origin`
//! are not from a page and pass), no upgrade → 426, subprotocol not offered
//! → 400, malformed `?session=` → 400, per-IP upgrade rate (30/min) → 429,
//! concurrent sockets per IP and in total → 429 / 503, session past its
//! maximum length → 410, session already holding its 3 WebSockets → 429.
//!
//! No browser authentication (decision OQ-09): access is limited by the
//! `Origin` check and, for non-browser clients that can forge it, by the
//! handshake gate and the limits. The client IP is only a limiter key and is
//! never logged.

pub mod codec;
pub mod gate;
pub mod inbound;
pub mod punch;
pub mod session;
pub mod stream;
#[cfg(test)]
mod tests;

use crate::config::{AllowedOrigins, TunnelSettings};
use crate::discovery::{self, Limiter, Refusal, StreamLimits};
use crate::utp::socket::SocketConfig;
use axum::extract::ws::{rejection::WebSocketUpgradeRejection, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{header, Extensions, HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use codec::SUBPROTOCOL;
use inbound::{Announcer, Inbound, InboundSettings};
use session::{Group, Limits};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::watch;

/// Largest WebSocket message accepted from a client (frames are at most
/// 1 MiB each; the engine sends data frames of 256 KiB, batched).
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

pub struct Tunnel {
    settings: TunnelSettings,
    origins: AllowedOrigins,
    /// `ALLOW_PRIVATE_PEERS`: honoured only for direct loopback requests (see `allow_private`).
    allow_private_peers: bool,
    client_ip_header: String,
    limiter: Arc<Limiter>,
    registry: Arc<Registry>,
    /// Announce and inbound peers; None without `TUNNEL_INBOUND_ADDR`.
    inbound: Option<Arc<Inbound>>,
    shutdown: watch::Receiver<bool>,
}

impl Tunnel {
    /// Sessions' own trackers only (tests).
    #[cfg(test)]
    pub fn new(
        settings: TunnelSettings,
        origins: AllowedOrigins,
        allow_private_peers: bool,
        client_ip_header: String,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        Self::with_announcer(
            settings,
            origins,
            allow_private_peers,
            client_ip_header,
            shutdown,
            Arc::new(Announcer::trackers_only()),
        )
    }

    /// With the announcer used by sessions that `announce` (trackers, DHT).
    pub fn with_announcer(
        settings: TunnelSettings,
        origins: AllowedOrigins,
        allow_private_peers: bool,
        client_ip_header: String,
        shutdown: watch::Receiver<bool>,
        announcer: Arc<Announcer>,
    ) -> Arc<Self> {
        let inbound = settings.inbound_addr.map(|listen| {
            Inbound::new(
                InboundSettings {
                    listen,
                    announce_port: settings.announce_port,
                    utp: utp_config(&settings),
                    interval: settings.announce_interval,
                    dht: settings.announce_dht,
                    per_session: settings.inbound_per_session,
                    max_inbound: settings.inbound_max,
                    max_pending: settings.inbound_pending,
                    per_socket: settings.announces_per_socket,
                    max_announces: settings.announces_max,
                    per_ip: settings.announces_per_ip,
                    rounds_per_ip: settings.announce_rounds_per_ip,
                    rounds_max: settings.announce_rounds_max,
                    handshake_timeout: settings.handshake_timeout,
                },
                announcer,
            )
        });
        let limiter = Limiter::new(StreamLimits {
            per_ip_per_minute: settings.upgrades_per_minute,
            per_ip_streams: settings.sockets_per_ip,
            total_streams: settings.max_sockets,
        });
        let registry = Arc::new(Registry {
            sessions: Mutex::new(HashMap::new()),
            per_session: settings.sockets_per_session,
            timeout: settings.session_timeout,
        });
        Arc::new(Self {
            settings,
            origins,
            allow_private_peers,
            client_ip_header,
            limiter,
            registry,
            inbound,
            shutdown,
        })
    }

    fn origin_allowed(&self, origin: Option<&str>) -> bool {
        match (origin, &self.origins) {
            (None, _) => true,
            (Some(_), AllowedOrigins::Any) => true,
            (Some(o), AllowedOrigins::List(list)) => list.iter().any(|a| a == o),
            (Some(_), AllowedOrigins::None) => false,
        }
    }

    /// `ALLOW_PRIVATE_PEERS` lets tunnels reach loopback/private peers only
    /// for a request made straight to a loopback listener on a loopback
    /// host, without the reverse proxy's client-IP header: a test on this
    /// machine. Through nginx (the header is set) or a public hostname, even
    /// a misconfigured variable cannot open the private network.
    fn allow_private(&self, peer: Option<SocketAddr>, headers: &HeaderMap) -> bool {
        if !self.allow_private_peers || headers.contains_key(self.client_ip_header.as_str()) {
            return false;
        }
        let loopback_peer = peer.is_some_and(|p| p.ip().is_loopback());
        let host = headers
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        loopback_peer && is_loopback_host(host)
    }
}

/// The inbound port's uTP socket: inbound connections and handshakes within
/// the `TUNNEL_INBOUND_*` limits, plus room for every WebSocket's outbound
/// connections. The connections' own timers are uTP's defaults (10 s to
/// complete the uTP handshake, 90 s idle): `TUNNEL_HANDSHAKE_TIMEOUT_MS`
/// bounds the BitTorrent handshake that follows, and
/// `TUNNEL_CONNECT_TIMEOUT_MS` an outbound connect, as for TCP.
fn utp_config(s: &TunnelSettings) -> SocketConfig {
    let outbound = s.max_sockets as usize * s.max_connections;
    SocketConfig {
        max_connections: s.inbound_max + s.inbound_pending + outbound,
        max_half_open: s.inbound_pending,
        accept_backlog: s.inbound_pending,
        ..SocketConfig::default()
    }
}

fn is_loopback_host(host: &str) -> bool {
    let name = match host.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or(""),
        None => host.rsplit_once(':').map_or(host, |(h, _)| h),
    };
    name.eq_ignore_ascii_case("localhost")
        || name.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// `/v1/tunnel` with its own state, to merge into the app's router.
pub fn router<S: Clone + Send + Sync + 'static>(tunnel: Arc<Tunnel>) -> Router<S> {
    Router::new()
        .route("/v1/tunnel", get(handler))
        .with_state(tunnel)
}

async fn handler(
    State(tunnel): State<Arc<Tunnel>>,
    headers: HeaderMap,
    extensions: Extensions,
    uri: Uri,
    upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let origin = headers
        .get(header::ORIGIN)
        .map(|o| o.to_str().unwrap_or("\u{0}"));
    if !tunnel.origin_allowed(origin) {
        tracing::info!(event = "tunnel.rejected", why = "origin");
        return error(StatusCode::FORBIDDEN, "origin_not_allowed");
    }
    let wants_upgrade = headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    if !wants_upgrade {
        let mut r = error(StatusCode::UPGRADE_REQUIRED, "websocket_required");
        r.headers_mut()
            .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        return r;
    }
    let Ok(upgrade) = upgrade else {
        return error(StatusCode::BAD_REQUEST, "bad_upgrade");
    };
    let offered = headers
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|p| p.trim() == SUBPROTOCOL);
    if !offered {
        tracing::info!(event = "tunnel.rejected", why = "subprotocol");
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "unsupported_protocol", "expected": SUBPROTOCOL})),
        )
            .into_response();
    }
    let Ok(name) = parse_session(uri.query()) else {
        return error(StatusCode::BAD_REQUEST, "bad_session");
    };

    let peer = extensions.get::<ConnectInfo<SocketAddr>>().map(|c| c.0);
    let ip = discovery::client_ip(
        peer,
        headers
            .get(tunnel.client_ip_header.as_str())
            .and_then(|v| v.to_str().ok()),
    );
    let permit = match tunnel.limiter.try_acquire(ip, Instant::now()) {
        Ok(p) => p,
        Err(refusal) => {
            let (status, code) = match refusal {
                Refusal::RequestsFromIp => (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
                Refusal::StreamsFromIp => (
                    StatusCode::TOO_MANY_REQUESTS,
                    "too_many_sockets_from_client",
                ),
                Refusal::TotalStreams => (StatusCode::SERVICE_UNAVAILABLE, "busy"),
            };
            tracing::info!(event = "tunnel.rejected", why = code);
            let mut r = error(status, code);
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
            return r;
        }
    };
    let seat = match tunnel.registry.join(name) {
        Ok(seat) => seat,
        Err(JoinError::Expired) => {
            tracing::info!(event = "tunnel.rejected", why = "session_expired");
            return error(StatusCode::GONE, "session_expired");
        }
        Err(JoinError::Full) => {
            tracing::info!(event = "tunnel.rejected", why = "session_full");
            return error(StatusCode::TOO_MANY_REQUESTS, "session_full");
        }
    };

    let s = &tunnel.settings;
    let limits = Arc::new(Limits {
        connect_timeout: s.connect_timeout,
        handshake_timeout: s.handshake_timeout,
        max_connections: s.max_connections,
        idle_timeout: s.idle_timeout,
        session_timeout: s.session_timeout,
        max_pending_write: s.max_pending_write,
        allow_private: tunnel.allow_private(peer, &headers),
    });
    let shutdown = tunnel.shutdown.clone();
    let inbound = tunnel.inbound.clone();
    upgrade
        .protocols([SUBPROTOCOL])
        .max_message_size(MAX_MESSAGE_BYTES)
        .max_frame_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |ws| async move {
            // Held for the WebSocket's lifetime: the limiter's socket counts
            // and the session's seat.
            let _permit = permit;
            let group = seat.group.clone();
            session::run(ws, limits, group, inbound, ip, shutdown).await;
            drop(seat);
        })
}

fn error(status: StatusCode, code: &str) -> Response {
    let mut r = (status, Json(serde_json::json!({ "error": code }))).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// `?session=<32 lowercase hex>` names a session shared by up to 3
/// WebSockets; absent, the WebSocket is a session of its own.
#[allow(clippy::result_unit_err)]
pub fn parse_session(query: Option<&str>) -> Result<Option<String>, ()> {
    let mut found = None;
    for (k, v) in url::form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
        if k == "session" {
            if found.is_some() {
                return Err(());
            }
            found = Some(v.into_owned());
        }
    }
    match found {
        None => Ok(None),
        Some(v) if v.len() == 32 && v.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) => {
            Ok(Some(v))
        }
        Some(_) => Err(()),
    }
}

/// Sessions with a name, while they have a WebSocket: the start time (for
/// the 2 h cap) and the number of WebSockets. Bounded by the limiter's
/// total socket count; an entry goes with its last WebSocket.
struct Registry {
    sessions: Mutex<HashMap<String, (Arc<Group>, usize)>>,
    per_session: usize,
    timeout: std::time::Duration,
}

enum JoinError {
    Expired,
    Full,
}

/// One WebSocket's place in its session; dropping it frees the place.
struct Seat {
    registry: Arc<Registry>,
    name: Option<String>,
    group: Arc<Group>,
}

impl Drop for Seat {
    fn drop(&mut self) {
        let Some(name) = &self.name else { return };
        let mut sessions = self.registry.lock();
        if let Some((_, n)) = sessions.get_mut(name) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                sessions.remove(name);
            }
        }
    }
}

impl Registry {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (Arc<Group>, usize)>> {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn join(self: &Arc<Self>, name: Option<String>) -> Result<Seat, JoinError> {
        let Some(name) = name else {
            return Ok(Seat {
                registry: self.clone(),
                name: None,
                group: Arc::new(Group::new()),
            });
        };
        let mut sessions = self.lock();
        let (group, n) = sessions
            .entry(name.clone())
            .or_insert_with(|| (Arc::new(Group::new()), 0));
        if group.is_expired(self.timeout) {
            return Err(JoinError::Expired);
        }
        if *n >= self.per_session {
            return Err(JoinError::Full);
        }
        *n += 1;
        let group = group.clone();
        drop(sessions);
        Ok(Seat {
            registry: self.clone(),
            name: Some(name),
            group,
        })
    }
}
