mod bencode;
mod cache;
mod config;
mod dht;
mod discovery;
mod files;
mod info;
mod lookup;
mod magnet;
mod metadata;
mod netguard;
#[cfg(test)]
mod peers_stream_tests;
mod peerwire;
mod probe;
mod resolver;
mod scout;
mod swarm;
#[cfg(test)]
mod testpeer;
mod token;
mod tracker;
mod tunnel;
mod utp;

use axum::body::Body;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{header, Extensions, HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use cache::PeerCache;
use config::{AllowedOrigins, Config};
use dht::DhtClient;
use lookup::{Lookup, Sources};
use resolver::GuardedResolver;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use swarm::SwarmService;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    lookup: Arc<Lookup>,
    cache: Arc<PeerCache>,
    /// Finished streamed lookups (`/peers?ih=`), 5 minutes.
    stream_cache: Arc<PeerCache>,
    limiter: Arc<discovery::Limiter>,
    swarm: Arc<SwarmService>,
    /// Probes, scores and remembers the addresses `/peers` streams.
    scout: Arc<scout::Scout>,
    /// Verified info dicts: single-flight fetches and a byte-weighted cache.
    info: Arc<info::InfoService>,
}

#[tokio::main]
async fn main() {
    init_tracing();

    let cfg = Arc::new(Config::from_env());
    tracing::info!(?cfg, "starting magnet-seeders");

    let dht = Arc::new(
        DhtClient::with_bootstrap(cfg.dht_port, cfg.dht_bootstrap.as_deref())
            .expect("failed to start DHT node"),
    );
    // No bootstrap nodes (DHT_BOOTSTRAP=none): nothing to warm up.
    if cfg
        .dht_bootstrap
        .as_ref()
        .is_none_or(|nodes| !nodes.is_empty())
    {
        let dht = dht.clone();
        tokio::spawn(async move { dht.warm_up().await });
    }

    let http_client = reqwest::Client::builder()
        .dns_resolver(Arc::new(GuardedResolver))
        .timeout(cfg.deadline + Duration::from_secs(1))
        .connect_timeout(cfg.deadline)
        .user_agent(concat!("magnet-seeders/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("failed to build HTTP client");

    let announcer = Arc::new(tunnel::inbound::Announcer {
        http: http_client.clone(),
        dht: Some(dht.clone()),
        default_trackers: cfg.default_trackers.clone(),
        max_trackers: cfg.max_trackers,
    });
    let lookup = Arc::new(Lookup::new(http_client, dht));
    let cache = Arc::new(PeerCache::new(cfg.peers_cache_bytes));
    // One pool of probes for the scout and /swarm (PROBE_CONCURRENCY).
    let probe_permits = Arc::new(tokio::sync::Semaphore::new(cfg.probe_concurrency));
    let info = info::InfoService::new(cfg.info_settings());
    let swarm = SwarmService::new(
        cfg.clone(),
        lookup.clone(),
        cache.clone(),
        probe_permits.clone(),
        info.clone(),
    );
    swarm.spawn_refresher();

    let state = AppState {
        scout: scout::Scout::new(cfg.scout_settings(), scout::real_prober(), probe_permits),
        stream_cache: Arc::new(PeerCache::new(cfg.peers_cache_bytes)),
        limiter: discovery::Limiter::new(discovery::StreamLimits {
            per_ip_per_minute: cfg.peers_per_ip_per_minute,
            per_ip_streams: cfg.peers_streams_per_ip,
            total_streams: cfg.peers_streams_total,
        }),
        config: cfg.clone(),
        lookup,
        cache,
        swarm,
        info,
    };

    if cfg.allow_private_peers {
        tracing::warn!(
            "ALLOW_PRIVATE_PEERS is set: for local tests only (the tunnel honours it only for direct loopback requests)"
        );
    }
    // Tunnels end with a close frame (4002) when the process is asked to stop.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let tunnel = tunnel::Tunnel::with_announcer(
        cfg.tunnel.clone(),
        cfg.allowed_origins.clone(),
        cfg.allow_private_peers,
        cfg.client_ip_header.clone(),
        shutdown_rx,
        announcer,
    );
    let app = router(state).merge(tunnel::router(tunnel));
    let listener = tokio::net::TcpListener::bind(cfg.bind_addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {}: {e}", cfg.bind_addr));
    tracing::info!(addr = %cfg.bind_addr, "listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
        // Time for the tunnels' close frames to go out.
        tokio::time::sleep(Duration::from_millis(300)).await;
    })
    .await
    .expect("server error");
}

fn router(state: AppState) -> Router {
    let cfg = state.config.clone();
    // Each route gets a timeout just above its own worst case: an announce,
    // plus (for /swarm) two rounds of probes, or (for /metadata) the
    // metadata deadline. (For the streamed /peers it bounds the time to the
    // response head only; the body ends at the stream deadline.)
    let slack = Duration::from_secs(2);
    let mut app = Router::new()
        .route("/health", get(health_handler))
        .route(
            "/peers",
            get(peers_handler).layer(timeout_layer(cfg.deadline + Duration::from_secs(1))),
        )
        .route(
            "/swarm",
            get(swarm_handler).layer(timeout_layer(cfg.deadline + cfg.probe_timeout * 2 + slack)),
        )
        .route(
            "/metadata",
            get(metadata_handler).layer(timeout_layer(cfg.peers_stream_deadline + slack)),
        )
        .route(
            "/files",
            get(files_handler).layer(timeout_layer(cfg.deadline + cfg.metadata_deadline + slack)),
        )
        // Spans carry method and path only: query strings hold info-hashes
        // and magnets, which never go to the logs.
        .layer(
            TraceLayer::new_for_http().make_span_with(|req: &Request<Body>| {
                tracing::debug_span!("http", method = %req.method(), path = %req.uri().path())
            }),
        )
        .with_state(state);

    if let Some(cors) = build_cors(&cfg.allowed_origins) {
        app = app.layer(cors);
    }
    app
}

fn init_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn timeout_layer(after: Duration) -> TimeoutLayer {
    TimeoutLayer::with_status_code(StatusCode::GATEWAY_TIMEOUT, after)
}

fn build_cors(origins: &AllowedOrigins) -> Option<CorsLayer> {
    let allow_origin = match origins {
        AllowedOrigins::None => return None,
        AllowedOrigins::Any => AllowOrigin::any(),
        AllowedOrigins::List(list) => {
            let values: Vec<HeaderValue> = list
                .iter()
                .filter_map(|o| match HeaderValue::from_str(o) {
                    Ok(v) => Some(v),
                    Err(_) => {
                        tracing::warn!(origin = %o, "ignoring invalid ALLOWED_ORIGINS entry");
                        None
                    }
                })
                .collect();
            if values.is_empty() {
                return None;
            }
            AllowOrigin::list(values)
        }
    };

    Some(
        CorsLayer::new()
            .allow_origin(allow_origin)
            .allow_methods([Method::GET])
            .max_age(Duration::from_secs(86400)),
    )
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received, draining in-flight requests");
}

async fn health_handler() -> &'static str {
    "ok"
}

#[derive(Debug, Deserialize)]
struct PeersQuery {
    magnet: Option<String>,
    hash: Option<String>,
    /// Streamed `/peers`: 40 hex, 32 base32, or 64 hex (v2, truncated).
    ih: Option<String>,
}

#[derive(Serialize)]
struct PeersResponse {
    info_hash: String,
    peers: Vec<String>,
    count: usize,
    cached: bool,
    fetched_at: u64,
    expires_at: u64,
    sources: Sources,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

fn bad_request(msg: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorBody { error: msg.into() }),
    )
        .into_response()
}

/// `GET /peers`: with `ih=` and/or `magnet=`, the streamed NDJSON
/// discovery (docs/discovery-api.md); with the legacy `hash=`, the PoC's
/// JSON answer after a 3 s lookup (removed with /swarm in M5-01).
async fn peers_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    extensions: Extensions,
    Query(query): Query<PeersQuery>,
) -> Response {
    if query.hash.is_some() && query.ih.is_none() && query.magnet.is_none() {
        return legacy_peers(state, query).await;
    }
    let (info_hash, trackers) = match resolve_stream_query(&query) {
        Ok(v) => v,
        Err(e) => return bad_request(e),
    };
    let permit = match admit(&state, &headers, &extensions) {
        Ok(p) => p,
        Err(refusal) => return refused(refusal),
    };
    let body = Body::from_stream(discovery_stream(&state, info_hash, trackers, permit).await);
    (
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-ndjson"),
            ),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
            // Ask proxies (nginx) not to buffer: lines must reach the page as found.
            (
                header::HeaderName::from_static("x-accel-buffering"),
                HeaderValue::from_static("no"),
            ),
        ],
        body,
    )
        .into_response()
}

/// The per-IP and total limits of `/peers` (OQ-14), also applied to `/metadata`.
fn admit(
    state: &AppState,
    headers: &HeaderMap,
    extensions: &Extensions,
) -> Result<discovery::Permit, discovery::Refusal> {
    let ip = discovery::client_ip(
        extensions.get::<ConnectInfo<SocketAddr>>().map(|c| c.0),
        headers
            .get(state.config.client_ip_header.as_str())
            .and_then(|v| v.to_str().ok()),
    );
    state.limiter.try_acquire(ip, Instant::now())
}

/// 429 with the limit's code and `Retry-After: 30`.
fn refused(refusal: discovery::Refusal) -> Response {
    tracing::info!(why = refusal.code(), "request refused");
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(ErrorBody {
            error: refusal.code().to_string(),
        }),
    )
        .into_response();
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("30"));
    response
}

/// A discovery stream: replayed from the 5-minute cache, or a new lookup.
async fn discovery_stream(
    state: &AppState,
    info_hash: [u8; 20],
    trackers: Vec<String>,
    permit: discovery::Permit,
) -> impl futures_lite::Stream<Item = Result<bytes::Bytes, std::convert::Infallible>> + Send + 'static
{
    let source = match state.stream_cache.get(&info_hash).await {
        Some(cached) => discovery::Source::Cached(cached),
        None => discovery::Source::Lookup {
            lookup: state.lookup.clone(),
            cache: state.stream_cache.clone(),
            trackers,
        },
    };
    discovery::stream(
        state.scout.clone(),
        state.info.clone(),
        state.config.clone(),
        info_hash,
        source,
        permit,
    )
}

/// `ih` names the info-hash; `magnet` adds its `tr=` trackers (and names the
/// info-hash when `ih` is absent). Both given: they must agree.
fn resolve_stream_query(query: &PeersQuery) -> Result<([u8; 20], Vec<String>), String> {
    let ih = match query.ih.as_deref() {
        Some(raw) => Some(magnet::decode_info_hash(raw).map_err(|e| e.to_string())?),
        None => None,
    };
    let parsed = match query.magnet.as_deref().filter(|s| !s.trim().is_empty()) {
        Some(raw) => Some(magnet::parse_magnet(raw).map_err(|e| e.to_string())?),
        None => None,
    };
    match (ih, parsed) {
        (Some(ih), None) => Ok((ih, Vec::new())),
        (None, Some(m)) => Ok((m.info_hash, m.trackers)),
        (Some(ih), Some(m)) if m.info_hash == ih => Ok((ih, m.trackers)),
        (Some(_), Some(_)) => Err("`ih` and the magnet's info-hash differ".to_string()),
        (None, None) => Err("provide an `ih` (or `magnet`) query parameter".to_string()),
    }
}

async fn legacy_peers(state: AppState, query: PeersQuery) -> Response {
    let (info_hash, magnet_trackers) = match resolve_query(query) {
        Ok(v) => v,
        Err(e) => return bad_request(e),
    };

    let lookup = state.lookup.clone();
    let cfg = state.config.clone();
    let (result, cached) = state
        .cache
        .get_or_compute(info_hash, async move {
            Arc::new(lookup.run(info_hash, magnet_trackers, &cfg).await)
        })
        .await;

    let expires_at = result.fetched_at + result.ttl.as_secs();
    let remaining_secs = expires_at.saturating_sub(unix_now());

    let body = PeersResponse {
        info_hash: hex::encode(result.info_hash),
        peers: result.peers.iter().map(ToString::to_string).collect(),
        count: result.peers.len(),
        cached,
        fetched_at: result.fetched_at,
        expires_at,
        sources: result.sources.clone(),
    };

    let mut response = Json(body).into_response();
    if let Ok(value) = HeaderValue::from_str(&format!("public, max-age={remaining_secs}")) {
        response.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    response
}

async fn swarm_handler(State(state): State<AppState>, Query(query): Query<PeersQuery>) -> Response {
    let (info_hash, magnet_trackers) = match resolve_query(query) {
        Ok(v) => v,
        Err(e) => return bad_request(e),
    };

    let snapshot = state.swarm.snapshot(info_hash, magnet_trackers).await;
    let num_pieces = state
        .swarm
        .cached_metadata(info_hash)
        .await
        .map(|m| m.num_pieces);
    let token_exp = unix_now() + state.config.token_ttl.as_secs();
    let tokens = state
        .config
        .worker_token_secret
        .as_ref()
        .map(|secret| (secret, token_exp));

    let mut response = Json(swarm::response(&snapshot, num_pieces, tokens)).into_response();
    // Tokens are minted per response and clients poll for fresh probes.
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// `GET /metadata?ih=` (also `hash=` or `magnet=`): the raw verified info
/// dict. From the cache without contacting any peer; otherwise a discovery
/// runs (as for `/peers`, under the same per-IP limits, its lines unread)
/// and the first live peers offering `ut_metadata` are asked. 404 when no
/// peer offers a verified one, 504 at the deadline.
async fn metadata_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    extensions: Extensions,
    Query(query): Query<PeersQuery>,
) -> Response {
    let resolved = if query.ih.is_some() || query.magnet.is_some() {
        resolve_stream_query(&query)
    } else {
        resolve_query(query)
    };
    let (info_hash, trackers) = match resolved {
        Ok(v) => v,
        Err(e) => return bad_request(e),
    };
    let permit = match admit(&state, &headers, &extensions) {
        Ok(p) => p,
        Err(refusal) => return refused(refusal),
    };
    if let Some(info) = state.info.cached(&info_hash).await {
        return info_response(&info);
    }
    let deadline = tokio::time::Instant::now() + state.config.peers_stream_deadline;
    // The discovery feeds the metadata fetcher; nobody reads its lines.
    let mut lines = Box::pin(discovery_stream(&state, info_hash, trackers, permit).await);
    let (done_tx, done_rx) = tokio::sync::watch::channel(false);
    let drain = tokio::spawn(async move {
        use futures_lite::StreamExt;
        while lines.next().await.is_some() {}
        let _ = done_tx.send(true);
    });
    let _drain = AbortOnDrop(drain);
    loop {
        if let Some(info) = state.info.cached(&info_hash).await {
            return info_response(&info);
        }
        let discovering = !*done_rx.borrow();
        if let Some(mut flight) = state.info.watch(&info_hash) {
            let waited = tokio::time::timeout_at(deadline, flight.wait_for(Option::is_some)).await;
            match waited {
                Err(_) => return metadata_error(&metadata::MetadataError::Timeout),
                Ok(Ok(outcome)) => match outcome.clone() {
                    // Larger than the cache takes: served from the flight.
                    Some(Ok(info)) => return info_response(&info),
                    Some(Err(e)) if !discovering => return metadata_error(&e),
                    // Failed, but the discovery may still find another peer.
                    _ => {}
                },
                Ok(Err(_)) => {}
            }
        } else if !discovering {
            return metadata_error(&metadata::MetadataError::NoPeers);
        }
        if tokio::time::Instant::now() >= deadline {
            return metadata_error(&metadata::MetadataError::Timeout);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Aborts a task when dropped (the request went away).
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn info_response(info: &metadata::InfoDict) -> Response {
    (
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            ),
            // Content-addressed by the info-hash: it can never change.
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=31536000, immutable"),
            ),
        ],
        info.bytes.clone(),
    )
        .into_response()
}

/// The magnet's files, read from its info dict (fetched from peers first
/// if need be): name, total size, and every file's path, size and offset,
/// largest first. See `files.rs`.
async fn files_handler(State(state): State<AppState>, Query(query): Query<PeersQuery>) -> Response {
    let (info_hash, magnet_trackers) = match resolve_query(query) {
        Ok(v) => v,
        Err(e) => return bad_request(e),
    };

    let info = match state.swarm.metadata(info_hash, magnet_trackers).await {
        Ok(info) => info,
        Err(e) => return metadata_error(&e),
    };
    match files::listing(&hex::encode(info_hash), &info.bytes) {
        Ok(listing) => {
            let mut response = Json(listing).into_response();
            // Content-addressed by the info-hash: it can never change.
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=31536000, immutable"),
            );
            response
        }
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrorBody {
                error: e.to_string(),
            }),
        )
            .into_response(),
    }
}

/// 504 at the deadline, 503 with every fetch slot busy, else 404: no peer
/// offered an info dict that hashes to the info-hash.
fn metadata_error(e: &metadata::MetadataError) -> Response {
    let status = match e {
        metadata::MetadataError::Timeout => StatusCode::GATEWAY_TIMEOUT,
        metadata::MetadataError::Busy => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::NOT_FOUND,
    };
    let mut response = (
        status,
        Json(ErrorBody {
            error: e.to_string(),
        }),
    )
        .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn resolve_query(query: PeersQuery) -> Result<([u8; 20], Vec<String>), String> {
    if let Some(magnet) = query.magnet.filter(|s| !s.trim().is_empty()) {
        let parsed = magnet::parse_magnet(&magnet).map_err(|e| e.to_string())?;
        return Ok((parsed.info_hash, parsed.trackers));
    }
    if let Some(hash) = query.hash.filter(|s| !s.trim().is_empty()) {
        let info_hash = magnet::decode_info_hash(&hash).map_err(|e| e.to_string())?;
        return Ok((info_hash, Vec::new()));
    }
    Err("provide either a `magnet` or a `hash` query parameter".to_string())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
