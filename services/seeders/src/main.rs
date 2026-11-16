mod bencode;
mod cache;
mod config;
mod dht;
mod files;
mod lookup;
mod magnet;
mod metadata;
mod netguard;
mod peerwire;
mod probe;
mod resolver;
mod swarm;
#[cfg(test)]
mod testpeer;
mod token;
mod tracker;

use axum::extract::{Query, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use cache::PeerCache;
use config::{AllowedOrigins, Config};
use dht::DhtClient;
use lookup::{Lookup, Sources};
use resolver::GuardedResolver;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use swarm::SwarmService;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    lookup: Arc<Lookup>,
    cache: Arc<PeerCache>,
    swarm: Arc<SwarmService>,
}

#[tokio::main]
async fn main() {
    init_tracing();

    let cfg = Arc::new(Config::from_env());
    tracing::info!(?cfg, "starting magnet-seeders");

    let dht = Arc::new(DhtClient::new(cfg.dht_port).expect("failed to start DHT node"));
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

    let lookup = Arc::new(Lookup::new(http_client, dht));
    let cache = Arc::new(PeerCache::new(cfg.max_cache_entries));
    let swarm = SwarmService::new(cfg.clone(), lookup.clone(), cache.clone());
    swarm.spawn_refresher();

    let state = AppState {
        config: cfg.clone(),
        lookup,
        cache,
        swarm,
    };

    // Each route gets a timeout just above its own worst case: an announce,
    // plus (for /swarm) two rounds of probes, or (for /metadata) the
    // metadata deadline.
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
            get(metadata_handler)
                .layer(timeout_layer(cfg.deadline + cfg.metadata_deadline + slack)),
        )
        .route(
            "/files",
            get(files_handler).layer(timeout_layer(cfg.deadline + cfg.metadata_deadline + slack)),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    if let Some(cors) = build_cors(&cfg.allowed_origins) {
        app = app.layer(cors);
    }

    let listener = tokio::net::TcpListener::bind(cfg.bind_addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {}: {e}", cfg.bind_addr));
    tracing::info!(addr = %cfg.bind_addr, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
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

async fn peers_handler(State(state): State<AppState>, Query(query): Query<PeersQuery>) -> Response {
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

async fn metadata_handler(
    State(state): State<AppState>,
    Query(query): Query<PeersQuery>,
) -> Response {
    let (info_hash, magnet_trackers) = match resolve_query(query) {
        Ok(v) => v,
        Err(e) => return bad_request(e),
    };

    match state.swarm.metadata(info_hash, magnet_trackers).await {
        Ok(info) => (
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
            .into_response(),
        Err(e) => metadata_error(&e),
    }
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

fn metadata_error(e: &metadata::MetadataError) -> Response {
    let status = match e {
        metadata::MetadataError::Timeout => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::BAD_GATEWAY,
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
