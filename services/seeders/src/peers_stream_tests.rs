//! Integration tests of the streamed `GET /peers`: the real router on a
//! loopback port, fake UDP (BEP 15) and HTTP (BEP 3) trackers on loopback,
//! no DHT.

use crate::cache::PeerCache;
use crate::config::Config;
use crate::lookup::Lookup;
use crate::{discovery, router, AppState};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

const IH: &str = "0123456789abcdef0123456789abcdef01234567";

fn compact(peers: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in peers {
        let addr: SocketAddr = p.parse().unwrap();
        if let SocketAddr::V4(v4) = addr {
            out.extend_from_slice(&v4.ip().octets());
            out.extend_from_slice(&v4.port().to_be_bytes());
        }
    }
    out
}

/// A BEP 15 tracker answering every announce with `peers` after `delay`
/// (None: never answers). Counts announces.
async fn udp_tracker(
    peers: Vec<&'static str>,
    delay: Option<Duration>,
) -> (String, Arc<AtomicUsize>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", socket.local_addr().unwrap());
    let announces = Arc::new(AtomicUsize::new(0));
    let count = announces.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            let Ok((n, from)) = socket.recv_from(&mut buf).await else {
                return;
            };
            let Some(delay) = delay else { continue };
            if n < 16 {
                continue;
            }
            let action = u32::from_be_bytes(buf[8..12].try_into().unwrap());
            let txn = &buf[12..16];
            let mut reply = Vec::new();
            if action == 0 {
                reply.extend_from_slice(&0u32.to_be_bytes());
                reply.extend_from_slice(txn);
                reply.extend_from_slice(&42u64.to_be_bytes());
            } else if action == 1 && n >= 98 {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(delay).await;
                reply.extend_from_slice(&1u32.to_be_bytes());
                reply.extend_from_slice(txn);
                reply.extend_from_slice(&1800u32.to_be_bytes());
                reply.extend_from_slice(&0u32.to_be_bytes());
                reply.extend_from_slice(&(peers.len() as u32).to_be_bytes());
                reply.extend_from_slice(&compact(&peers));
            } else {
                continue;
            }
            let _ = socket.send_to(&reply, from).await;
        }
    });
    (url, announces)
}

/// A BEP 3 tracker answering with `body` (or a 500 if None) after `delay`.
async fn http_tracker(
    peers: Option<Vec<&'static str>>,
    delay: Duration,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let announces = Arc::new(AtomicUsize::new(0));
    let count = announces.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let count = count.clone();
            let peers = peers.clone();
            tokio::spawn(async move {
                let mut req = Vec::new();
                let mut buf = [0u8; 1024];
                while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => req.extend_from_slice(&buf[..n]),
                    }
                }
                count.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(delay).await;
                let response = match peers {
                    Some(peers) => {
                        let c = compact(&peers);
                        let mut body = format!("d8:intervali1800e5:peers{}:", c.len()).into_bytes();
                        body.extend_from_slice(&c);
                        body.push(b'e');
                        let mut r = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).into_bytes();
                        r.extend_from_slice(&body);
                        r
                    }
                    None => b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
                };
                let _ = sock.write_all(&response).await;
            });
        }
    });
    (url, announces)
}

struct Server {
    base: String,
}

async fn serve(trackers: Vec<String>, tune: impl FnOnce(&mut Config)) -> Server {
    let mut cfg = Config::from_env();
    cfg.default_trackers = trackers;
    cfg.max_trackers = 20;
    cfg.peers_stream_deadline = Duration::from_secs(4);
    cfg.peers_per_ip_per_minute = 100;
    cfg.peers_streams_per_ip = 10;
    cfg.peers_streams_total = 100;
    cfg.allowed_origins = crate::config::AllowedOrigins::List(vec!["https://site.example".into()]);
    tune(&mut cfg);
    let cfg = Arc::new(cfg);
    let lookup = Arc::new(Lookup::trackers_only_for_tests(reqwest::Client::new()));
    let cache = Arc::new(PeerCache::new(100));
    let swarm = crate::swarm::SwarmService::new(cfg.clone(), lookup.clone(), cache.clone());
    let state = AppState {
        stream_cache: Arc::new(PeerCache::new(100)),
        limiter: discovery::Limiter::new(discovery::StreamLimits {
            per_ip_per_minute: cfg.peers_per_ip_per_minute,
            per_ip_streams: cfg.peers_streams_per_ip,
            total_streams: cfg.peers_streams_total,
        }),
        config: cfg,
        lookup,
        cache,
        swarm,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(
            listener,
            router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server { base }
}

/// Every line of the response, with the time it arrived since the request.
async fn lines(url: &str) -> (u16, Vec<(Duration, serde_json::Value)>, Duration) {
    let started = Instant::now();
    let mut resp = reqwest::get(url).await.unwrap();
    let status = resp.status().as_u16();
    let mut out = Vec::new();
    let mut pending = String::new();
    while let Some(chunk) = resp.chunk().await.unwrap() {
        pending.push_str(std::str::from_utf8(&chunk).unwrap());
        while let Some(i) = pending.find('\n') {
            let line: String = pending.drain(..=i).collect();
            out.push((
                started.elapsed(),
                serde_json::from_str(line.trim_end()).unwrap(),
            ));
        }
    }
    assert!(pending.is_empty(), "unterminated line {pending:?}");
    (status, out, started.elapsed())
}

fn peers_of(lines: &[(Duration, serde_json::Value)]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|(_, v)| v.get("peer").and_then(|p| p.as_str()).map(str::to_string))
        .collect()
}

#[tokio::test]
async fn streams_each_source_as_it_answers_deduplicated_and_filtered() {
    let (fast, _) = udp_tracker(
        vec![
            "8.8.8.8:6881",
            "8.8.4.4:6881",
            "10.0.0.1:6881",
            "8.8.8.8:80",
        ],
        Some(Duration::ZERO),
    )
    .await;
    let (slow, _) = http_tracker(
        Some(vec!["8.8.4.4:6881", "9.9.9.9:51413"]),
        Duration::from_millis(1500),
    )
    .await;
    let (broken, _) = http_tracker(None, Duration::ZERO).await;
    let server = serve(vec![fast, slow, broken], |_| {}).await;

    let (status, got, total) = lines(&format!("{}/peers?ih={IH}", server.base)).await;
    assert_eq!(status, 200);
    let peers = peers_of(&got);
    assert_eq!(peers, ["8.8.8.8:6881", "8.8.4.4:6881", "9.9.9.9:51413"]);
    // The fast tracker's peers did not wait for the slow one.
    assert!(
        got[0].0 < Duration::from_millis(1000),
        "first line after {:?}",
        got[0].0
    );
    assert!(got[2].0 >= Duration::from_millis(1400));
    let end = &got.last().unwrap().1;
    assert_eq!(end["done"], true);
    assert_eq!(end["count"], 3);
    assert_eq!(end["cached"], false);
    assert_eq!(end["complete"], true);
    // Every source finished: no need to wait for the deadline.
    assert!(total < Duration::from_secs(3));
}

#[tokio::test]
async fn closes_at_the_deadline_while_sources_are_still_silent() {
    let (fast, _) = udp_tracker(vec!["8.8.8.8:6881"], Some(Duration::ZERO)).await;
    let (silent_udp, _) = udp_tracker(vec![], None).await;
    let (silent_http, _) =
        http_tracker(Some(vec!["9.9.9.9:6881"]), Duration::from_secs(3600)).await;
    let server = serve(vec![fast, silent_udp, silent_http], |c| {
        c.peers_stream_deadline = Duration::from_secs(2)
    })
    .await;
    let (_, got, total) = lines(&format!("{}/peers?ih={IH}", server.base)).await;
    assert_eq!(peers_of(&got), ["8.8.8.8:6881"]);
    assert!(got[0].0 < Duration::from_millis(500));
    // The silent HTTP tracker never answers: the stream still ends by the deadline.
    assert!(
        total >= Duration::from_millis(1400) && total < Duration::from_millis(2300),
        "closed after {total:?}"
    );
    assert_eq!(got.last().unwrap().1["done"], true);
}

#[tokio::test]
async fn a_second_request_within_the_ttl_is_served_from_the_cache() {
    let (tracker, announces) =
        udp_tracker(vec!["8.8.8.8:6881", "1.1.1.1:6881"], Some(Duration::ZERO)).await;
    let server = serve(vec![tracker], |_| {}).await;
    let url = format!("{}/peers?ih={IH}", server.base);
    let (_, first, _) = lines(&url).await;
    // Let the finished lookup land in the cache.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_, second, total) = lines(&url).await;
    assert_eq!(announces.load(Ordering::SeqCst), 1);
    let mut a = peers_of(&first);
    let mut b = peers_of(&second);
    a.sort();
    b.sort();
    assert_eq!(a, b);
    assert_eq!(second.last().unwrap().1["cached"], true);
    assert!(total < Duration::from_millis(500));
}

#[tokio::test]
async fn takes_the_magnet_trackers_and_v2_hashes() {
    let (tracker, announces) = udp_tracker(vec!["8.8.8.8:6881"], Some(Duration::ZERO)).await;
    let server = serve(vec![], |_| {}).await;
    let v2 = "caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e";
    let magnet = format!("magnet:?xt=urn:btmh:1220{v2}&tr={tracker}");
    let url = reqwest::Url::parse_with_params(
        &format!("{}/peers", server.base),
        &[("magnet", magnet.as_str())],
    )
    .unwrap();
    let (status, got, _) = lines(url.as_str()).await;
    assert_eq!(status, 200);
    assert_eq!(peers_of(&got), ["8.8.8.8:6881"]);
    assert_eq!(announces.load(Ordering::SeqCst), 1);
    // ih + magnet must agree.
    let url = reqwest::Url::parse_with_params(
        &format!("{}/peers", server.base),
        &[("ih", IH), ("magnet", magnet.as_str())],
    )
    .unwrap();
    assert_eq!(reqwest::get(url).await.unwrap().status(), 400);
}

#[tokio::test]
async fn bad_input_is_400_json_without_any_lookup() {
    let (tracker, announces) = udp_tracker(vec!["8.8.8.8:6881"], Some(Duration::ZERO)).await;
    let server = serve(vec![tracker], |_| {}).await;
    for query in [
        "ih=xyz",
        "ih=",
        "",
        "ih=0123456789abcdef0123456789abcdef0123456",
        "magnet=http%3A%2F%2Fx",
    ] {
        let resp = reqwest::get(format!("{}/peers?{query}", server.base))
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "{query}");
        assert!(resp.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json"));
        let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
        assert!(body["error"].is_string());
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(announces.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn over_the_limits_answers_429_without_starting_a_lookup() {
    let (tracker, announces) =
        udp_tracker(vec!["8.8.8.8:6881"], Some(Duration::from_millis(800))).await;
    let server = serve(vec![tracker], |c| {
        c.peers_per_ip_per_minute = 3;
        c.peers_streams_per_ip = 2;
    })
    .await;
    let other = "1111111111111111111111111111111111111111";
    // Two concurrent streams are fine, the third is refused.
    let a = reqwest::get(format!("{}/peers?ih={IH}", server.base))
        .await
        .unwrap();
    let b = reqwest::get(format!("{}/peers?ih={other}", server.base))
        .await
        .unwrap();
    let c = reqwest::get(format!(
        "{}/peers?ih=2222222222222222222222222222222222222222",
        server.base
    ))
    .await
    .unwrap();
    assert_eq!(
        (
            a.status().as_u16(),
            b.status().as_u16(),
            c.status().as_u16()
        ),
        (200, 200, 429)
    );
    assert_eq!(c.headers()["content-type"], "application/json");
    drop((a.text().await, b.text().await));
    // The per-minute budget (3) is spent after one more.
    assert_eq!(
        reqwest::get(format!("{}/peers?ih={IH}", server.base))
            .await
            .unwrap()
            .status(),
        200
    );
    let refused = reqwest::get(format!(
        "{}/peers?ih=3333333333333333333333333333333333333333",
        server.base
    ))
    .await
    .unwrap();
    assert_eq!(refused.status(), 429);
    assert_eq!(
        announces.load(Ordering::SeqCst),
        2,
        "a refused request started a lookup"
    );
}

#[tokio::test]
async fn cors_allows_only_the_configured_origin() {
    let server = serve(vec![], |_| {}).await;
    let client = reqwest::Client::new();
    let allowed = client
        .get(format!("{}/health", server.base))
        .header("origin", "https://site.example")
        .send()
        .await
        .unwrap();
    assert_eq!(
        allowed.headers()["access-control-allow-origin"],
        "https://site.example"
    );
    let foreign = client
        .get(format!("{}/health", server.base))
        .header("origin", "https://evil.example")
        .send()
        .await
        .unwrap();
    assert!(foreign
        .headers()
        .get("access-control-allow-origin")
        .is_none());
}
