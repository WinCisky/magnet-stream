//! Integration tests of the streamed `GET /peers`: the real router on a
//! loopback port, fake UDP (BEP 15) and HTTP (BEP 3) trackers on loopback,
//! no DHT. The scout probes with a fake prober (no network) unless a test
//! uses the real one against fake peers on loopback.

use crate::cache::PeerCache;
use crate::config::Config;
use crate::lookup::Lookup;
use crate::peerwire::ExtensionHandshake;
use crate::probe::{Availability, PeerProbe};
use crate::scout::{self, Prober, Scout};
use crate::testpeer::{self, FakeAvailability, FakePeer};
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
    info: Arc<crate::info::InfoService>,
}

/// A prober that finds every address alive (a partial peer) at once.
fn alive_prober() -> Prober {
    Arc::new(|_, _, _| {
        Box::pin(async {
            PeerProbe {
                error: None,
                connect_ms: Some(20),
                fast: true,
                extensions: ExtensionHandshake::default(),
                availability: Availability::Bitfield(vec![0b1000_0000]),
                unchoke_ms: None,
            }
        })
    })
}

async fn serve(trackers: Vec<String>, tune: impl FnOnce(&mut Config)) -> Server {
    serve_with(trackers, tune, alive_prober()).await
}

async fn serve_with(
    trackers: Vec<String>,
    tune: impl FnOnce(&mut Config),
    prober: Prober,
) -> Server {
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
    let cache = Arc::new(PeerCache::new(1 << 20));
    let permits = Arc::new(tokio::sync::Semaphore::new(cfg.probe_concurrency));
    let info = crate::info::InfoService::new(cfg.info_settings());
    let swarm = crate::swarm::SwarmService::new(
        cfg.clone(),
        lookup.clone(),
        cache.clone(),
        permits.clone(),
        info.clone(),
    );
    let state = AppState {
        scout: Scout::new(cfg.scout_settings(), prober, permits),
        stream_cache: Arc::new(PeerCache::new(1 << 20)),
        limiter: discovery::Limiter::new(discovery::StreamLimits {
            per_ip_per_minute: cfg.peers_per_ip_per_minute,
            per_ip_streams: cfg.peers_streams_per_ip,
            total_streams: cfg.peers_streams_total,
        }),
        config: cfg,
        lookup,
        cache,
        swarm,
        info: info.clone(),
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
    Server { base, info }
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
    assert_eq!(end["alive"], 3);
    assert!(got[..3]
        .iter()
        .all(|(_, v)| v["alive"] == true && v["score"].is_number()));
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

/// A UDP tracker for loopback peers (the fake tracker takes `&'static str`).
async fn tracker_for(peers: &[SocketAddr]) -> String {
    let leaked: Vec<&'static str> = peers
        .iter()
        .map(|a| &*Box::leak(a.to_string().into_boxed_str()))
        .collect();
    udp_tracker(leaked, Some(Duration::ZERO)).await.0
}

/// Wraps a prober and counts its calls.
fn counted(inner: Prober, calls: Arc<AtomicUsize>) -> Prober {
    Arc::new(move |addr, ih, t| {
        calls.fetch_add(1, Ordering::SeqCst);
        inner(addr, ih, t)
    })
}

fn local(tune: impl FnOnce(&mut Config)) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.allow_private_peers = true;
        c.probe_connect_timeout = Duration::from_millis(500);
        c.probe_timeout = Duration::from_millis(1500);
        c.probe_unchoke_wait = Duration::from_millis(500);
        tune(c);
    }
}

#[tokio::test]
async fn scouts_real_peers_live_first_with_their_facts() {
    let ih_bytes: [u8; 20] = hex::decode(IH).unwrap().try_into().unwrap();
    let seed = testpeer::spawn(FakePeer::seed_with_metadata(
        ih_bytes,
        testpeer::fake_info_dict(4),
    ))
    .await;
    let partial = testpeer::spawn(FakePeer {
        availability: FakeAvailability::Bitfield(vec![0b1010_0000]),
        unchoke: false,
        ..FakePeer::seed(ih_bytes)
    })
    .await;
    // Refused: nothing listens there.
    let closed = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    // Accepts and never says a word.
    let silent_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent = silent_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = silent_listener.accept().await {
            held.push(sock);
        }
    });
    // Speaks something else.
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = http_listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = http_listener.accept().await {
            tokio::spawn(async move {
                let _ = sock
                    .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
                    .await;
                let mut buf = [0u8; 256];
                let _ = sock.read(&mut buf).await;
            });
        }
    });
    let tracker = tracker_for(&[closed, silent, http, partial, seed]).await;
    let server = serve_with(vec![tracker], local(|_| {}), scout::real_prober()).await;

    let (status, got, total) = lines(&format!("{}/peers?ih={IH}", server.base)).await;
    assert_eq!(status, 200);
    let by_peer = |addr: SocketAddr| {
        got.iter()
            .position(|(_, v)| v["peer"] == addr.to_string())
            .unwrap_or_else(|| panic!("{addr} missing"))
    };
    let (s, p) = (by_peer(seed), by_peer(partial));
    let seed_line = &got[s].1;
    assert_eq!(seed_line["alive"], true);
    assert_eq!(seed_line["seed"], true);
    assert_eq!(seed_line["md"], true);
    assert_eq!(seed_line["unchoked"], true);
    assert_eq!(seed_line["have"], 1.0);
    let partial_line = &got[p].1;
    assert_eq!(partial_line["alive"], true);
    assert_eq!(partial_line["seed"], false);
    assert_eq!(partial_line["md"], false);
    assert_eq!(partial_line["unchoked"], false);
    assert_eq!(partial_line["have"], 0.25);
    assert!(seed_line["score"].as_f64() > partial_line["score"].as_f64());
    for dead in [closed, silent, http] {
        let i = by_peer(dead);
        assert_eq!(got[i].1["alive"], false, "{dead}");
        assert!(got[i].1["score"].as_f64().unwrap() < 0.1);
        // Live lines come before every dead one.
        assert!(i > s && i > p);
    }
    let end = &got.last().unwrap().1;
    assert_eq!(
        (
            end["alive"].clone(),
            end["dead"].clone(),
            end["unknown"].clone()
        ),
        (2.into(), 3.into(), 0.into())
    );
    assert_eq!(end["count"], 5);
    assert_eq!(end["complete"], true);
    // Probes ended well before the stream deadline: no need to wait for it.
    assert!(total < Duration::from_secs(3), "{total:?}");
}

#[tokio::test]
async fn a_second_stream_gets_the_ranked_list_without_new_probes() {
    let ih_bytes: [u8; 20] = hex::decode(IH).unwrap().try_into().unwrap();
    let seed = testpeer::spawn(FakePeer::seed(ih_bytes)).await;
    let partial = testpeer::spawn(FakePeer {
        availability: FakeAvailability::Bitfield(vec![0b1000_0000]),
        unchoke: false,
        ..FakePeer::seed(ih_bytes)
    })
    .await;
    let tracker = tracker_for(&[partial, seed]).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let server = serve_with(
        vec![tracker],
        local(|_| {}),
        counted(scout::real_prober(), calls.clone()),
    )
    .await;
    let url = format!("{}/peers?ih={IH}", server.base);
    let _ = lines(&url).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_, again, total) = lines(&url).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2, "addresses probed again");
    // Best first, at once.
    assert_eq!(peers_of(&again), [seed.to_string(), partial.to_string()]);
    assert!(again[0].1["alive"] == true && again[1].1["alive"] == true);
    assert!(total < Duration::from_millis(500), "{total:?}");
    assert_eq!(again.last().unwrap().1["cached"], true);
}

#[tokio::test]
async fn a_client_reading_only_peer_still_gets_every_address() {
    let (tracker, _) =
        udp_tracker(vec!["8.8.8.8:6881", "9.9.9.9:6881"], Some(Duration::ZERO)).await;
    let server = serve(vec![tracker], |_| {}).await;
    let (_, got, _) = lines(&format!("{}/peers?ih={IH}", server.base)).await;
    let mut peers = peers_of(&got);
    peers.sort();
    assert_eq!(peers, ["8.8.8.8:6881", "9.9.9.9:6881"]);
    // Every non-final line is a peer line, the last one is `done`.
    assert!(got[..got.len() - 1]
        .iter()
        .all(|(_, v)| v["peer"].is_string()));
    assert_eq!(got.last().unwrap().1["done"], true);
}

/// A prober that tracks how many probes run at once, overall and per
/// info-hash, and answers "alive" after `delay`.
struct Gauge {
    now: AtomicUsize,
    max: AtomicUsize,
    per_hash: std::sync::Mutex<std::collections::HashMap<[u8; 20], (usize, usize)>>,
}

fn gauged(delay: Duration, gauge: Arc<Gauge>) -> Prober {
    Arc::new(move |_, ih, _| {
        let gauge = gauge.clone();
        Box::pin(async move {
            let n = gauge.now.fetch_add(1, Ordering::SeqCst) + 1;
            gauge.max.fetch_max(n, Ordering::SeqCst);
            {
                let mut map = gauge.per_hash.lock().unwrap();
                let e = map.entry(ih).or_default();
                e.0 += 1;
                e.1 = e.1.max(e.0);
            }
            tokio::time::sleep(delay).await;
            gauge.now.fetch_sub(1, Ordering::SeqCst);
            gauge.per_hash.lock().unwrap().get_mut(&ih).unwrap().0 -= 1;
            alive_prober()("127.0.0.1:1".parse().unwrap(), ih, scout_timeouts()).await
        })
    })
}

fn scout_timeouts() -> crate::probe::ProbeTimeouts {
    crate::probe::ProbeTimeouts {
        connect: Duration::from_secs(1),
        total: Duration::from_secs(2),
        unchoke_wait: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn probes_in_flight_respect_the_global_and_per_stream_limits() {
    let many: Vec<&'static str> = (0..30)
        .map(|i| &*Box::leak(format!("8.8.{i}.1:6881").into_boxed_str()))
        .collect();
    let (tracker, _) = udp_tracker(many, Some(Duration::ZERO)).await;
    let gauge = Arc::new(Gauge {
        now: AtomicUsize::new(0),
        max: AtomicUsize::new(0),
        per_hash: Default::default(),
    });
    let server = serve_with(
        vec![tracker],
        |c| {
            c.probe_concurrency = 6;
            c.scout_per_stream = 4;
        },
        gauged(Duration::from_millis(40), gauge.clone()),
    )
    .await;
    let hashes = [
        "1111111111111111111111111111111111111111",
        "2222222222222222222222222222222222222222",
        "3333333333333333333333333333333333333333",
    ];
    let runs = hashes.map(|h| {
        let url = format!("{}/peers?ih={h}", server.base);
        tokio::spawn(async move { lines(&url).await })
    });
    for run in runs {
        let (_, got, _) = run.await.unwrap();
        assert_eq!(got.last().unwrap().1["alive"], 30);
    }
    assert!(gauge.max.load(Ordering::SeqCst) <= 6);
    assert!(gauge.max.load(Ordering::SeqCst) >= 4);
    for (_, max) in gauge.per_hash.lock().unwrap().values() {
        assert!(*max <= 4, "{max} probes at once for one stream");
    }
}

#[tokio::test]
async fn concurrent_streams_share_each_probe() {
    let (tracker, _) =
        udp_tracker(vec!["8.8.8.8:6881", "9.9.9.9:6881"], Some(Duration::ZERO)).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let slow: Prober = {
        Arc::new(move |a, ih, t| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                alive_prober()(a, ih, t).await
            })
        })
    };
    let server = serve_with(vec![tracker], |_| {}, counted(slow, calls.clone())).await;
    let url = format!("{}/peers?ih={IH}", server.base);
    let (a, b) = tokio::join!(lines(&url), lines(&url));
    assert_eq!(a.1.last().unwrap().1["alive"], 2);
    assert_eq!(b.1.last().unwrap().1["alive"], 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn over_its_probe_budget_an_info_hash_gets_unknown_addresses() {
    let (tracker, _) = udp_tracker(
        vec!["8.8.8.8:6881", "8.8.4.4:6881", "9.9.9.9:6881"],
        Some(Duration::ZERO),
    )
    .await;
    let server = serve(vec![tracker], |c| c.max_probe_peers = 1).await;
    let (_, got, _) = lines(&format!("{}/peers?ih={IH}", server.base)).await;
    let end = &got.last().unwrap().1;
    assert_eq!(end["alive"], 1);
    assert_eq!(end["unknown"], 2);
    let unknown: Vec<_> = got
        .iter()
        .filter(|(_, v)| v["peer"].is_string() && v.get("alive").is_none())
        .collect();
    assert_eq!(unknown.len(), 2);
    assert!(unknown.iter().all(|(_, v)| v["score"] == 0.2));
}

#[tokio::test]
async fn addresses_still_unprobed_near_the_deadline_go_out_as_unknown() {
    let (tracker, _) = udp_tracker(vec!["8.8.8.8:6881"], Some(Duration::ZERO)).await;
    let hung: Prober = Arc::new(|_, _, _| Box::pin(std::future::pending()));
    let server = serve_with(
        vec![tracker],
        |c| {
            c.peers_stream_deadline = Duration::from_secs(2);
            c.probe_timeout = Duration::from_secs(10);
        },
        hung,
    )
    .await;
    let (_, got, total) = lines(&format!("{}/peers?ih={IH}", server.base)).await;
    assert_eq!(peers_of(&got), ["8.8.8.8:6881"]);
    assert_eq!(got[0].1["score"], 0.2);
    // Written at the flush (deadline - 1 s), and the stream ends right after.
    assert!(got[0].0 >= Duration::from_millis(900), "{:?}", got[0].0);
    assert!(total < Duration::from_millis(1500), "{total:?}");
    assert_eq!(got.last().unwrap().1["unknown"], 1);
}

// --- the info dict in the stream and `GET /metadata` (M3-09) -----------------

fn v1_torrent() -> (Vec<u8>, [u8; 20]) {
    use sha1::Digest;
    let info = testpeer::fake_info_dict(64);
    let ih: [u8; 20] = sha1::Sha1::digest(&info).into();
    (info, ih)
}

fn info_lines(got: &[(Duration, serde_json::Value)]) -> Vec<Vec<u8>> {
    use base64::Engine;
    got.iter()
        .filter_map(|(_, v)| v.get("info").and_then(|i| i.as_str()))
        .map(|b| base64::engine::general_purpose::STANDARD.decode(b).unwrap())
        .collect()
}

#[tokio::test]
async fn the_stream_carries_one_verified_info_line() {
    use sha1::Digest;
    let (info, ih) = v1_torrent();
    let seed = FakePeer::seed_with_metadata(ih, info.clone());
    let fetches = seed.metadata_fetches.clone();
    let seed = testpeer::spawn(seed).await;
    let tracker = tracker_for(&[seed]).await;
    let server = serve_with(vec![tracker], local(|_| {}), scout::real_prober()).await;

    let (_, got, total) = lines(&format!("{}/peers?ih={}", server.base, hex::encode(ih))).await;
    let infos = info_lines(&got);
    assert_eq!(infos.len(), 1);
    assert_eq!(sha1::Sha1::digest(&infos[0]).as_slice(), ih);
    let line = got.iter().find(|(_, v)| v.get("info").is_some()).unwrap();
    assert_eq!(line.1["size"], info.len());
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert!(total < Duration::from_secs(3), "{total:?}");

    // Cached now: a second stream has it first of all, with no new fetch.
    let (_, again, _) = lines(&format!("{}/peers?ih={}", server.base, hex::encode(ih))).await;
    assert!(again[0].1.get("info").is_some(), "{:?}", again[0].1);
    assert_eq!(info_lines(&again).len(), 1);
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_v2_only_info_dict_matches_its_truncated_sha256() {
    use sha2::Digest;
    let info = testpeer::fake_v2_info_dict();
    let full = sha2::Sha256::digest(&info);
    let ih: [u8; 20] = full[..20].try_into().unwrap();
    let seed = testpeer::spawn(FakePeer::seed_with_metadata(ih, info.clone())).await;
    let tracker = tracker_for(&[seed]).await;
    let server = serve_with(vec![tracker], local(|_| {}), scout::real_prober()).await;
    // Asked by its full v2 hash (64 hex), as a btmh magnet would.
    let (_, got, _) = lines(&format!("{}/peers?ih={}", server.base, hex::encode(full))).await;
    let infos = info_lines(&got);
    assert_eq!(infos, vec![info]);
}

#[tokio::test]
async fn concurrent_streams_share_one_fetch() {
    let (info, ih) = v1_torrent();
    let seed = FakePeer::seed_with_metadata(ih, info.clone());
    let fetches = seed.metadata_fetches.clone();
    let seed = testpeer::spawn(seed).await;
    let tracker = tracker_for(&[seed]).await;
    let server = serve_with(vec![tracker], local(|_| {}), scout::real_prober()).await;
    let url = format!("{}/peers?ih={}", server.base, hex::encode(ih));
    let (a, b) = tokio::join!(lines(&url), lines(&url));
    assert_eq!(info_lines(&a.1), vec![info.clone()]);
    assert_eq!(info_lines(&b.1), vec![info]);
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert_eq!(server.info.flights_started(), 1);
}

#[tokio::test]
async fn a_lying_peer_is_skipped_and_never_served_or_cached() {
    let (info, ih) = v1_torrent();
    let liar = FakePeer {
        corrupt_metadata: true,
        ..FakePeer::seed_with_metadata(ih, info.clone())
    };
    let liar_fetches = liar.metadata_fetches.clone();
    let liar = testpeer::spawn(liar).await;
    // Only a liar: no info line, nothing cached, 404 from /metadata.
    let tracker = tracker_for(&[liar]).await;
    let server = serve_with(vec![tracker], local(|_| {}), scout::real_prober()).await;
    let (_, got, total) = lines(&format!("{}/peers?ih={}", server.base, hex::encode(ih))).await;
    assert!(info_lines(&got).is_empty());
    assert!(liar_fetches.load(Ordering::SeqCst) >= 1);
    assert!(server.info.cached(&ih).await.is_none());
    assert!(
        total < Duration::from_secs(3),
        "the failed fetch must not hold the stream: {total:?}"
    );
    let resp = reqwest::get(format!("{}/metadata?ih={}", server.base, hex::encode(ih)))
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    // A liar and an honest seed: the honest answer is the one served.
    let honest = testpeer::spawn(FakePeer::seed_with_metadata(ih, info.clone())).await;
    let tracker = tracker_for(&[liar, honest]).await;
    let server = serve_with(vec![tracker], local(|_| {}), scout::real_prober()).await;
    let (_, got, _) = lines(&format!("{}/peers?ih={}", server.base, hex::encode(ih))).await;
    assert_eq!(info_lines(&got), vec![info]);
}

#[tokio::test]
async fn metadata_comes_from_the_cache_without_peers_with_immutable_headers() {
    let (info, ih) = v1_torrent();
    let seed = FakePeer::seed_with_metadata(ih, info.clone());
    let fetches = seed.metadata_fetches.clone();
    let seed = testpeer::spawn(seed).await;
    let (tracker, announces) = udp_tracker(
        vec![Box::leak(seed.to_string().into_boxed_str())],
        Some(Duration::ZERO),
    )
    .await;
    let server = serve_with(vec![tracker], local(|_| {}), scout::real_prober()).await;
    let url = format!("{}/metadata?ih={}", server.base, hex::encode(ih));

    // First: a discovery runs and the seed is asked.
    let first = reqwest::get(&url).await.unwrap();
    assert_eq!(first.status().as_u16(), 200);
    assert_eq!(
        first.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    assert_eq!(first.bytes().await.unwrap().as_ref(), info.as_slice());
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    let announced = announces.load(Ordering::SeqCst);

    // Then from the cache: no announce, no peer contacted.
    let started = Instant::now();
    let second = reqwest::get(&url).await.unwrap();
    assert_eq!(second.status().as_u16(), 200);
    assert_eq!(
        second.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    assert_eq!(second.bytes().await.unwrap().as_ref(), info.as_slice());
    assert!(started.elapsed() < Duration::from_millis(300));
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert_eq!(announces.load(Ordering::SeqCst), announced);
}

#[tokio::test]
async fn metadata_without_any_peer_offering_it_is_404_and_respects_the_limits() {
    let (_, ih) = v1_torrent();
    let (tracker, _) = udp_tracker(vec![], Some(Duration::ZERO)).await;
    let server = serve(vec![tracker], |c| c.peers_per_ip_per_minute = 2).await;
    let url = format!("{}/metadata?ih={}", server.base, hex::encode(ih));
    let first = reqwest::get(&url).await.unwrap();
    assert_eq!(first.status().as_u16(), 404);
    assert_eq!(first.headers()["cache-control"], "no-store");
    assert_eq!(reqwest::get(&url).await.unwrap().status().as_u16(), 404);
    // Same per-IP budget as /peers.
    assert_eq!(reqwest::get(&url).await.unwrap().status().as_u16(), 429);
    assert_eq!(
        reqwest::get(format!("{}/metadata?ih=zz", server.base))
            .await
            .unwrap()
            .status()
            .as_u16(),
        400
    );
}
