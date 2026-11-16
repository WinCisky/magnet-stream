//! Orchestrates a single peer lookup: every tracker (from the magnet plus
//! the configured defaults) is announced to in parallel with a DHT
//! `get_peers` query, all under one deadline. Peers are deduped across every
//! source; a source that errors or times out just contributes nothing,
//! rather than failing the whole request.

use crate::config::Config;
use crate::dht::DhtClient;
use crate::tracker::{self, AnnounceParams};
use serde::Serialize;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinSet;

#[derive(Debug, Clone, Serialize)]
pub struct TrackerOutcome {
    pub url: String,
    pub ok: bool,
    pub peers: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sources {
    pub dht: usize,
    pub trackers: Vec<TrackerOutcome>,
}

/// The result of one lookup, exactly as it sits in the cache. `ttl` is
/// decided here (shorter for an empty result) so the cache layer doesn't
/// need to know anything about what makes a result "good".
#[derive(Debug, Clone)]
pub struct LookupResult {
    pub info_hash: [u8; 20],
    pub peers: Vec<SocketAddr>,
    pub sources: Sources,
    pub fetched_at: u64,
    pub ttl: Duration,
}

pub struct Lookup {
    http_client: reqwest::Client,
    dht: Arc<DhtClient>,
}

impl Lookup {
    pub fn new(http_client: reqwest::Client, dht: Arc<DhtClient>) -> Self {
        Self { http_client, dht }
    }

    pub async fn run(
        &self,
        info_hash: [u8; 20],
        magnet_trackers: Vec<String>,
        cfg: &Config,
    ) -> LookupResult {
        self.run_streaming(info_hash, magnet_trackers, cfg, None)
            .await
    }

    /// Like `run`, but also sends each source's peers to `found_tx` as soon
    /// as it answers (possibly repeating peers another source sent), so
    /// probing can start long before the slowest tracker gives up.
    pub async fn run_streaming(
        &self,
        info_hash: [u8; 20],
        magnet_trackers: Vec<String>,
        cfg: &Config,
        found_tx: Option<UnboundedSender<Vec<SocketAddr>>>,
    ) -> LookupResult {
        let trackers = merge_trackers(magnet_trackers, &cfg.default_trackers, cfg.max_trackers);
        let params = AnnounceParams::new(info_hash);

        let mut join_set = JoinSet::new();
        for url in trackers {
            let http_client = self.http_client.clone();
            let params = params.clone();
            let deadline = cfg.deadline;
            join_set.spawn(async move {
                match tokio::time::timeout(
                    deadline,
                    tracker::announce(&http_client, &url, &params, deadline),
                )
                .await
                {
                    Ok(Ok(peers)) => TrackerRun::success(url, peers),
                    Ok(Err(e)) => TrackerRun::failed(url, e.to_string()),
                    Err(_) => TrackerRun::failed(url, "timed out".to_string()),
                }
            });
        }

        let dht = self.dht.clone();
        let dht_deadline = cfg.deadline;
        let dht_tx = found_tx.clone();
        let dht_handle =
            tokio::spawn(async move { dht.get_peers(info_hash, dht_deadline, dht_tx).await });

        let mut all_peers: HashSet<SocketAddr> = HashSet::new();
        let mut tracker_outcomes = Vec::new();

        while let Some(joined) = join_set.join_next().await {
            if let Ok(run) = joined {
                if let Some(tx) = &found_tx {
                    let usable: Vec<SocketAddr> =
                        run.peers.iter().copied().filter(is_usable_peer).collect();
                    if !usable.is_empty() {
                        let _ = tx.send(usable);
                    }
                }
                all_peers.extend(run.peers.into_iter().filter(is_usable_peer));
                tracker_outcomes.push(run.outcome);
            }
            // A `JoinError` here means the tracker task panicked, which
            // shouldn't happen (no unwraps on untrusted data) — if it ever
            // does, we simply don't count that tracker rather than failing
            // the whole request.
        }

        let dht_peers = dht_handle.await.unwrap_or_default();
        let dht_count = dht_peers.len();
        all_peers.extend(dht_peers.into_iter().filter(is_usable_peer));

        let mut peers: Vec<SocketAddr> = all_peers.into_iter().collect();
        peers.sort();

        let ttl = if peers.is_empty() {
            cfg.empty_cache_ttl
        } else {
            cfg.cache_ttl
        };

        LookupResult {
            info_hash,
            peers,
            sources: Sources {
                dht: dht_count,
                trackers: tracker_outcomes,
            },
            fetched_at: unix_now(),
            ttl,
        }
    }
}

struct TrackerRun {
    outcome: TrackerOutcome,
    peers: Vec<SocketAddr>,
}

impl TrackerRun {
    fn success(url: String, peers: Vec<SocketAddr>) -> Self {
        let outcome = TrackerOutcome {
            url,
            ok: true,
            peers: peers.len(),
            error: None,
        };
        Self { outcome, peers }
    }

    fn failed(url: String, error: String) -> Self {
        Self {
            outcome: TrackerOutcome {
                url,
                ok: false,
                peers: 0,
                error: Some(error),
            },
            peers: Vec::new(),
        }
    }
}

fn is_usable_peer(addr: &SocketAddr) -> bool {
    addr.port() != 0
}

/// Combine the magnet's own trackers with the configured defaults, in that
/// order, deduplicated and capped.
fn merge_trackers(magnet_trackers: Vec<String>, defaults: &[String], max: usize) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for t in magnet_trackers.into_iter().chain(defaults.iter().cloned()) {
        if result.len() >= max {
            break;
        }
        if seen.insert(t.clone()) {
            result.push(t);
        }
    }
    result
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_trackers_dedupes_and_prefers_magnet_order() {
        let magnet = vec!["udp://a".to_string(), "udp://b".to_string()];
        let defaults = vec!["udp://b".to_string(), "udp://c".to_string()];
        let merged = merge_trackers(magnet, &defaults, 10);
        assert_eq!(merged, vec!["udp://a", "udp://b", "udp://c"]);
    }

    #[test]
    fn merge_trackers_respects_cap() {
        let magnet = vec!["udp://a".to_string(), "udp://b".to_string()];
        let defaults = vec!["udp://c".to_string()];
        let merged = merge_trackers(magnet, &defaults, 2);
        assert_eq!(merged, vec!["udp://a", "udp://b"]);
    }

    #[test]
    fn zero_port_peers_are_filtered() {
        let ok: SocketAddr = "1.2.3.4:6881".parse().unwrap();
        let bad: SocketAddr = "1.2.3.4:0".parse().unwrap();
        assert!(is_usable_peer(&ok));
        assert!(!is_usable_peer(&bad));
    }
}
