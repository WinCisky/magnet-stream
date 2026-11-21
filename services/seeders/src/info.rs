//! Info dicts for the `/peers` stream and `GET /metadata?ih=`
//! (docs/architecture.md §Chunk recovery › Scouting on the VPS, decisions
//! OQ-21, OQ-22).
//!
//! As soon as the scout finds live peers offering `ut_metadata`, they are
//! offered here. The first offer for an info-hash starts one fetch (a
//! "flight"): up to `parallel` (3) peers at a time, the first answer that
//! hashes to the info-hash wins, and the flight gives up at its deadline
//! (15 s) or once every peer it was offered has failed. Later offers for the
//! same info-hash join the running flight (single-flight); no fetch is
//! queued: with `fetches` (4) flights in the air, an offer is dropped and a
//! later one tries again. A flight runs on its own task, so one that ends
//! after the stream that started it closed still fills the cache.
//!
//! The cache is weighed in bytes (`cache_bytes`, 32 MiB); an info dict over
//! `max_entry_bytes` (4 MiB) is served to whoever waited for it but not
//! kept. Peers' buffers come out of a budget of `fetches` × 8 MiB, so
//! transient memory is bounded whatever peers announce. Only verified info
//! dicts are ever cached or served. Logs carry no info-hash or address.

use crate::metadata::{self, InfoDict, MetadataError, MAX_METADATA_SIZE, MEMORY_UNIT};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct InfoSettings {
    /// A flight gives up this long after it started (`METADATA_DEADLINE_MS`).
    pub deadline: Duration,
    /// Byte budget of the cache (`METADATA_CACHE_MB`).
    pub cache_bytes: u64,
    /// Larger info dicts are served but not cached (`METADATA_CACHE_MAX_ENTRY_MB`).
    pub max_entry_bytes: usize,
    /// Flights in the air at once, in total (`METADATA_FETCHES`).
    pub fetches: usize,
    /// Peers asked at once by one flight.
    pub parallel: usize,
    pub connect_timeout: Duration,
}

pub type Outcome = Result<Arc<InfoDict>, MetadataError>;

struct Flight {
    id: u64,
    peers: UnboundedSender<SocketAddr>,
    outcome: watch::Receiver<Option<Outcome>>,
}

pub struct InfoService {
    settings: InfoSettings,
    cache: moka::future::Cache<[u8; 20], Arc<InfoDict>>,
    flights: Mutex<HashMap<[u8; 20], Flight>>,
    slots: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    next_id: AtomicUsize,
    started: AtomicUsize,
    peak: AtomicUsize,
}

impl InfoService {
    pub fn new(settings: InfoSettings) -> Arc<Self> {
        let fetches = settings.fetches.max(1);
        let cache = moka::future::Cache::builder()
            .weigher(|_: &[u8; 20], info: &Arc<InfoDict>| {
                u32::try_from(info.bytes.len()).unwrap_or(u32::MAX)
            })
            .max_capacity(settings.cache_bytes)
            .build();
        let memory_units = fetches * MAX_METADATA_SIZE / MEMORY_UNIT;
        Arc::new(Self {
            cache,
            flights: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(fetches)),
            memory: Arc::new(Semaphore::new(memory_units)),
            next_id: AtomicUsize::new(1),
            started: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            settings,
        })
    }

    pub async fn cached(&self, info_hash: &[u8; 20]) -> Option<Arc<InfoDict>> {
        self.cache.get(info_hash).await
    }

    /// The outcome of the flight for `info_hash`, if one is in the air.
    pub fn watch(&self, info_hash: &[u8; 20]) -> Option<watch::Receiver<Option<Outcome>>> {
        lock(&self.flights)
            .get(info_hash)
            .map(|f| f.outcome.clone())
    }

    /// Peers that offer the info dict. They join the flight in the air, or
    /// start one if a fetch slot is free (else they are dropped). Returns
    /// whether a flight now has them.
    pub fn offer(self: &Arc<Self>, info_hash: [u8; 20], peers: &[SocketAddr]) -> bool {
        if peers.is_empty() || self.cache.contains_key(&info_hash) {
            return false;
        }
        let mut flights = lock(&self.flights);
        if let Some(flight) = flights.get(&info_hash) {
            for &peer in peers {
                let _ = flight.peers.send(peer);
            }
            return true;
        }
        let Ok(slot) = self.slots.clone().try_acquire_owned() else {
            tracing::debug!("metadata fetch slots full, offer dropped");
            return false;
        };
        let (peers_tx, peers_rx) = unbounded_channel();
        for &peer in peers {
            let _ = peers_tx.send(peer);
        }
        let (outcome_tx, outcome_rx) = watch::channel(None);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) as u64;
        flights.insert(
            info_hash,
            Flight {
                id,
                peers: peers_tx,
                outcome: outcome_rx,
            },
        );
        drop(flights);
        self.started.fetch_add(1, Ordering::Relaxed);
        let in_air = self.settings.fetches.max(1) - self.slots.available_permits();
        self.peak.fetch_max(in_air, Ordering::Relaxed);
        let this = self.clone();
        tokio::spawn(async move {
            let outcome = this.fly(info_hash, id, peers_rx, slot).await;
            outcome_tx.send_replace(Some(outcome));
        });
        true
    }

    /// The info dict: from the cache, or from a flight over `peers`.
    pub async fn fetch(self: &Arc<Self>, info_hash: [u8; 20], peers: &[SocketAddr]) -> Outcome {
        if let Some(info) = self.cached(&info_hash).await {
            return Ok(info);
        }
        self.offer(info_hash, peers);
        let Some(mut outcome) = self.watch(&info_hash) else {
            // No flight: none could start, or one just finished.
            if let Some(info) = self.cached(&info_hash).await {
                return Ok(info);
            }
            return Err(if peers.is_empty() {
                MetadataError::NoPeers
            } else {
                MetadataError::Busy
            });
        };
        let result = match outcome.wait_for(Option::is_some).await {
            Ok(value) => value.clone().unwrap_or(Err(MetadataError::Timeout)),
            Err(_) => Err(MetadataError::Timeout),
        };
        result
    }

    async fn fly(
        self: &Arc<Self>,
        info_hash: [u8; 20],
        id: u64,
        mut peers: UnboundedReceiver<SocketAddr>,
        _slot: OwnedSemaphorePermit,
    ) -> Outcome {
        let deadline = Instant::now() + self.settings.deadline;
        let parallel = self.settings.parallel.max(1);
        let mut tried = HashSet::new();
        let mut queue = VecDeque::new();
        let mut attempts = JoinSet::new();
        let mut failed = 0usize;
        let outcome = loop {
            while let Ok(peer) = peers.try_recv() {
                if tried.insert(peer) {
                    queue.push_back(peer);
                }
            }
            while attempts.len() < parallel {
                let Some(peer) = queue.pop_front() else { break };
                attempts.spawn(metadata::fetch_from_peer(
                    peer,
                    info_hash,
                    self.settings.connect_timeout,
                    deadline,
                    Some(self.memory.clone()),
                ));
            }
            if attempts.is_empty() {
                // Every peer offered so far failed. Close the flight under
                // the lock, unless an offer slipped in meanwhile.
                let mut flights = lock(&self.flights);
                if let Ok(peer) = peers.try_recv() {
                    if tried.insert(peer) {
                        queue.push_back(peer);
                    }
                    continue;
                }
                remove_flight(&mut flights, &info_hash, id);
                break Err(if failed == 0 {
                    MetadataError::NoPeers
                } else {
                    MetadataError::AllFailed(failed)
                });
            }
            tokio::select! {
                Some(peer) = peers.recv() => {
                    if tried.insert(peer) {
                        queue.push_back(peer);
                    }
                }
                Some(joined) = attempts.join_next() => {
                    match joined {
                        Ok(Ok(bytes)) => match InfoDict::parse(bytes) {
                            Ok(info) => break Ok(Arc::new(info)),
                            Err(_) => failed += 1,
                        },
                        _ => failed += 1,
                    }
                }
                _ = tokio::time::sleep_until(deadline.into()) => {
                    break Err(MetadataError::Timeout);
                }
            }
        };
        attempts.abort_all();
        if let Ok(info) = &outcome {
            if info.bytes.len() <= self.settings.max_entry_bytes {
                self.cache.insert(info_hash, info.clone()).await;
            }
        }
        remove_flight(&mut lock(&self.flights), &info_hash, id);
        tracing::debug!(
            ok = outcome.is_ok(),
            peers = tried.len(),
            failed,
            "metadata flight done"
        );
        outcome
    }

    /// Flights started so far (tests, diagnostics).
    #[cfg(test)]
    pub fn flights_started(&self) -> usize {
        self.started.load(Ordering::Relaxed)
    }

    /// Most flights ever in the air at once.
    #[cfg(test)]
    pub fn peak_flights(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub async fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.weighted_size()
    }

    #[cfg(test)]
    pub async fn insert(&self, info_hash: [u8; 20], info: Arc<InfoDict>) {
        if info.bytes.len() <= self.settings.max_entry_bytes {
            self.cache.insert(info_hash, info).await;
        }
    }
}

fn remove_flight(flights: &mut HashMap<[u8; 20], Flight>, info_hash: &[u8; 20], id: u64) {
    if flights.get(info_hash).is_some_and(|f| f.id == id) {
        flights.remove(info_hash);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The stream's line carrying the info dict: `{"info":"<base64>","size":n}`.
pub fn info_line(info: &InfoDict) -> String {
    use base64::Engine;
    format!(
        "{{\"info\":\"{}\",\"size\":{}}}\n",
        base64::engine::general_purpose::STANDARD.encode(&info.bytes),
        info.bytes.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testpeer::{self, FakePeer};
    use sha1::{Digest, Sha1};

    fn settings() -> InfoSettings {
        InfoSettings {
            deadline: Duration::from_secs(3),
            cache_bytes: 1 << 20,
            max_entry_bytes: 1 << 20,
            fetches: 4,
            parallel: 3,
            connect_timeout: Duration::from_millis(500),
        }
    }

    fn hash(info: &[u8]) -> [u8; 20] {
        Sha1::digest(info).into()
    }

    #[tokio::test]
    async fn concurrent_requests_share_one_flight_and_fill_the_cache() {
        let info = testpeer::fake_info_dict(40);
        let ih = hash(&info);
        let peer = testpeer::spawn(FakePeer::seed_with_metadata(ih, info.clone())).await;
        let service = InfoService::new(settings());
        let a = {
            let s = service.clone();
            tokio::spawn(async move { s.fetch(ih, &[peer]).await })
        };
        let b = {
            let s = service.clone();
            tokio::spawn(async move { s.fetch(ih, &[peer]).await })
        };
        assert_eq!(a.await.unwrap().unwrap().bytes, info);
        assert_eq!(b.await.unwrap().unwrap().bytes, info);
        assert_eq!(service.flights_started(), 1);
        // Now from the cache: no flight, no peer needed.
        assert_eq!(service.fetch(ih, &[]).await.unwrap().bytes, info);
        assert_eq!(service.flights_started(), 1);
    }

    #[tokio::test]
    async fn a_lying_peer_is_skipped_and_never_cached() {
        let info = testpeer::fake_info_dict(10);
        let ih = hash(&info);
        let liar = testpeer::spawn(FakePeer {
            corrupt_metadata: true,
            ..FakePeer::seed_with_metadata(ih, info.clone())
        })
        .await;
        let service = InfoService::new(InfoSettings {
            parallel: 1,
            ..settings()
        });
        let err = service.fetch(ih, &[liar]).await.unwrap_err();
        assert!(matches!(err, MetadataError::AllFailed(1)), "{err:?}");
        assert!(service.cached(&ih).await.is_none());
        // With an honest peer after the liar, the honest answer wins.
        let honest = testpeer::spawn(FakePeer::seed_with_metadata(ih, info.clone())).await;
        let got = service.fetch(ih, &[liar, honest]).await.unwrap();
        assert_eq!(got.bytes, info);
    }

    #[tokio::test]
    async fn no_more_than_the_configured_flights_and_no_queue() {
        // Peers that accept and never answer: flights stay in the air.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        let service = InfoService::new(InfoSettings {
            fetches: 2,
            deadline: Duration::from_millis(400),
            ..settings()
        });
        let started: Vec<bool> = (0..4u8)
            .map(|i| service.offer([i; 20], &[silent]))
            .collect();
        assert_eq!(started, [true, true, false, false]);
        assert_eq!(service.peak_flights(), 2);
        // The dropped ones were not queued.
        assert!(service.watch(&[3; 20]).is_none());
        let mut w = service.watch(&[0; 20]).unwrap();
        let outcome = w.wait_for(Option::is_some).await.unwrap().clone().unwrap();
        assert!(matches!(outcome, Err(MetadataError::Timeout)));
        // A slot is free again.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(service.offer([3; 20], &[silent]));
    }

    #[tokio::test]
    async fn the_cache_keeps_to_its_byte_budget_and_skips_large_entries() {
        let service = InfoService::new(InfoSettings {
            cache_bytes: 64 * 1024,
            max_entry_bytes: 8 * 1024,
            ..settings()
        });
        let big = Arc::new(InfoDict::parse(testpeer::fake_info_dict(1000)).unwrap());
        assert!(big.bytes.len() > 8 * 1024);
        service.insert([1; 20], big).await;
        assert!(service.cached(&[1; 20]).await.is_none());
        for i in 0..100u8 {
            let small = Arc::new(InfoDict::parse(testpeer::fake_info_dict(100)).unwrap());
            service.insert([i; 20], small).await;
        }
        let size = service.weighted_size().await;
        assert!(size <= 64 * 1024 && size > 0, "{size}");
    }

    #[test]
    fn the_info_line_is_one_json_object() {
        let info = InfoDict::parse(testpeer::fake_info_dict(2)).unwrap();
        let line = info_line(&info);
        assert!(line.ends_with('\n'));
        let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(v["size"], info.bytes.len());
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(v["info"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, info.bytes);
    }
}
