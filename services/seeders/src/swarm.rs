//! Swarm snapshots: the announced peers of an info-hash, each probed over TCP
//! (see `probe`), plus the torrent's verified info dict (see `metadata`).
//!
//! A swarm becomes *active* when a client asks for it. Active swarms are
//! re-announced and re-probed in the background every
//! `REFRESH_INTERVAL_SECS`, so a client polling `/swarm` never waits for a
//! cold lookup; swarms nobody asked about for `ACTIVE_TTL_SECS` are dropped.
//!
//! A cold build probes peers as trackers and the DHT hand them over, and
//! publishes partial snapshots (`complete: false`) as soon as a few peers
//! answer, then every `PUBLISH_EVERY` until every peer is probed. The first
//! client doesn't wait for the slowest tracker or probe, and polls again for
//! the rest.

use crate::cache::PeerCache;
use crate::config::{Config, Secret};
use crate::lookup::Lookup;
use crate::metadata::{self, InfoDict, MetadataError};
use crate::netguard;
use crate::peerwire::WireError;
use crate::probe::{self, Availability, PeerProbe, ProbeTimeouts};
use crate::token;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

const REFRESH_TICK: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_REFRESHES: usize = 4;
const MAX_SWARMS: u64 = 1000;
const METADATA_TTI: Duration = Duration::from_secs(24 * 3600);
/// A cold build answers as soon as this many peers unchoked a probe...
const FIRST_PUBLISH_UNCHOKED: usize = 8;
/// ...or this long after it started, if anyone is reachable by then.
const FIRST_PUBLISH_AFTER: Duration = Duration::from_millis(3000);
/// Later partial snapshots, while probes are still coming in.
const PUBLISH_EVERY: Duration = Duration::from_millis(1500);
const PUBLISH_CHECK: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct ProbedPeer {
    pub addr: SocketAddr,
    pub probe: PeerProbe,
}

#[derive(Debug)]
pub struct SwarmSnapshot {
    pub info_hash: [u8; 20],
    pub announced_at: u64,
    pub probed_at: u64,
    /// Peers announced by trackers + DHT, before filtering and capping.
    pub known: usize,
    pub peers: Vec<ProbedPeer>,
    /// Peers whose probe hasn't finished yet (partial snapshots only).
    pub pending: Vec<SocketAddr>,
    /// False while a build is still probing: poll again for more peers.
    pub complete: bool,
}

type Published = watch::Receiver<Option<Arc<SwarmSnapshot>>>;

struct ActiveSwarm {
    last_request: Instant,
    trackers: Vec<String>,
    refreshing: bool,
}

pub struct SwarmService {
    cfg: Arc<Config>,
    lookup: Arc<Lookup>,
    peer_cache: Arc<PeerCache>,
    snapshots: moka::future::Cache<[u8; 20], Arc<SwarmSnapshot>>,
    metadata: moka::future::Cache<[u8; 20], Arc<InfoDict>>,
    active: Mutex<HashMap<[u8; 20], ActiveSwarm>>,
    /// Cold builds in progress; requests wait for their first snapshot.
    building: Mutex<HashMap<[u8; 20], Published>>,
    probe_permits: Arc<Semaphore>,
}

impl SwarmService {
    pub fn new(cfg: Arc<Config>, lookup: Arc<Lookup>, peer_cache: Arc<PeerCache>) -> Arc<Self> {
        let metadata = moka::future::Cache::builder()
            .weigher(|_: &[u8; 20], info: &Arc<InfoDict>| {
                u32::try_from(info.bytes.len()).unwrap_or(u32::MAX)
            })
            .max_capacity(cfg.metadata_cache_bytes)
            .time_to_idle(METADATA_TTI)
            .build();
        Arc::new(Self {
            probe_permits: Arc::new(Semaphore::new(cfg.probe_concurrency)),
            snapshots: moka::future::Cache::builder()
                .max_capacity(MAX_SWARMS)
                .build(),
            metadata,
            active: Mutex::new(HashMap::new()),
            building: Mutex::new(HashMap::new()),
            cfg,
            lookup,
            peer_cache,
        })
    }

    /// The latest snapshot. The first request starts a build (concurrent
    /// first requests share it) and gets its first partial snapshot; the
    /// refresher keeps it fresh after that.
    pub async fn snapshot(
        self: &Arc<Self>,
        info_hash: [u8; 20],
        trackers: Vec<String>,
    ) -> Arc<SwarmSnapshot> {
        self.mark_requested(info_hash, &trackers);
        if let Some(snapshot) = self.snapshots.get(&info_hash).await {
            return snapshot;
        }
        let mut published = self.start_build(info_hash, trackers);
        let first = published
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|s| s.clone());
        match first {
            Some(snapshot) => snapshot,
            // The build died without publishing (it panicked): report an
            // empty swarm now, and the next request starts over.
            None => Arc::new(SwarmSnapshot {
                info_hash,
                announced_at: unix_now(),
                probed_at: unix_now(),
                known: 0,
                peers: Vec::new(),
                pending: Vec::new(),
                complete: true,
            }),
        }
    }

    fn start_build(self: &Arc<Self>, info_hash: [u8; 20], trackers: Vec<String>) -> Published {
        let mut building = self.building.lock().unwrap();
        if let Some(published) = building.get(&info_hash) {
            return published.clone();
        }
        let (tx, rx) = watch::channel(None);
        building.insert(info_hash, rx.clone());
        let this = self.clone();
        tokio::spawn(async move {
            let _done = BuildGuard {
                service: this.clone(),
                info_hash,
            };
            let snapshot = this.build(info_hash, trackers, Some(&tx)).await;
            this.snapshots.insert(info_hash, snapshot.clone()).await;
            tx.send_replace(Some(snapshot));
        });
        rx
    }

    pub async fn cached_metadata(&self, info_hash: [u8; 20]) -> Option<Arc<InfoDict>> {
        self.metadata.get(&info_hash).await
    }

    /// The verified info dict. Concurrent requests share one fetch, and a
    /// failure isn't cached, so the next request tries again.
    pub async fn metadata(
        self: &Arc<Self>,
        info_hash: [u8; 20],
        trackers: Vec<String>,
    ) -> Result<Arc<InfoDict>, Arc<MetadataError>> {
        let this = self.clone();
        self.metadata
            .try_get_with(info_hash, async move {
                let deadline = Instant::now() + this.cfg.metadata_deadline;
                // The browser asks for the swarm next anyway: probe it now, and
                // ask the first peers that answer (partial snapshots come
                // within a second or so), then the ones still being probed.
                let snapshot = this.snapshot(info_hash, trackers).await;
                let mut candidates = metadata_candidates(&snapshot);
                candidates.extend(&snapshot.pending);
                let parallel = if snapshot.complete { 3 } else { 6 };
                this.fetch_metadata(info_hash, candidates, parallel, deadline)
                    .await
            })
            .await
    }

    async fn fetch_metadata(
        &self,
        info_hash: [u8; 20],
        candidates: Vec<SocketAddr>,
        parallel: usize,
        deadline: Instant,
    ) -> Result<Arc<InfoDict>, MetadataError> {
        let bytes = metadata::fetch_from_any(
            candidates,
            info_hash,
            parallel,
            self.cfg.probe_connect_timeout,
            deadline,
        )
        .await?;
        Ok(Arc::new(InfoDict::parse(bytes)?))
    }

    /// Announce and probe every announced peer (up to `max_probe_peers`),
    /// starting each probe as soon as its peer is announced. Peers usable
    /// last time go first; peers that failed last time go last. With `live`,
    /// partial snapshots are published along the way (see module docs).
    async fn build(
        self: &Arc<Self>,
        info_hash: [u8; 20],
        trackers: Vec<String>,
        live: Option<&watch::Sender<Option<Arc<SwarmSnapshot>>>>,
    ) -> Arc<SwarmSnapshot> {
        let started = Instant::now();
        let deadline = started + self.cfg.deadline + self.cfg.probe_timeout * 2;
        let previous = self.snapshots.get(&info_hash).await;
        let timeouts = ProbeTimeouts {
            connect: self.cfg.probe_connect_timeout,
            total: self.cfg.probe_timeout,
            unchoke_wait: self.cfg.probe_unchoke_wait,
        };

        let (found_tx, mut found_rx) = mpsc::unbounded_channel::<Vec<SocketAddr>>();
        let lookup_task = {
            let this = self.clone();
            tokio::spawn(async move {
                let lookup = this.lookup.clone();
                let cfg = this.cfg.clone();
                let tx = found_tx.clone();
                // Shared with `/peers` and `/metadata`: if one of them is
                // already announcing, this waits for its result instead.
                let (result, _) = this
                    .peer_cache
                    .get_or_compute(info_hash, async move {
                        Arc::new(
                            lookup
                                .run_streaming(info_hash, trackers, &cfg, Some(tx))
                                .await,
                        )
                    })
                    .await;
                let _ = found_tx.send(result.peers.clone());
                result
            })
        };

        let mut queue = ProbeQueue::new(&self.cfg, previous.as_deref());
        let mut probes = JoinSet::new();
        for addr in queue.take_previous_ok() {
            spawn_probe(&mut probes, &self.probe_permits, addr, info_hash, timeouts);
        }
        let mut done: Vec<ProbedPeer> = Vec::new();
        let mut unchoked = 0usize;
        let mut reachable = 0usize;
        let mut announcing = true;
        let mut published_at: Option<Instant> = None;
        let mut published_count = 0usize;
        let mut metadata_warmed = false;
        let mut check = tokio::time::interval(PUBLISH_CHECK);
        check.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                batch = found_rx.recv(), if announcing => match batch {
                    Some(batch) => {
                        for addr in queue.announced(batch) {
                            spawn_probe(&mut probes, &self.probe_permits, addr, info_hash, timeouts);
                        }
                    }
                    None => {
                        announcing = false;
                        for addr in queue.take_deferred() {
                            spawn_probe(&mut probes, &self.probe_permits, addr, info_hash, timeouts);
                        }
                    }
                },
                joined = probes.join_next(), if !probes.is_empty() => {
                    if let Some(Ok(peer)) = joined {
                        queue.finished(peer.addr);
                        if peer.probe.ok() {
                            reachable += 1;
                            if peer.probe.unchoke_ms.is_some() {
                                unchoked += 1;
                            }
                        }
                        done.push(peer);
                    }
                }
                _ = check.tick(), if live.is_some() => {}
                _ = tokio::time::sleep_until(deadline.into()) => break,
            }
            if !announcing && probes.is_empty() {
                break;
            }
            let Some(live) = live else { continue };
            let due = match published_at {
                None => {
                    unchoked >= FIRST_PUBLISH_UNCHOKED
                        || (reachable > 0 && started.elapsed() >= FIRST_PUBLISH_AFTER)
                }
                Some(at) => at.elapsed() >= PUBLISH_EVERY && done.len() > published_count,
            };
            if !due {
                continue;
            }
            let partial = Arc::new(SwarmSnapshot {
                info_hash,
                announced_at: unix_now(),
                probed_at: unix_now(),
                known: queue.known(),
                peers: done.clone(),
                pending: queue.pending(),
                complete: false,
            });
            if !metadata_warmed {
                metadata_warmed = true;
                self.warm_metadata(&partial);
            }
            self.snapshots.insert(info_hash, partial.clone()).await;
            live.send_replace(Some(partial));
            published_at = Some(Instant::now());
            published_count = done.len();
        }
        probes.abort_all();

        // Probes cut off by the deadline count as timed out.
        let finished: HashSet<SocketAddr> = done.iter().map(|p| p.addr).collect();
        done.extend(
            queue
                .pending()
                .into_iter()
                .filter(|addr| !finished.contains(addr))
                .map(|addr| ProbedPeer {
                    addr,
                    probe: PeerProbe::failed(WireError::Timeout),
                }),
        );
        let lookup = match tokio::time::timeout_at(deadline.into(), lookup_task).await {
            Ok(Ok(lookup)) => Some(lookup),
            _ => None,
        };
        let snapshot = Arc::new(SwarmSnapshot {
            info_hash,
            announced_at: lookup.as_ref().map_or_else(unix_now, |l| l.fetched_at),
            probed_at: unix_now(),
            known: lookup
                .as_ref()
                .map_or(0, |l| l.peers.len())
                .max(queue.known()),
            peers: done,
            pending: Vec::new(),
            complete: true,
        });
        if !metadata_warmed {
            self.warm_metadata(&snapshot);
        }
        snapshot
    }

    /// Warm the metadata cache from the peers just probed, so the browser's
    /// `/metadata` call right after is usually a cache hit.
    fn warm_metadata(self: &Arc<Self>, snapshot: &SwarmSnapshot) {
        let candidates = metadata_candidates(snapshot);
        if candidates.is_empty() {
            return;
        }
        let info_hash = snapshot.info_hash;
        let this = self.clone();
        tokio::spawn(async move {
            if this.metadata.get(&info_hash).await.is_some() {
                return;
            }
            let deadline = Instant::now() + this.cfg.metadata_deadline;
            let fetch = this.fetch_metadata(info_hash, candidates, 3, deadline);
            let _ = this.metadata.try_get_with(info_hash, fetch).await;
        });
    }

    fn mark_requested(&self, info_hash: [u8; 20], trackers: &[String]) {
        let mut active = self.active.lock().unwrap();
        let swarm = active.entry(info_hash).or_insert_with(|| ActiveSwarm {
            last_request: Instant::now(),
            trackers: Vec::new(),
            refreshing: false,
        });
        swarm.last_request = Instant::now();
        if !trackers.is_empty() {
            swarm.trackers = trackers.to_vec();
        }
    }

    /// Keep active swarms fresh, and forget the ones nobody asks about.
    pub fn spawn_refresher(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_REFRESHES));
            let mut tick = tokio::time::interval(REFRESH_TICK);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                for (info_hash, trackers) in this.due_for_refresh().await {
                    let this = this.clone();
                    let permits = permits.clone();
                    tokio::spawn(async move {
                        let _permit = permits.acquire_owned().await;
                        let fresh = this.build(info_hash, trackers, None).await;
                        this.snapshots.insert(info_hash, fresh).await;
                        if let Some(swarm) = this.active.lock().unwrap().get_mut(&info_hash) {
                            swarm.refreshing = false;
                        }
                    });
                }
            }
        });
    }

    async fn due_for_refresh(&self) -> Vec<([u8; 20], Vec<String>)> {
        let now = Instant::now();
        let (expired, idle) = {
            let mut active = self.active.lock().unwrap();
            let mut expired = Vec::new();
            active.retain(|info_hash, swarm| {
                let keep = now.duration_since(swarm.last_request) <= self.cfg.active_ttl;
                if !keep {
                    expired.push(*info_hash);
                }
                keep
            });
            let idle: Vec<_> = active
                .iter()
                .filter(|(_, swarm)| !swarm.refreshing)
                .map(|(info_hash, swarm)| (*info_hash, swarm.trackers.clone()))
                .collect();
            (expired, idle)
        };
        for info_hash in expired {
            self.snapshots.invalidate(&info_hash).await;
        }

        let mut due = Vec::new();
        for (info_hash, trackers) in idle {
            // No snapshot yet means a request is building it right now (or it
            // was evicted, and the next request rebuilds it).
            if let Some(snapshot) = self.snapshots.get(&info_hash).await {
                if unix_now().saturating_sub(snapshot.probed_at)
                    >= self.cfg.refresh_interval.as_secs()
                {
                    due.push((info_hash, trackers));
                }
            }
        }
        let mut active = self.active.lock().unwrap();
        for (info_hash, _) in &due {
            if let Some(swarm) = active.get_mut(info_hash) {
                swarm.refreshing = true;
            }
        }
        due
    }
}

struct BuildGuard {
    service: Arc<SwarmService>,
    info_hash: [u8; 20],
}

impl Drop for BuildGuard {
    fn drop(&mut self) {
        self.service
            .building
            .lock()
            .unwrap()
            .remove(&self.info_hash);
    }
}

fn spawn_probe(
    probes: &mut JoinSet<ProbedPeer>,
    permits: &Arc<Semaphore>,
    addr: SocketAddr,
    info_hash: [u8; 20],
    timeouts: ProbeTimeouts,
) {
    // Permits are granted in order, so probes run in the order spawned.
    let permits = permits.clone();
    probes.spawn(async move {
        let _permit = permits.acquire_owned().await;
        ProbedPeer {
            addr,
            probe: probe::probe(addr, info_hash, timeouts).await,
        }
    });
}

/// Which announced peers to probe, and in what order.
struct ProbeQueue {
    allow_private: bool,
    cap: usize,
    /// Every usable peer announced so far.
    seen: HashSet<SocketAddr>,
    previous_ok: Vec<SocketAddr>,
    /// Failed last time: probed once announcing is over.
    previous_failed: HashSet<SocketAddr>,
    deferred: Vec<SocketAddr>,
    spawned: usize,
    /// Spawned and not finished, in spawn order.
    in_flight: Vec<SocketAddr>,
}

impl ProbeQueue {
    fn new(cfg: &Config, previous: Option<&SwarmSnapshot>) -> Self {
        let mut queue = Self {
            allow_private: cfg.allow_private_peers,
            cap: cfg.max_probe_peers,
            seen: HashSet::new(),
            previous_ok: Vec::new(),
            previous_failed: HashSet::new(),
            deferred: Vec::new(),
            spawned: 0,
            in_flight: Vec::new(),
        };
        for peer in previous.iter().flat_map(|p| p.peers.iter()) {
            if !queue.usable(&peer.addr) {
                continue;
            }
            if peer.probe.ok() {
                if queue.seen.insert(peer.addr) {
                    queue.previous_ok.push(peer.addr);
                }
            } else {
                queue.previous_failed.insert(peer.addr);
            }
        }
        queue
    }

    fn usable(&self, addr: &SocketAddr) -> bool {
        self.allow_private || netguard::is_public(addr.ip())
    }

    fn spawn(&mut self, addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
        let room = self.cap.saturating_sub(self.spawned);
        let out: Vec<SocketAddr> = addrs.into_iter().take(room).collect();
        self.spawned += out.len();
        self.in_flight.extend(&out);
        out
    }

    /// Peers usable last time: probe them straight away (they were
    /// announced minutes ago, and most still are).
    fn take_previous_ok(&mut self) -> Vec<SocketAddr> {
        let previous = std::mem::take(&mut self.previous_ok);
        self.spawn(previous)
    }

    /// New peers from a source: the ones to probe now.
    fn announced(&mut self, batch: Vec<SocketAddr>) -> Vec<SocketAddr> {
        let mut now = Vec::new();
        for addr in batch {
            if !self.usable(&addr) || !self.seen.insert(addr) {
                continue;
            }
            if self.previous_failed.contains(&addr) {
                self.deferred.push(addr);
            } else {
                now.push(addr);
            }
        }
        self.spawn(now)
    }

    /// Announcing is over: the peers that failed last time get their turn.
    fn take_deferred(&mut self) -> Vec<SocketAddr> {
        let deferred = std::mem::take(&mut self.deferred);
        self.spawn(deferred)
    }

    fn finished(&mut self, addr: SocketAddr) {
        if let Some(i) = self.in_flight.iter().position(|a| *a == addr) {
            self.in_flight.remove(i);
        }
    }

    fn pending(&self) -> Vec<SocketAddr> {
        self.in_flight.clone()
    }

    fn known(&self) -> usize {
        self.seen.len()
    }
}

/// Peers a probe saw offering ut_metadata, fastest first, then any other
/// reachable peer (the probe may have missed a late extension handshake).
fn metadata_candidates(snapshot: &SwarmSnapshot) -> Vec<SocketAddr> {
    let mut reachable: Vec<&ProbedPeer> = snapshot.peers.iter().filter(|p| p.probe.ok()).collect();
    reachable.sort_by_key(|p| {
        let ext = &p.probe.extensions;
        let offers_metadata = ext.ut_metadata.is_some() && ext.metadata_size.is_some();
        (!offers_metadata, p.probe.connect_ms.unwrap_or(u32::MAX))
    });
    reachable.into_iter().map(|p| p.addr).collect()
}

#[derive(Debug, Serialize)]
pub struct SwarmResponse {
    info_hash: String,
    announced_at: u64,
    probed_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_exp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_pieces: Option<u32>,
    /// False while probes are still running: poll again soon for more.
    complete: bool,
    counts: Counts,
    peers: Vec<PeerJson>,
}

#[derive(Debug, Serialize)]
struct Counts {
    known: usize,
    probed: usize,
    /// Still being probed (listed with `err: "probing"`).
    pending: usize,
    reachable: usize,
    seeds: usize,
    unchoked: usize,
}

#[derive(Debug, Serialize)]
struct PeerJson {
    addr: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<&'static str>,
    seed: bool,
    /// Base64 bitfield, for reachable peers that aren't seeds.
    #[serde(skip_serializing_if = "Option::is_none")]
    have: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    have_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reqq: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rtt_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unchoke_ms: Option<u32>,
    fast: bool,
    /// Token letting magnet-worker connect to this peer (see `token`).
    #[serde(skip_serializing_if = "Option::is_none")]
    t: Option<String>,
}

/// The JSON for `/swarm`. Seed flags are derived here rather than at probe
/// time because the piece count may only have become known since.
pub fn response(
    snapshot: &SwarmSnapshot,
    num_pieces: Option<u32>,
    tokens: Option<(&Secret, u64)>,
) -> SwarmResponse {
    let info_hash = hex::encode(snapshot.info_hash);
    let mut peers: Vec<PeerJson> = snapshot
        .peers
        .iter()
        .map(|peer| {
            let probe = &peer.probe;
            let addr = peer.addr.to_string();
            let seed = probe.ok()
                && match &probe.availability {
                    Availability::Everything => true,
                    Availability::Bitfield(bits) => probe::is_complete(bits, num_pieces),
                    Availability::Nothing | Availability::Unknown => false,
                };
            let bits = match &probe.availability {
                Availability::Bitfield(bits) if probe.ok() && !seed => Some(bits),
                _ => None,
            };
            PeerJson {
                t: tokens.map(|(secret, exp)| token::sign(secret, &info_hash, &addr, exp)),
                addr,
                ok: probe.ok(),
                err: probe.error.as_ref().map(WireError::code),
                seed,
                have: bits.map(|b| STANDARD.encode(b)),
                have_count: bits.map(|b| probe::count_pieces(b, num_pieces)),
                client: probe.extensions.client.clone(),
                reqq: probe.extensions.reqq,
                rtt_ms: probe.connect_ms,
                unchoke_ms: probe.unchoke_ms,
                fast: probe.fast,
            }
        })
        .collect();
    let probed = peers.len();
    peers.extend(snapshot.pending.iter().map(|addr| {
        let addr = addr.to_string();
        PeerJson {
            t: tokens.map(|(secret, exp)| token::sign(secret, &info_hash, &addr, exp)),
            addr,
            ok: false,
            err: Some("probing"),
            seed: false,
            have: None,
            have_count: None,
            client: None,
            reqq: None,
            rtt_ms: None,
            unchoke_ms: None,
            fast: false,
        }
    }));

    // Best first: reachable, then quickest to unchoke, then seeds, then
    // lowest round-trip time.
    peers.sort_by_key(|p| {
        let failed = !p.ok && p.err != Some("probing");
        (
            !p.ok,
            failed,
            p.unchoke_ms.unwrap_or(u32::MAX),
            !p.seed,
            p.rtt_ms.unwrap_or(u32::MAX),
        )
    });

    let counts = Counts {
        known: snapshot.known,
        probed,
        pending: snapshot.pending.len(),
        reachable: peers.iter().filter(|p| p.ok).count(),
        seeds: peers.iter().filter(|p| p.seed).count(),
        unchoked: peers
            .iter()
            .filter(|p| p.ok && p.unchoke_ms.is_some())
            .count(),
    };

    SwarmResponse {
        info_hash,
        announced_at: snapshot.announced_at,
        probed_at: snapshot.probed_at,
        token_exp: tokens.map(|(_, exp)| exp),
        num_pieces,
        complete: snapshot.complete,
        counts,
        peers,
    }
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
    use crate::cache::PeerCache;
    use crate::lookup::LookupResult;
    use crate::peerwire::ExtensionHandshake;
    use crate::testpeer::{self, FakeAvailability, FakePeer};

    fn probe_with(
        availability: Availability,
        unchoke_ms: Option<u32>,
        connect_ms: u32,
    ) -> PeerProbe {
        PeerProbe {
            error: None,
            connect_ms: Some(connect_ms),
            fast: true,
            extensions: ExtensionHandshake {
                reqq: Some(250),
                ..Default::default()
            },
            availability,
            unchoke_ms,
        }
    }

    fn snapshot(peers: Vec<(&str, PeerProbe)>) -> SwarmSnapshot {
        SwarmSnapshot {
            info_hash: [0xAB; 20],
            announced_at: 10,
            probed_at: 12,
            known: 99,
            peers: peers
                .into_iter()
                .map(|(addr, probe)| ProbedPeer {
                    addr: addr.parse().unwrap(),
                    probe,
                })
                .collect(),
            pending: Vec::new(),
            complete: true,
        }
    }

    #[test]
    fn response_sorts_best_first_and_flags_seeds() {
        let snap = snapshot(vec![
            ("9.9.9.9:1", PeerProbe::failed(WireError::ConnectTimeout)),
            (
                "1.1.1.1:1",
                probe_with(Availability::Bitfield(vec![0xFF, 0x80]), None, 20),
            ),
            (
                "2.2.2.2:2",
                probe_with(Availability::Bitfield(vec![0xF0, 0x00]), Some(300), 50),
            ),
            (
                "3.3.3.3:3",
                probe_with(Availability::Everything, Some(40), 90),
            ),
        ]);
        let secret = Secret::new("s");
        let resp = response(&snap, Some(9), Some((&secret, 1234)));
        let order: Vec<&str> = resp.peers.iter().map(|p| p.addr.as_str()).collect();
        assert_eq!(order, ["3.3.3.3:3", "2.2.2.2:2", "1.1.1.1:1", "9.9.9.9:1"]);

        let full_bitfield = &resp.peers[2];
        assert!(full_bitfield.seed, "9 pieces, all set");
        assert!(full_bitfield.have.is_none());

        let partial = &resp.peers[1];
        assert!(!partial.seed);
        assert_eq!(partial.have.as_deref(), Some("8AA="));
        assert_eq!(partial.have_count, Some(4));

        let failed = &resp.peers[3];
        assert_eq!(failed.err, Some("connect_timeout"));
        assert!(!failed.seed);

        assert_eq!(resp.token_exp, Some(1234));
        let expected = token::sign(&secret, &"ab".repeat(20), "3.3.3.3:3", 1234);
        assert_eq!(resp.peers[0].t.as_deref(), Some(expected.as_str()));
        assert_eq!(resp.counts.reachable, 3);
        assert_eq!(resp.counts.seeds, 2);
        assert_eq!(resp.counts.unchoked, 2);
        assert_eq!(resp.counts.known, 99);
    }

    #[test]
    fn no_secret_no_tokens() {
        let snap = snapshot(vec![(
            "3.3.3.3:3",
            probe_with(Availability::Everything, None, 5),
        )]);
        let resp = response(&snap, None, None);
        assert!(resp.peers[0].t.is_none());
        assert!(resp.token_exp.is_none());
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("\"t\""));
    }

    #[test]
    fn pending_peers_are_listed_after_probed_ones_with_tokens() {
        let mut snap = snapshot(vec![
            ("9.9.9.9:1", PeerProbe::failed(WireError::ConnectTimeout)),
            (
                "3.3.3.3:3",
                probe_with(Availability::Everything, Some(40), 90),
            ),
        ]);
        snap.pending = vec!["4.4.4.4:4".parse().unwrap()];
        snap.complete = false;
        let secret = Secret::new("s");
        let resp = response(&snap, Some(9), Some((&secret, 1234)));
        let order: Vec<&str> = resp.peers.iter().map(|p| p.addr.as_str()).collect();
        assert_eq!(order, ["3.3.3.3:3", "4.4.4.4:4", "9.9.9.9:1"]);
        assert_eq!(resp.peers[1].err, Some("probing"));
        assert!(resp.peers[1].t.is_some());
        assert!(!resp.complete);
        assert_eq!((resp.counts.probed, resp.counts.pending), (2, 1));
    }

    fn test_config() -> Config {
        let mut cfg = Config::from_env();
        cfg.allow_private_peers = true;
        cfg.max_probe_peers = 1000;
        cfg
    }

    fn addrs(list: &[&str]) -> Vec<SocketAddr> {
        list.iter().map(|a| a.parse().unwrap()).collect()
    }

    #[test]
    fn probe_queue_orders_previous_good_first_and_previous_failures_last() {
        let previous = snapshot(vec![
            (
                "1.1.1.1:1",
                probe_with(Availability::Everything, Some(0), 10),
            ),
            ("2.2.2.2:2", PeerProbe::failed(WireError::ConnectTimeout)),
        ]);
        let mut queue = ProbeQueue::new(&test_config(), Some(&previous));
        assert_eq!(queue.take_previous_ok(), addrs(&["1.1.1.1:1"]));
        // Already probing 1.1.1.1; 2.2.2.2 failed last time: wait.
        let now = queue.announced(addrs(&["1.1.1.1:1", "2.2.2.2:2", "3.3.3.3:3", "3.3.3.3:3"]));
        assert_eq!(now, addrs(&["3.3.3.3:3"]));
        assert_eq!(queue.take_deferred(), addrs(&["2.2.2.2:2"]));
        assert_eq!(queue.known(), 3);
        queue.finished("3.3.3.3:3".parse().unwrap());
        assert_eq!(queue.pending(), addrs(&["1.1.1.1:1", "2.2.2.2:2"]));
    }

    #[test]
    fn probe_queue_caps_and_skips_private_peers() {
        let mut cfg = test_config();
        cfg.max_probe_peers = 2;
        cfg.allow_private_peers = false;
        let mut queue = ProbeQueue::new(&cfg, None);
        let now = queue.announced(addrs(&[
            "10.0.0.1:1",
            "1.1.1.1:1",
            "2.2.2.2:2",
            "3.3.3.3:3",
        ]));
        assert_eq!(now, addrs(&["1.1.1.1:1", "2.2.2.2:2"]));
        assert!(queue.announced(addrs(&["4.4.4.4:4"])).is_empty());
    }

    #[tokio::test]
    async fn cold_build_answers_before_slow_probes_finish() {
        let info_hash = [7u8; 20];
        let mut peers = Vec::new();
        for _ in 0..FIRST_PUBLISH_UNCHOKED {
            peers.push(testpeer::spawn(FakePeer::seed(info_hash)).await);
        }
        let silent = testpeer::spawn(FakePeer {
            availability: FakeAvailability::Silent,
            extensions: false,
            unchoke: false,
            ..FakePeer::seed(info_hash)
        })
        .await;
        peers.push(silent);

        let mut cfg = test_config();
        cfg.probe_timeout = Duration::from_millis(2500);
        cfg.probe_unchoke_wait = Duration::from_millis(2000);
        let cfg = Arc::new(cfg);
        let dht = Arc::new(crate::dht::DhtClient::new(0).unwrap());
        let lookup = Arc::new(Lookup::new(reqwest::Client::new(), dht));
        let cache = Arc::new(PeerCache::new(10));
        // Announce already done: the build takes the cached peer list.
        cache
            .insert(
                info_hash,
                Arc::new(LookupResult {
                    info_hash,
                    peers: peers.clone(),
                    sources: crate::lookup::Sources {
                        dht: 0,
                        trackers: vec![],
                    },
                    fetched_at: unix_now(),
                    ttl: Duration::from_secs(60),
                }),
            )
            .await;
        let service = SwarmService::new(cfg, lookup, cache);

        let started = Instant::now();
        let first = service.snapshot(info_hash, vec![]).await;
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "waited for the silent peer"
        );
        assert!(!first.complete);
        assert_eq!(first.pending, vec![silent]);
        assert_eq!(
            first.peers.iter().filter(|p| p.probe.ok()).count(),
            FIRST_PUBLISH_UNCHOKED
        );

        // A second request gets the same partial snapshot from the cache.
        assert!(!service.snapshot(info_hash, vec![]).await.complete);

        tokio::time::sleep(Duration::from_millis(3000)).await;
        let last = service.snapshot(info_hash, vec![]).await;
        assert!(last.complete);
        assert!(last.pending.is_empty());
        assert_eq!(last.peers.len(), FIRST_PUBLISH_UNCHOKED + 1);
    }

    #[test]
    fn metadata_candidates_prefer_ut_metadata_peers() {
        let mut with_metadata = probe_with(Availability::Everything, None, 80);
        with_metadata.extensions.ut_metadata = Some(2);
        with_metadata.extensions.metadata_size = Some(1000);
        let snap = snapshot(vec![
            ("1.1.1.1:1", probe_with(Availability::Everything, None, 10)),
            ("2.2.2.2:2", with_metadata),
            ("3.3.3.3:3", PeerProbe::failed(WireError::Closed)),
        ]);
        let order: Vec<String> = metadata_candidates(&snap)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(order, ["2.2.2.2:2", "1.1.1.1:1"]);
    }
}
