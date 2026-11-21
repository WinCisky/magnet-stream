//! Scouting for the streamed `/peers` (docs/architecture.md §Chunk recovery
//! › Scouting on the VPS; decisions OQ-21, OQ-22). Every address discovery
//! finds is probed once (`probe::probe`: connect, handshake, first
//! messages), scored from 0 to 1, and remembered in memory so a later
//! stream for the same info-hash gets the ranked list at once.
//!
//! - **Score.** About 0.5 for a completed handshake, more for a seed (or the
//!   share of pieces), for offering the metadata, for unchoking the probe and
//!   for a short round trip; near 0 for a dead address; [`UNKNOWN_SCORE`]
//!   for one never probed. Scores fade toward [`UNKNOWN_SCORE`] with a
//!   half-life, because home addresses and ports change within hours.
//! - **Memory.** One record per info-hash, weighed in bytes (moka), with a
//!   cap on addresses per info-hash and an idle expiry. Nothing on disk.
//! - **Limits.** A global pool of probes (shared with `/swarm`), at most
//!   `max_per_hash` probes per info-hash per `reprobe_after` window, and one
//!   probe per (info-hash, address) at a time, shared by concurrent streams.
//!   The per-stream limit is applied by the stream (`discovery`).
//!
//! Scores are advice for the browser's dial order, nothing more: the
//! tunnel's handshake gate still decides what may be reached. Nothing here
//! logs addresses or info-hashes.

use crate::peerwire::WireError;
use crate::probe::{self, Availability, PeerProbe, ProbeTimeouts};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Semaphore};

/// The score of an address nobody has probed (and what every score fades to).
pub const UNKNOWN_SCORE: f32 = 0.2;
/// The score of an address whose probe failed, when fresh.
const DEAD_SCORE: f32 = 0.02;
/// Estimated bytes per remembered address: the key (32), the entry (~32)
/// and the hash table's overhead.
pub const ENTRY_WEIGHT: u32 = 96;
/// Estimated fixed bytes per remembered info-hash.
pub const SWARM_WEIGHT: u32 = 256;
/// A probe that outlives its own deadline by this much is abandoned.
const PROBE_SLACK: Duration = Duration::from_secs(1);

/// What one probe learned about an address.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Facts {
    /// It completed the BitTorrent handshake for this info-hash.
    pub alive: bool,
    pub seed: bool,
    /// Share of pieces it has (0..1), if it said.
    pub have: Option<f32>,
    /// It offers the metadata (`ut_metadata` with a `metadata_size`).
    pub md: bool,
    /// It unchoked the probe.
    pub unchoked: bool,
    /// TCP connect time, a round trip.
    pub rtt_ms: Option<u16>,
}

impl Facts {
    pub fn dead() -> Self {
        Self {
            alive: false,
            seed: false,
            have: None,
            md: false,
            unchoked: false,
            rtt_ms: None,
        }
    }

    pub fn from_probe(p: &PeerProbe) -> Self {
        // A handshake completed, then the peer went quiet: still reachable.
        let alive = p.connect_ms.is_some() && matches!(p.error, None | Some(WireError::Timeout));
        if !alive {
            return Self::dead();
        }
        let (seed, have) = match &p.availability {
            Availability::Unknown => (false, None),
            Availability::Nothing => (false, Some(0.0)),
            Availability::Everything => (true, Some(1.0)),
            Availability::Bitfield(bits) => {
                let seed = probe::is_complete(bits, None);
                let total = (bits.len() * 8).max(1) as f32;
                let share = if seed {
                    1.0
                } else {
                    (probe::count_pieces(bits, None) as f32 / total).clamp(0.0, 1.0)
                };
                (seed, Some(share))
            }
        };
        Self {
            alive: true,
            seed,
            have,
            md: p.extensions.ut_metadata.is_some() && p.extensions.metadata_size.is_some(),
            unchoked: p.unchoke_ms.is_some(),
            rtt_ms: p.connect_ms.map(|ms| u16::try_from(ms).unwrap_or(u16::MAX)),
        }
    }

    /// The score when just probed, in 0..=1.
    pub fn base_score(&self) -> f32 {
        if !self.alive {
            return DEAD_SCORE;
        }
        let mut s = 0.5;
        if self.seed {
            s += 0.2;
        } else if let Some(have) = self.have {
            s += 0.15 * have;
        }
        if self.md {
            s += 0.05;
        }
        if self.unchoked {
            s += 0.15;
        }
        if let Some(rtt) = self.rtt_ms {
            s += 0.05 * (1.0 - f32::from(rtt.min(1000)) / 1000.0);
        }
        s.clamp(0.0, 1.0)
    }
}

/// A remembered probe result.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Entry {
    pub facts: Facts,
    pub probed_at: Instant,
}

impl Entry {
    /// The score now: the base score faded toward [`UNKNOWN_SCORE`].
    pub fn score(&self, now: Instant, half_life: Duration) -> f32 {
        decay(
            self.facts.base_score(),
            now.saturating_duration_since(self.probed_at),
            half_life,
        )
    }
}

pub fn decay(base: f32, age: Duration, half_life: Duration) -> f32 {
    if half_life.is_zero() {
        return UNKNOWN_SCORE;
    }
    let halves = age.as_secs_f32() / half_life.as_secs_f32();
    UNKNOWN_SCORE + (base - UNKNOWN_SCORE) * 0.5f32.powf(halves)
}

#[derive(Debug, Clone)]
pub struct ScoutSettings {
    pub timeouts: ProbeTimeouts,
    /// Probes per info-hash within one `reprobe_after` window.
    pub max_per_hash: usize,
    /// An address probed more recently than this is not probed again.
    pub reprobe_after: Duration,
    pub half_life: Duration,
    /// A record nobody used for this long is dropped.
    pub idle: Duration,
    /// Byte budget of the whole memory.
    pub cache_bytes: u64,
    /// Addresses remembered per info-hash.
    pub max_addrs: usize,
}

pub type ProbeFuture = Pin<Box<dyn Future<Output = PeerProbe> + Send>>;
/// How an address is probed (the real `probe::probe`, or a fake in tests).
pub type Prober = Arc<dyn Fn(SocketAddr, [u8; 20], ProbeTimeouts) -> ProbeFuture + Send + Sync>;

pub fn real_prober() -> Prober {
    Arc::new(|addr, info_hash, timeouts| Box::pin(probe::probe(addr, info_hash, timeouts)))
}

/// What [`Scout::ask`] decided for an address.
pub enum Ask {
    /// Probed recently: here is what we know.
    Fresh(Entry),
    /// A probe is running (perhaps for another stream): wait for it.
    Wait(watch::Receiver<Option<Entry>>),
    /// This info-hash used its probe budget: report the address unprobed.
    OverBudget,
}

struct Memory {
    entries: HashMap<SocketAddr, Entry>,
    window_start: Instant,
    window_probes: usize,
}

type Record = Arc<Mutex<Memory>>;
type ProbeKey = ([u8; 20], SocketAddr);

pub struct Scout {
    settings: ScoutSettings,
    prober: Prober,
    permits: Arc<Semaphore>,
    memory: moka::future::Cache<[u8; 20], Record>,
    inflight: Mutex<HashMap<ProbeKey, watch::Sender<Option<Entry>>>>,
}

impl Scout {
    /// `permits` is the global probe pool (shared with `/swarm`).
    pub fn new(settings: ScoutSettings, prober: Prober, permits: Arc<Semaphore>) -> Arc<Self> {
        let memory = moka::future::Cache::builder()
            .weigher(|_: &[u8; 20], record: &Record| weigh(record))
            .max_capacity(settings.cache_bytes)
            .time_to_idle(settings.idle)
            .build();
        Arc::new(Self {
            settings,
            prober,
            permits,
            memory,
            inflight: Mutex::new(HashMap::new()),
        })
    }

    pub fn settings(&self) -> &ScoutSettings {
        &self.settings
    }

    /// Every address of `info_hash` probed within `reprobe_after`, best
    /// first (alive ones by score, then the dead).
    pub async fn fresh(&self, info_hash: [u8; 20]) -> Vec<(SocketAddr, Entry)> {
        let Some(record) = self.memory.get(&info_hash).await else {
            return Vec::new();
        };
        let now = Instant::now();
        let mut out: Vec<(SocketAddr, Entry)> = {
            let memory = lock(&record);
            memory
                .entries
                .iter()
                .filter(|(_, e)| {
                    now.saturating_duration_since(e.probed_at) < self.settings.reprobe_after
                })
                .map(|(a, e)| (*a, *e))
                .collect()
        };
        let half_life = self.settings.half_life;
        out.sort_by(|a, b| {
            b.1.score(now, half_life)
                .total_cmp(&a.1.score(now, half_life))
                .then(a.0.cmp(&b.0))
        });
        out
    }

    /// Fresh facts, a probe to wait for (started now or shared), or "over
    /// budget".
    pub async fn ask(self: &Arc<Self>, info_hash: [u8; 20], addr: SocketAddr) -> Ask {
        let key = (info_hash, addr);
        if let Some(tx) = lock(&self.inflight).get(&key) {
            return Ask::Wait(tx.subscribe());
        }
        let record = self.record(info_hash).await;
        {
            let now = Instant::now();
            let mut memory = lock(&record);
            if let Some(entry) = memory.entries.get(&addr) {
                if now.saturating_duration_since(entry.probed_at) < self.settings.reprobe_after {
                    return Ask::Fresh(*entry);
                }
            }
            if now.saturating_duration_since(memory.window_start) >= self.settings.reprobe_after {
                memory.window_start = now;
                memory.window_probes = 0;
            }
            if memory.window_probes >= self.settings.max_per_hash {
                return Ask::OverBudget;
            }
            memory.window_probes += 1;
        }
        let rx = {
            let mut inflight = lock(&self.inflight);
            if let Some(tx) = inflight.get(&key) {
                return Ask::Wait(tx.subscribe());
            }
            let (tx, rx) = watch::channel(None);
            inflight.insert(key, tx);
            rx
        };
        let this = self.clone();
        tokio::spawn(async move { this.run_probe(key).await });
        Ask::Wait(rx)
    }

    async fn run_probe(self: Arc<Self>, key: ProbeKey) {
        let _cleanup = InflightGuard {
            scout: self.clone(),
            key,
        };
        let Ok(_permit) = self.permits.clone().acquire_owned().await else {
            return;
        };
        // Every stream waiting for it went away while it queued: skip it.
        let wanted = lock(&self.inflight)
            .get(&key)
            .is_some_and(|tx| tx.receiver_count() > 0);
        if !wanted {
            return;
        }
        let (info_hash, addr) = key;
        let timeouts = self.settings.timeouts;
        let probed = tokio::time::timeout(
            timeouts.total + PROBE_SLACK,
            (self.prober)(addr, info_hash, timeouts),
        )
        .await;
        let facts = match probed {
            Ok(p) => Facts::from_probe(&p),
            Err(_) => Facts::dead(),
        };
        let entry = Entry {
            facts,
            probed_at: Instant::now(),
        };
        self.remember(info_hash, addr, entry).await;
        // Bound first: as the tail expression, the guard would outlive
        // `_cleanup`, whose drop takes the same lock.
        let waiting = lock(&self.inflight).remove(&key);
        if let Some(tx) = waiting {
            tx.send_replace(Some(entry));
        }
    }

    /// Store a probe result, keeping at most `max_addrs` per info-hash
    /// (the oldest dead entry goes first, then the oldest of all).
    pub async fn remember(&self, info_hash: [u8; 20], addr: SocketAddr, entry: Entry) {
        let record = self.record(info_hash).await;
        {
            let mut memory = lock(&record);
            if !memory.entries.contains_key(&addr)
                && memory.entries.len() >= self.settings.max_addrs.max(1)
            {
                let victim = memory
                    .entries
                    .iter()
                    .min_by_key(|(_, e)| (e.facts.alive, e.probed_at))
                    .map(|(a, _)| *a);
                if let Some(victim) = victim {
                    memory.entries.remove(&victim);
                }
            }
            memory.entries.insert(addr, entry);
        }
        // Insert again so the cache re-weighs the record.
        self.memory.insert(info_hash, record).await;
    }

    async fn record(&self, info_hash: [u8; 20]) -> Record {
        self.memory
            .get_with(info_hash, async {
                Arc::new(Mutex::new(Memory {
                    entries: HashMap::new(),
                    window_start: Instant::now(),
                    window_probes: 0,
                }))
            })
            .await
    }

    /// Bytes the memory holds by its own estimate (after pending evictions).
    #[cfg(test)]
    pub async fn weighted_size(&self) -> u64 {
        self.memory.run_pending_tasks().await;
        self.memory.weighted_size()
    }

    #[cfg(test)]
    pub async fn remembered(&self, info_hash: [u8; 20]) -> usize {
        match self.memory.get(&info_hash).await {
            Some(record) => lock(&record).entries.len(),
            None => 0,
        }
    }
}

fn weigh(record: &Record) -> u32 {
    let n = u32::try_from(lock(record).entries.len()).unwrap_or(u32::MAX);
    SWARM_WEIGHT.saturating_add(n.saturating_mul(ENTRY_WEIGHT))
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Removes an in-flight probe however its task ends (done, skipped,
/// panicked), so waiters see the sender drop instead of hanging.
struct InflightGuard {
    scout: Arc<Scout>,
    key: ProbeKey,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        lock(&self.scout.inflight).remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peerwire::ExtensionHandshake;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn settings() -> ScoutSettings {
        ScoutSettings {
            timeouts: ProbeTimeouts {
                connect: Duration::from_millis(200),
                total: Duration::from_millis(500),
                unchoke_wait: Duration::from_millis(100),
            },
            max_per_hash: 1000,
            reprobe_after: Duration::from_secs(600),
            half_life: Duration::from_secs(1800),
            idle: Duration::from_secs(7200),
            cache_bytes: 16 << 20,
            max_addrs: 1000,
        }
    }

    fn alive_probe(availability: Availability, unchoke: bool, md: bool) -> PeerProbe {
        PeerProbe {
            error: None,
            connect_ms: Some(40),
            fast: true,
            extensions: ExtensionHandshake {
                ut_metadata: md.then_some(3),
                metadata_size: md.then_some(1000),
                ..ExtensionHandshake::default()
            },
            availability,
            unchoke_ms: unchoke.then_some(10),
        }
    }

    fn addr(i: u32) -> SocketAddr {
        SocketAddr::from(([10, (i >> 16) as u8, (i >> 8) as u8, i as u8], 6881))
    }

    /// A prober that answers "alive seed" after `delay` and counts calls.
    fn counting(delay: Duration, calls: Arc<AtomicUsize>) -> Prober {
        Arc::new(move |_, _, _| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(delay).await;
                alive_probe(Availability::Everything, true, true)
            })
        })
    }

    #[test]
    fn facts_from_probes() {
        let seed = Facts::from_probe(&alive_probe(Availability::Everything, true, true));
        assert!(seed.alive && seed.seed && seed.md && seed.unchoked);
        assert_eq!(seed.have, Some(1.0));
        assert_eq!(seed.rtt_ms, Some(40));

        let partial = Facts::from_probe(&alive_probe(
            Availability::Bitfield(vec![0b1010_0000]),
            false,
            false,
        ));
        assert!(partial.alive && !partial.seed && !partial.md && !partial.unchoked);
        assert_eq!(partial.have, Some(0.25));

        // Handshake done, then silence: reachable.
        let quiet = PeerProbe {
            error: Some(WireError::Timeout),
            ..alive_probe(Availability::Unknown, false, false)
        };
        assert!(Facts::from_probe(&quiet).alive);

        for error in [
            WireError::ConnectTimeout,
            WireError::ConnectFailed("refused".into()),
            WireError::InfoHashMismatch,
            WireError::BadHandshake,
        ] {
            assert_eq!(Facts::from_probe(&PeerProbe::failed(error)), Facts::dead());
        }
        let closed_after_handshake = PeerProbe {
            error: Some(WireError::Closed),
            ..alive_probe(Availability::Unknown, false, false)
        };
        assert!(!Facts::from_probe(&closed_after_handshake).alive);
    }

    #[test]
    fn scores_rank_seeds_over_partials_over_unknown_over_dead() {
        let seed =
            Facts::from_probe(&alive_probe(Availability::Everything, true, true)).base_score();
        let partial = Facts::from_probe(&alive_probe(
            Availability::Bitfield(vec![0b1000_0000]),
            false,
            false,
        ))
        .base_score();
        let dead = Facts::dead().base_score();
        assert!(seed > partial && partial > UNKNOWN_SCORE && UNKNOWN_SCORE > dead);
        assert!(seed <= 1.0 && dead >= 0.0);
    }

    #[test]
    fn scores_fade_toward_unknown_with_the_half_life() {
        let hl = Duration::from_secs(1800);
        assert!((decay(0.9, Duration::ZERO, hl) - 0.9).abs() < 1e-6);
        assert!((decay(0.9, hl, hl) - 0.55).abs() < 1e-6);
        assert!((decay(0.02, hl, hl) - 0.11).abs() < 1e-6);
        assert!((decay(0.9, hl * 20, hl) - UNKNOWN_SCORE).abs() < 1e-3);
        let entry = Entry {
            facts: Facts::from_probe(&alive_probe(Availability::Everything, true, true)),
            probed_at: Instant::now(),
        };
        let later = entry.probed_at + hl;
        let expected = UNKNOWN_SCORE + (entry.facts.base_score() - UNKNOWN_SCORE) / 2.0;
        assert!((entry.score(later, hl) - expected).abs() < 1e-5);
    }

    #[tokio::test]
    async fn concurrent_asks_share_one_probe_and_later_asks_are_fresh() {
        let calls = Arc::new(AtomicUsize::new(0));
        let scout = Scout::new(
            settings(),
            counting(Duration::from_millis(50), calls.clone()),
            Arc::new(Semaphore::new(8)),
        );
        let (Ask::Wait(mut a), Ask::Wait(mut b)) = (
            scout.ask([1; 20], addr(1)).await,
            scout.ask([1; 20], addr(1)).await,
        ) else {
            panic!("expected two waits");
        };
        let ea = a.wait_for(Option::is_some).await.unwrap().unwrap();
        let eb = b.wait_for(Option::is_some).await.unwrap().unwrap();
        assert_eq!(ea, eb);
        assert!(ea.facts.alive);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(scout.ask([1; 20], addr(1)).await, Ask::Fresh(_)));
        // Another info-hash is another probe.
        assert!(matches!(scout.ask([2; 20], addr(1)).await, Ask::Wait(_)));
        assert_eq!(scout.fresh([1; 20]).await.len(), 1);
    }

    #[tokio::test]
    async fn a_probe_nobody_waits_for_is_skipped() {
        let calls = Arc::new(AtomicUsize::new(0));
        let permits = Arc::new(Semaphore::new(1));
        let held = permits.clone().acquire_owned().await.unwrap();
        let scout = Scout::new(settings(), counting(Duration::ZERO, calls.clone()), permits);
        drop(scout.ask([1; 20], addr(1)).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // And a new ask starts over rather than waiting on a dead probe.
        let Ask::Wait(mut rx) = scout.ask([1; 20], addr(1)).await else {
            panic!("expected a wait");
        };
        assert!(rx.wait_for(Option::is_some).await.is_ok());
    }

    #[tokio::test]
    async fn the_budget_per_info_hash_caps_probes_per_window() {
        let calls = Arc::new(AtomicUsize::new(0));
        let scout = Scout::new(
            ScoutSettings {
                max_per_hash: 2,
                ..settings()
            },
            counting(Duration::ZERO, calls.clone()),
            Arc::new(Semaphore::new(8)),
        );
        let mut over = 0;
        for i in 0..4 {
            if matches!(scout.ask([1; 20], addr(i)).await, Ask::OverBudget) {
                over += 1;
            }
        }
        assert_eq!(over, 2);
        assert!(matches!(scout.ask([2; 20], addr(9)).await, Ask::Wait(_)));
    }

    #[tokio::test]
    async fn a_hung_probe_is_abandoned_as_dead() {
        let prober: Prober = Arc::new(|_, _, _| Box::pin(std::future::pending()));
        let scout = Scout::new(settings(), prober, Arc::new(Semaphore::new(8)));
        let Ask::Wait(mut rx) = scout.ask([1; 20], addr(1)).await else {
            panic!("expected a wait");
        };
        let entry = rx.wait_for(Option::is_some).await.unwrap().unwrap();
        assert!(!entry.facts.alive);
    }

    #[tokio::test]
    async fn keeps_at_most_max_addrs_per_info_hash_dropping_dead_first() {
        let scout = Scout::new(
            ScoutSettings {
                max_addrs: 10,
                ..settings()
            },
            counting(Duration::ZERO, Arc::new(AtomicUsize::new(0))),
            Arc::new(Semaphore::new(8)),
        );
        let now = Instant::now();
        let alive = Facts::from_probe(&alive_probe(Availability::Everything, false, false));
        for i in 0..10 {
            let facts = if i < 3 { Facts::dead() } else { alive };
            scout
                .remember(
                    [1; 20],
                    addr(i),
                    Entry {
                        facts,
                        probed_at: now,
                    },
                )
                .await;
        }
        for i in 10..13 {
            scout
                .remember(
                    [1; 20],
                    addr(i),
                    Entry {
                        facts: alive,
                        probed_at: now,
                    },
                )
                .await;
        }
        let kept = scout.fresh([1; 20]).await;
        assert_eq!(kept.len(), 10);
        assert!(kept.iter().all(|(_, e)| e.facts.alive));
        // Best first.
        assert!(kept
            .windows(2)
            .all(|w| w[0].1.score(now, settings().half_life)
                >= w[1].1.score(now, settings().half_life)));
    }

    #[tokio::test]
    async fn memory_stays_within_its_byte_budget() {
        let budget = 64 * 1024;
        let scout = Scout::new(
            ScoutSettings {
                cache_bytes: budget,
                max_addrs: 100,
                ..settings()
            },
            counting(Duration::ZERO, Arc::new(AtomicUsize::new(0))),
            Arc::new(Semaphore::new(8)),
        );
        let now = Instant::now();
        for h in 0..200u32 {
            let ih = {
                let mut ih = [0u8; 20];
                ih[..4].copy_from_slice(&h.to_be_bytes());
                ih
            };
            for i in 0..100 {
                scout
                    .remember(
                        ih,
                        addr(i),
                        Entry {
                            facts: Facts::dead(),
                            probed_at: now,
                        },
                    )
                    .await;
            }
        }
        let size = scout.weighted_size().await;
        assert!(size <= budget, "{size} > {budget}");
        assert!(size > 0);
        // A full record weighs what the estimate says.
        assert_eq!(
            u64::from(SWARM_WEIGHT + 100 * ENTRY_WEIGHT),
            u64::from(weigh(&Arc::new(Mutex::new(Memory {
                entries: (0..100)
                    .map(|i| (
                        addr(i),
                        Entry {
                            facts: Facts::dead(),
                            probed_at: now
                        }
                    ))
                    .collect(),
                window_start: now,
                window_probes: 0,
            }))))
        );
        assert!(scout.remembered([0; 20]).await <= 100);
    }
}
