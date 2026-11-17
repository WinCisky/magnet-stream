//! Per-info-hash result cache. Two requests for the same hash within the
//! entry's TTL get the identical cached peer list; concurrent misses for the
//! same hash are coalesced into a single in-flight lookup by `moka`.

use crate::lookup::LookupResult;
use moka::future::Cache;
use moka::Expiry;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct PeerCache {
    inner: Cache<[u8; 20], Arc<LookupResult>>,
}

impl PeerCache {
    pub fn new(max_capacity: u64) -> Self {
        let inner = Cache::builder()
            .max_capacity(max_capacity)
            .expire_after(PerEntryTtl)
            .build();
        Self { inner }
    }

    /// Returns the result for `key`, and whether it was already cached.
    /// `compute` only runs on a genuine miss, and concurrent misses for the
    /// same key share a single call to it.
    pub async fn get_or_compute<F>(&self, key: [u8; 20], compute: F) -> (Arc<LookupResult>, bool)
    where
        F: Future<Output = Arc<LookupResult>>,
    {
        if let Some(hit) = self.inner.get(&key).await {
            return (hit, true);
        }
        let value = self.inner.get_with(key, compute).await;
        (value, false)
    }
}

impl PeerCache {
    /// The cached result for `key`, if any (no lookup is started).
    pub async fn get(&self, key: &[u8; 20]) -> Option<Arc<LookupResult>> {
        self.inner.get(key).await
    }

    pub async fn insert(&self, key: [u8; 20], value: Arc<LookupResult>) {
        self.inner.insert(key, value).await;
    }
}

/// Each [`LookupResult`] already carries the TTL it should be cached for
/// (shorter for an empty/failed result) — this expiry just reads it back.
struct PerEntryTtl;

impl Expiry<[u8; 20], Arc<LookupResult>> for PerEntryTtl {
    fn expire_after_create(
        &self,
        _key: &[u8; 20],
        value: &Arc<LookupResult>,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lookup::Sources;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dummy_result(ttl: Duration) -> Arc<LookupResult> {
        Arc::new(LookupResult {
            info_hash: [0u8; 20],
            peers: vec![],
            sources: Sources {
                dht: 0,
                trackers: vec![],
            },
            fetched_at: 0,
            ttl,
        })
    }

    #[tokio::test]
    async fn coalesces_concurrent_misses() {
        let cache = Arc::new(PeerCache::new(1000));
        let calls = Arc::new(AtomicUsize::new(0));
        let key = [1u8; 20];

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let calls = calls.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_compute(key, async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        dummy_result(Duration::from_secs(60))
                    })
                    .await
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn second_request_is_a_cache_hit() {
        let cache = PeerCache::new(1000);
        let key = [2u8; 20];

        let (_, cached_first) = cache
            .get_or_compute(key, async { dummy_result(Duration::from_secs(60)) })
            .await;
        assert!(!cached_first);

        let (_, cached_second) = cache
            .get_or_compute(key, async { dummy_result(Duration::from_secs(60)) })
            .await;
        assert!(cached_second);
    }

    #[tokio::test]
    async fn short_ttl_entry_expires_quickly() {
        let cache = PeerCache::new(1000);
        let key = [3u8; 20];

        cache
            .get_or_compute(key, async { dummy_result(Duration::from_millis(20)) })
            .await;

        tokio::time::sleep(Duration::from_millis(150)).await;
        // moka's expiration is lazy; poke it so the entry is actually reaped/inspected.
        cache.inner.run_pending_tasks().await;

        let (_, cached) = cache
            .get_or_compute(key, async { dummy_result(Duration::from_secs(60)) })
            .await;
        assert!(!cached, "entry should have expired by now");
    }
}
