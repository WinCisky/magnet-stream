//! Thin wrapper around the `mainline` BitTorrent DHT client, used to look up
//! peers for an info-hash independently of (and in parallel with) trackers.

use futures_lite::StreamExt;
use mainline::async_dht::AsyncDht;
use mainline::{Dht, Id};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

pub struct DhtClient {
    dht: AsyncDht,
}

impl DhtClient {
    /// Starts a DHT node listening on `port`. This does not block on
    /// bootstrapping — the routing table populates itself as needed.
    pub fn new(port: u16) -> std::io::Result<Self> {
        let dht = Dht::builder().port(port).build()?.as_async();
        Ok(Self { dht })
    }

    /// Best-effort warm-up so the first real request isn't the one paying
    /// for the initial bootstrap. Never blocks startup for long.
    pub async fn warm_up(&self) {
        match tokio::time::timeout(Duration::from_secs(10), self.dht.bootstrapped()).await {
            Ok(true) => tracing::info!("DHT bootstrap complete"),
            Ok(false) => tracing::warn!("DHT bootstrap finished but found no nodes"),
            Err(_) => tracing::warn!("DHT bootstrap did not finish within 10s, continuing anyway"),
        }
    }

    /// Look up peers announced for `info_hash`, collecting responses until
    /// either the DHT query naturally completes or `deadline` elapses,
    /// whichever is first. Each new batch also goes to `found_tx`, if any.
    pub async fn get_peers(
        &self,
        info_hash: [u8; 20],
        deadline: Duration,
        found_tx: Option<UnboundedSender<Vec<SocketAddr>>>,
    ) -> Vec<SocketAddr> {
        let id = Id::from(info_hash);
        let mut stream = self.dht.get_peers(id);
        let mut found = HashSet::new();

        let _ = tokio::time::timeout(deadline, async {
            while let Some(batch) = stream.next().await {
                let fresh: Vec<SocketAddr> = batch
                    .into_iter()
                    .map(SocketAddr::V4)
                    .filter(|a| found.insert(*a))
                    .collect();
                if let Some(tx) = &found_tx {
                    if !fresh.is_empty() {
                        let _ = tx.send(fresh);
                    }
                }
            }
        })
        .await;

        found.into_iter().collect()
    }
}
