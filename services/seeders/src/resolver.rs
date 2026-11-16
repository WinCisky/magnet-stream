//! Custom DNS resolver plugged into the shared `reqwest::Client`, so every
//! HTTP(S) tracker request is subject to the same SSRF guard as UDP
//! trackers: only public, routable addresses are ever connected to.

use crate::netguard;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

pub struct GuardedResolver;

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_string();
            let addrs = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let filtered: Vec<std::net::SocketAddr> =
                addrs.filter(|a| netguard::is_public(a.ip())).collect();
            if filtered.is_empty() {
                return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "no public address for host {host}"
                )));
            }
            Ok(Box::new(filtered.into_iter()) as Addrs)
        })
    }
}
