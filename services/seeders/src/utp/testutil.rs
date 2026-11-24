//! Test helpers shared by the uTP socket tests and the tunnel's uTP tests:
//! a lossy UDP relay on loopback.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

/// Largest UDP payload.
const MAX_DATAGRAM: usize = 65_535;

#[derive(Clone, Copy)]
pub struct Link {
    pub loss: f64,
    pub dup: f64,
    /// Each datagram is delayed by a uniform 0..jitter (reordering).
    pub jitter: Duration,
}

/// 5 % loss, 2 % duplication, up to 4 ms of reordering, both ways.
pub const LOSSY: Link = Link {
    loss: 0.05,
    dup: 0.02,
    jitter: Duration::from_millis(4),
};

/// A UDP relay between one client and `server`: the client sends to the
/// returned address, the server sees the relay's other socket. Every
/// datagram, both ways, may be dropped, duplicated or delayed.
pub async fn relay(server: SocketAddr, link: Link, seed: u64) -> SocketAddr {
    let local: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let front = Arc::new(UdpSocket::bind(local).await.unwrap());
    let back = Arc::new(UdpSocket::bind(local).await.unwrap());
    let addr = front.local_addr().unwrap();
    tokio::spawn(async move {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut client: Option<SocketAddr> = None;
        let (mut a, mut b) = (vec![0u8; MAX_DATAGRAM], vec![0u8; MAX_DATAGRAM]);
        loop {
            let (data, out, to) = tokio::select! {
                r = front.recv_from(&mut a) => {
                    let Ok((n, from)) = r else { continue };
                    client = Some(from);
                    (a[..n].to_vec(), back.clone(), server)
                }
                r = back.recv_from(&mut b) => {
                    let Ok((n, from)) = r else { continue };
                    let Some(c) = client.filter(|_| from == server) else { continue };
                    (b[..n].to_vec(), front.clone(), c)
                }
            };
            if rng.gen_bool(link.loss) {
                continue;
            }
            let copies = if rng.gen_bool(link.dup) { 2 } else { 1 };
            for _ in 0..copies {
                let delay = link.jitter.mul_f64(rng.gen::<f64>());
                let (out, data) = (out.clone(), data.clone());
                if delay.is_zero() {
                    let _ = out.send_to(&data, to).await;
                } else {
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = out.send_to(&data, to).await;
                    });
                }
            }
        }
    });
    addr
}
