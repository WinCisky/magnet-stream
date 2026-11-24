//! LEDBAT on the simulated link (M4-04c): a bottleneck with a FIFO queue
//! drained at a fixed rate, propagation delay, optional loss. Declared
//! targets, checked here:
//!
//! - **Throughput**: once converged (after 10 s), a bulk transfer moves at
//!   least 80 % of the bottleneck's capacity in payload bytes, with a deep
//!   queue (where the delay signal governs) and a shallow one (where
//!   losses do).
//! - **Delay**: with a deep queue the queueing delay it causes stays near
//!   the 100 ms target (mean under 150 ms) and the queue never overflows.
//! - **Back-off**: the window shrinks when the delay rises and on loss, and
//!   **never stalls**: bytes keep arriving every second.

use super::*;

/// A one-way bulk transfer A → B of zeros, measured.
struct Bulk {
    delivered: u64,
    /// (time, bytes delivered so far, A's window, A's queueing delay).
    trace: Vec<(Duration, u64, usize, Duration)>,
    next_sample: Duration,
}

const SAMPLE_EVERY: Duration = Duration::from_millis(100);

impl Bulk {
    fn new() -> Self {
        Self {
            delivered: 0,
            trace: Vec::new(),
            next_sample: Duration::ZERO,
        }
    }

    /// Runs for `duration` of simulated time from now.
    fn run(&mut self, sim: &mut Sim, duration: Duration) {
        static ZEROS: [u8; 64 * 1024] = [0; 64 * 1024];
        let until = sim.elapsed() + duration;
        let mut buf = vec![0u8; 64 * 1024];
        sim.run(until + secs(1), |s| {
            let a = &mut s.eps[A].conns[0];
            while a.write(&ZEROS) > 0 {}
            let b = &mut s.eps[B].conns[0];
            loop {
                let n = b.read(&mut buf);
                if n == 0 {
                    break;
                }
                self.delivered += n as u64;
            }
            let t = s.elapsed();
            if t >= self.next_sample {
                let cwnd = s.eps[A].conns[0].congestion().window();
                self.trace.push((t, self.delivered, cwnd, s.queue_delay[A]));
                self.next_sample = t + SAMPLE_EVERY;
            }
            t >= until
        });
    }

    fn delivered_at(&self, t: Duration) -> u64 {
        self.trace
            .iter()
            .take_while(|e| e.0 <= t)
            .last()
            .map_or(0, |e| e.1)
    }

    /// Payload bytes per second between `from` and `to`.
    fn rate(&self, from: Duration, to: Duration) -> f64 {
        (self.delivered_at(to) - self.delivered_at(from)) as f64 / (to - from).as_secs_f64()
    }

    fn window_at(&self, t: Duration) -> usize {
        self.trace
            .iter()
            .take_while(|e| e.0 <= t)
            .last()
            .map_or(0, |e| e.2)
    }

    fn mean_queue_delay(&self, from: Duration, to: Duration) -> Duration {
        let v: Vec<Duration> = self
            .trace
            .iter()
            .filter(|e| e.0 >= from && e.0 <= to)
            .map(|e| e.3)
            .collect();
        v.iter().sum::<Duration>() / v.len().max(1) as u32
    }

    /// Longest stretch of simulated time with no byte delivered, after
    /// `from`.
    fn longest_stall(&self, from: Duration) -> Duration {
        let mut longest = Duration::ZERO;
        let mut last_progress = from;
        let mut last_bytes = self.delivered_at(from);
        for &(t, bytes, ..) in self.trace.iter().filter(|e| e.0 > from) {
            if bytes > last_bytes {
                last_bytes = bytes;
                last_progress = t;
            }
            longest = longest.max(t - last_progress);
        }
        longest
    }
}

fn bottleneck(rate: u64, one_way: Duration, queue: usize) -> LinkCfg {
    LinkCfg {
        loss: 0.0,
        dup: 0.0,
        delay: one_way,
        jitter: Duration::ZERO,
        rate,
        queue,
    }
}

const MB: u64 = 1_000_000;

/// Deep queues (well above the 100 ms target): LEDBAT fills the pipe and
/// holds the queue near the target without ever overflowing it.
#[test]
fn converges_to_capacity_with_delay_near_target() {
    for (rate, one_way_ms, seed) in [(MB, 20u64, 1u64), (2 * MB, 30, 2), (MB / 4, 10, 3)] {
        // A queue of 400 ms at this rate.
        let link = bottleneck(rate, ms(one_way_ms), (rate * 4 / 10) as usize);
        let mut sim = established(link, seed, Config::default());
        sim.keep_log = false;
        let start = sim.elapsed();
        let mut bulk = Bulk::new();
        bulk.run(&mut sim, secs(40));
        let (from, to) = (start + secs(10), start + secs(40));
        let achieved = bulk.rate(from, to);
        let delay = bulk.mean_queue_delay(from, to);
        let label = format!("{rate} B/s, {one_way_ms} ms: {achieved:.0} B/s, queue {delay:?}");
        assert!(achieved >= 0.80 * rate as f64, "{label}");
        assert!(achieved <= rate as f64, "{label}");
        assert!(delay >= ms(50) && delay <= ms(150), "{label}");
        assert_eq!(sim.queue_drops[A], 0, "{label}");
        let cc = sim.eps[A].conns[0].congestion();
        assert_eq!(cc.stats().loss_decreases, 0, "{label}");
        assert_eq!(cc.stats().timeout_decreases, 0, "{label}");
        assert!(!cc.in_slow_start(), "{label}");
        // The window settles around capacity × (round trip + target).
        let w = cc.window() as f64;
        let ideal = rate as f64 * (2.0 * one_way_ms as f64 / 1000.0 + 0.1);
        assert!(w > 0.6 * ideal && w < 1.5 * ideal, "{label}: window {w}");
    }
}

/// A shallow queue (below the target): the delay never reaches 100 ms, so
/// the window grows until the queue overflows, and losses halve it.
#[test]
fn shallow_queue_is_governed_by_losses() {
    let rate = MB;
    // 30 ms of queue.
    let link = bottleneck(rate, ms(20), 30_000);
    let mut sim = established(link, 4, Config::default());
    sim.keep_log = false;
    let start = sim.elapsed();
    let mut bulk = Bulk::new();
    bulk.run(&mut sim, secs(60));
    let (from, to) = (start + secs(10), start + secs(60));
    let achieved = bulk.rate(from, to);
    assert!(achieved >= 0.80 * rate as f64, "{achieved:.0} B/s");
    let cc = sim.eps[A].conns[0].congestion();
    assert!(sim.queue_drops[A] > 0);
    assert!(cc.stats().loss_decreases >= 5, "{:?}", cc.stats());
    // Sawtooth: the window never runs away beyond what the path holds.
    let path = rate as f64 * 0.04 + 30_000.0;
    let peak = bulk.trace.iter().map(|e| e.2).max().unwrap_or(0) as f64;
    assert!(peak < 2.0 * path + 10_000.0, "peak {peak}");
    // And it does halve: some sample is below half of the peak after 10 s.
    let low = bulk
        .trace
        .iter()
        .filter(|e| e.0 >= from)
        .map(|e| e.2)
        .min()
        .unwrap_or(0) as f64;
    let high = bulk
        .trace
        .iter()
        .filter(|e| e.0 >= from)
        .map(|e| e.2)
        .max()
        .unwrap_or(0) as f64;
    assert!(low <= 0.6 * high, "low {low}, high {high}");
    assert!(bulk.longest_stall(start + secs(1)) < secs(1));
}

/// Another flow fills the bottleneck's queue (seen here as 200 ms more
/// one-way delay): the window backs off to a few packets while it lasts;
/// the transfer keeps moving; once the history forgets the old base
/// delay (two minutes) the window grows again, and when the queue clears
/// it is back to full speed.
#[test]
fn backs_off_when_delay_rises_and_never_stalls() {
    let rate = MB;
    let link = bottleneck(rate, ms(20), 400_000);
    let mut sim = established(link, 5, Config::default());
    sim.keep_log = false;
    let start = sim.elapsed();
    let mut bulk = Bulk::new();
    bulk.run(&mut sim, secs(20));
    let before = bulk.window_at(start + secs(20));
    let before_rate = bulk.rate(start + secs(10), start + secs(20));
    assert!(before_rate >= 0.8 * rate as f64);

    sim.link.delay = ms(220);
    let rise = sim.elapsed();
    bulk.run(&mut sim, secs(20));
    let backed_off = bulk.window_at(rise + secs(20));
    assert!(
        backed_off * 4 <= before,
        "window {before} → {backed_off} after the delay rose"
    );
    // Still moving every second.
    assert!(bulk.longest_stall(rise) < secs(1));
    assert!(bulk.delivered_at(rise + secs(20)) > bulk.delivered_at(rise + secs(10)));

    // The higher delay persists: after two minutes it is the new base
    // and the window grows back.
    bulk.run(&mut sim, secs(160));
    let regrown = bulk.window_at(rise + secs(180));
    assert!(regrown > 4 * backed_off, "{backed_off} → {regrown}");
    // The other flow is gone: the delay drops, and LEDBAT is back at
    // capacity.
    sim.link.delay = ms(20);
    let clear = sim.elapsed();
    bulk.run(&mut sim, secs(30));
    assert!(bulk.rate(clear + secs(10), clear + secs(30)) >= 0.8 * rate as f64);
    assert!(bulk.longest_stall(start + secs(1)) < secs(1));
    let cc = sim.eps[A].conns[0].congestion();
    assert!(cc.window() >= Config::default().max_payload);
}

/// Heavy random loss and jitter on top of a bottleneck: the window is
/// knocked down again and again, never below one packet, and the stream
/// arrives intact.
#[test]
fn heavy_loss_never_stalls() {
    let cfg = Config::default();
    let link = LinkCfg {
        loss: 0.25,
        dup: 0.02,
        delay: ms(30),
        jitter: Duration::ZERO,
        rate: MB,
        queue: 100_000,
    };
    let mut sim = established(link, 6, cfg.clone());
    sim.keep_log = false;
    let mut up = Stream::new(2 * 1024 * 1024, 7);
    let mut min_window = usize::MAX;
    assert!(sim.run(secs(3600), |s| {
        up.pump_write(&mut s.eps[A].conns[0], true);
        up.pump_read(&mut s.eps[B].conns[0]);
        min_window = min_window.min(s.eps[A].conns[0].congestion().window());
        up.complete()
    }));
    up.assert_identical();
    assert_eq!(min_window, cfg.max_payload);
    let st = sim.eps[A].conns[0].congestion().stats();
    assert!(st.loss_decreases > 10, "{st:?}");
}
