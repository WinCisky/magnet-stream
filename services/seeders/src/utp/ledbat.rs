//! LEDBAT congestion control for uTP (BEP 29), sans-IO (M4-04c).
//!
//! [`Ledbat`] owns the congestion window (`max_window` in BEP 29): the
//! bytes a connection may have in flight. The connection
//! (`super::conn`) feeds it
//!
//! - [`Ledbat::on_delay_sample`]: the remote's
//!   `timestamp_difference_microseconds` of every packet accepted, i.e. the
//!   one-way delay of our packets towards it, plus the unknown offset
//!   between the two clocks;
//! - [`Ledbat::on_ack`]: the payload bytes newly acknowledged (cumulative or
//!   selective) and the bytes that were in flight before;
//! - [`Ledbat::on_loss`]: a packet marked lost by duplicate or selective
//!   ACKs; [`Ledbat::on_timeout`]: a retransmission timeout.
//!
//! The algorithm is BEP 29's:
//!
//! - `base_delay` is the lowest sample of the last two minutes (one minimum
//!   per minute, [`BASE_HISTORY`] of them), which removes the clock offset;
//!   `our_delay` is the lowest of the last [`CURRENT_FILTER`] samples minus
//!   `base_delay`: the queueing delay our own packets see.
//! - On every ACK, `off_target = target - our_delay` (target 100 ms,
//!   `CCONTROL_TARGET`) and the window moves by
//!   `gain · (off_target / target) · (bytes_acked / window)`: at most
//!   `gain` bytes per round trip upwards ([`DEFAULT_GAIN`], 3 000 B, as
//!   libutp), downwards in proportion to how far the delay overshoots.
//! - A loss halves the window, at most once per window of data (a loss of
//!   a packet sent before the last decrease is the same congestion event);
//!   a retransmission timeout drops it to the minimum.
//! - The window never goes below one packet ([`Params::min_window`]), and
//!   the connection always lets one packet out when nothing is in flight:
//!   it can be slowed down, never stalled.
//!
//! Beyond the text of BEP 29, as libutp and libtorrent do: the window
//! grows only while the sender actually fills it (an application-limited
//! sender does not inflate it), and a slow start (the window grows by the
//! bytes acknowledged, doubling per round trip) runs from the start and
//! after a timeout, until the delay reaches 90 % of the target, a loss, or
//! the threshold left by the last decrease. Without it a timeout would cost
//! a whole window of round trips at 3 000 B each.
//!
//! Timestamps are 32-bit and wrap (about every 71.6 minutes); samples are
//! compared modulo 2^32, so the wrap is harmless. A sample of 0 means "no
//! measurement" (the remote's SYN, or a remote that never measures) and is
//! ignored; without any sample the delay counts as 0, so the window is
//! then governed by losses alone. The packet size is fixed
//! (`conn::MAX_PAYLOAD`, 1 400 B or less): no PMTU probing.

use std::time::{Duration, Instant};

/// Target queueing delay (BEP 29 `CCONTROL_TARGET`).
pub const DEFAULT_TARGET: Duration = Duration::from_millis(100);
/// Largest window increase per round trip, in bytes (BEP 29
/// `MAX_CWND_INCREASE_PACKETS_PER_RTT`; libutp's value).
pub const DEFAULT_GAIN: usize = 3000;
/// Width of one bucket of the base-delay history.
pub const BASE_BUCKET: Duration = Duration::from_secs(60);
/// Buckets of the base-delay history: the base delay is the minimum of the
/// last two minutes (BEP 29).
pub const BASE_HISTORY: usize = 2;
/// Samples of the current-delay filter (their minimum is `our_delay`).
pub const CURRENT_FILTER: usize = 3;
/// Initial window, in packets.
const INITIAL_WINDOW_PACKETS: usize = 2;
/// Slow start ends once the delay reaches this fraction of the target.
const SLOW_START_EXIT: f64 = 0.9;

/// Parameters of one controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Params {
    /// Target queueing delay.
    pub target: Duration,
    /// Largest increase per round trip, in bytes.
    pub gain: usize,
    /// Smallest window: one packet.
    pub min_window: usize,
    /// Largest window.
    pub max_window: usize,
}

impl Params {
    fn sanitized(mut self) -> Self {
        self.target = self.target.max(Duration::from_millis(1));
        self.min_window = self.min_window.max(1);
        self.max_window = self.max_window.max(self.min_window);
        self
    }
}

/// Counters of one controller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CcStats {
    /// Window halvings on loss.
    pub loss_decreases: u64,
    /// Window drops to the minimum on a retransmission timeout.
    pub timeout_decreases: u64,
    /// Delay samples taken.
    pub delay_samples: u64,
}

/// `a` is earlier than `b` on the 32-bit timestamp ring.
fn wrapping_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

fn wrapping_min(a: u32, b: u32) -> u32 {
    if wrapping_lt(b, a) {
        b
    } else {
        a
    }
}

/// LEDBAT state of one connection. See the module documentation.
#[derive(Debug, Clone)]
pub struct Ledbat {
    params: Params,
    /// The window in bytes (fractional: gains of less than a byte per ACK
    /// add up).
    cwnd: f64,
    slow_start: bool,
    ssthresh: f64,
    /// Per-minute minima, oldest first; `base[..base_len]` is valid.
    base: [u32; BASE_HISTORY],
    base_len: usize,
    bucket_start: Option<Instant>,
    /// The latest samples, a ring; `current[..current_len]` is valid.
    current: [u32; CURRENT_FILTER],
    current_len: usize,
    current_next: usize,
    last_decrease: Option<Instant>,
    stats: CcStats,
}

impl Ledbat {
    pub fn new(params: Params) -> Self {
        let params = params.sanitized();
        let initial = (params.min_window * INITIAL_WINDOW_PACKETS).min(params.max_window);
        Self {
            params,
            cwnd: initial as f64,
            slow_start: true,
            ssthresh: params.max_window as f64,
            base: [0; BASE_HISTORY],
            base_len: 0,
            bucket_start: None,
            current: [0; CURRENT_FILTER],
            current_len: 0,
            current_next: 0,
            last_decrease: None,
            stats: CcStats::default(),
        }
    }

    pub fn params(&self) -> Params {
        self.params
    }

    /// The congestion window in bytes: at least one packet.
    pub fn window(&self) -> usize {
        self.cwnd as usize
    }

    pub fn in_slow_start(&self) -> bool {
        self.slow_start
    }

    pub fn stats(&self) -> CcStats {
        self.stats
    }

    /// Lowest sample of the base-delay history.
    pub fn base_delay(&self) -> Option<u32> {
        self.base[..self.base_len]
            .iter()
            .copied()
            .reduce(wrapping_min)
    }

    /// The current queueing delay estimate (`our_delay`), if any sample was
    /// taken.
    pub fn our_delay(&self) -> Option<Duration> {
        let base = self.base_delay()?;
        let current = self.current[..self.current_len]
            .iter()
            .copied()
            .reduce(wrapping_min)?;
        let d = current.wrapping_sub(base);
        // Below the base (cannot happen while the history holds every
        // recent sample, but the ring wraps): no queueing.
        let d = if (d as i32) < 0 { 0 } else { d };
        Some(Duration::from_micros(u64::from(d)))
    }

    /// One `timestamp_difference_microseconds` from the remote.
    pub fn on_delay_sample(&mut self, now: Instant, sample: u32) {
        if sample == 0 {
            return;
        }
        self.stats.delay_samples += 1;
        match self.bucket_start {
            None => {
                self.base[0] = sample;
                self.base_len = 1;
                self.bucket_start = Some(now);
            }
            Some(start) => {
                let elapsed = now.saturating_duration_since(start);
                let buckets = elapsed.as_secs() / BASE_BUCKET.as_secs().max(1);
                if buckets == 0 {
                    let last = self.base_len - 1;
                    self.base[last] = wrapping_min(self.base[last], sample);
                } else if buckets >= BASE_HISTORY as u64 {
                    // Silent for the whole history: start over, the old
                    // current samples are stale too.
                    self.base[0] = sample;
                    self.base_len = 1;
                    self.current_len = 0;
                    self.current_next = 0;
                    self.bucket_start = Some(now);
                } else {
                    for _ in 0..buckets {
                        if self.base_len == BASE_HISTORY {
                            self.base.copy_within(1.., 0);
                            self.base_len -= 1;
                        }
                        self.base[self.base_len] = sample;
                        self.base_len += 1;
                    }
                    // `buckets < BASE_HISTORY`, so the cast is lossless.
                    self.bucket_start = Some(start + BASE_BUCKET * buckets as u32);
                }
            }
        }
        self.current[self.current_next] = sample;
        self.current_next = (self.current_next + 1) % CURRENT_FILTER;
        self.current_len = (self.current_len + 1).min(CURRENT_FILTER);
    }

    /// `acked` payload bytes newly acknowledged; `flight_before` bytes were
    /// in flight before this ACK.
    pub fn on_ack(&mut self, acked: usize, flight_before: usize) {
        if acked == 0 {
            return;
        }
        let min = self.params.min_window as f64;
        let max = self.params.max_window as f64;
        // The sender filled the window (to within one packet): only then
        // may it grow.
        let window_limited = (flight_before + self.params.min_window) as f64 >= self.cwnd;
        let target = self.params.target.as_micros() as f64;
        let delay = self.our_delay().map_or(0.0, |d| d.as_micros() as f64);

        let delay_factor = ((target - delay) / target).min(1.0);
        let window_factor = (acked as f64 / self.cwnd).min(1.0);
        let gain = self.params.gain as f64 * delay_factor * window_factor;
        let mut next = if gain > 0.0 && !window_limited {
            self.cwnd
        } else {
            self.cwnd + gain
        };

        if self.slow_start && window_limited {
            if delay >= target * SLOW_START_EXIT {
                self.slow_start = false;
                self.ssthresh = self.cwnd;
            } else {
                let doubled = self.cwnd + acked as f64;
                if doubled >= self.ssthresh {
                    self.slow_start = false;
                }
                next = next.max(doubled.min(self.ssthresh));
            }
        }
        self.cwnd = next.clamp(min, max);
    }

    /// A packet sent at `sent_at` was lost (duplicate or selective ACKs):
    /// the window halves, once per window of data. Returns whether it did.
    pub fn on_loss(&mut self, now: Instant, sent_at: Instant) -> bool {
        if self.last_decrease.is_some_and(|t| sent_at < t) {
            return false;
        }
        let min = self.params.min_window as f64;
        self.cwnd = (self.cwnd / 2.0).max(min);
        self.ssthresh = self.cwnd;
        self.slow_start = false;
        self.last_decrease = Some(now);
        self.stats.loss_decreases += 1;
        true
    }

    /// A retransmission timeout: the window drops to one packet and slow
    /// start runs again up to half the previous window.
    pub fn on_timeout(&mut self, now: Instant) {
        let min = self.params.min_window as f64;
        self.ssthresh = (self.cwnd / 2.0).max(min);
        self.cwnd = min;
        self.slow_start = true;
        self.last_decrease = Some(now);
        self.stats.timeout_decreases += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: usize = 1400;

    fn params() -> Params {
        Params {
            target: DEFAULT_TARGET,
            gain: DEFAULT_GAIN,
            min_window: MSS,
            max_window: 1 << 20,
        }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Leaves slow start with a window of `w` bytes, the base delay set
    /// from `base` and `our_delay` = 0.
    fn steady(t0: Instant, base: u32, w: usize) -> Ledbat {
        let mut cc = Ledbat::new(params());
        cc.on_delay_sample(t0, base);
        cc.cwnd = w as f64;
        cc.slow_start = false;
        cc
    }

    /// Acknowledges one full window in `MSS` steps; the window is full.
    fn ack_window(cc: &mut Ledbat) {
        let w = cc.window();
        let mut acked = 0;
        while acked < w {
            let n = (w - acked).min(MSS);
            cc.on_ack(n, cc.window());
            acked += n;
        }
    }

    #[test]
    fn grows_by_at_most_gain_per_round_trip_below_target() {
        let t0 = Instant::now();
        let mut cc = steady(t0, 50_000, 30_000);
        let before = cc.window();
        ack_window(&mut cc);
        let grown = cc.window() - before;
        assert!(grown > 2_500 && grown <= DEFAULT_GAIN, "{grown}");

        // Half the target: about half the gain.
        let mut cc = steady(t0, 50_000, 30_000);
        cc.on_delay_sample(t0, 50_000 + 50_000);
        cc.current = [100_000; CURRENT_FILTER];
        assert_eq!(cc.our_delay(), Some(ms(50)));
        ack_window(&mut cc);
        let grown = cc.window() - 30_000;
        assert!((1_300..=1_600).contains(&grown), "{grown}");
    }

    #[test]
    fn shrinks_above_target_in_proportion() {
        let t0 = Instant::now();
        for (delay_ms, min_loss) in [(150u32, 1_200usize), (300, 5_000), (1_000, 25_000)] {
            let mut cc = steady(t0, 10_000, 60_000);
            let s = 10_000 + delay_ms * 1000;
            for _ in 0..CURRENT_FILTER {
                cc.on_delay_sample(t0, s);
            }
            ack_window(&mut cc);
            assert!(
                cc.window() <= 60_000 - min_loss,
                "{delay_ms}: {}",
                cc.window()
            );
        }
        // Exactly on target: no change.
        let mut cc = steady(t0, 10_000, 60_000);
        for _ in 0..CURRENT_FILTER {
            cc.on_delay_sample(t0, 110_000);
        }
        ack_window(&mut cc);
        assert_eq!(cc.window(), 60_000);
    }

    #[test]
    fn never_below_one_packet() {
        let t0 = Instant::now();
        let mut cc = steady(t0, 1, 20_000);
        for _ in 0..CURRENT_FILTER {
            cc.on_delay_sample(t0, u32::MAX / 4);
        }
        for _ in 0..1000 {
            cc.on_ack(MSS, cc.window());
            assert!(cc.window() >= MSS);
        }
        assert_eq!(cc.window(), MSS);
        for _ in 0..10 {
            cc.on_loss(t0 + ms(1), t0 + ms(1));
            cc.on_timeout(t0 + ms(1));
            assert_eq!(cc.window(), MSS);
        }
        // Once the delay is gone it grows again.
        let mut cc2 = steady(t0, 1, MSS);
        cc2.on_ack(MSS, MSS);
        assert!(cc2.window() > MSS);
    }

    #[test]
    fn application_limited_sender_does_not_grow() {
        let t0 = Instant::now();
        let mut cc = steady(t0, 5, 50_000);
        for _ in 0..1000 {
            cc.on_ack(MSS, 10_000);
        }
        assert_eq!(cc.window(), 50_000);
        // Slow start too.
        let mut cc = Ledbat::new(params());
        let w = cc.window();
        for _ in 0..100 {
            cc.on_ack(MSS, 0);
        }
        assert_eq!(cc.window(), w);
        // But it still shrinks when the delay says so.
        let mut cc = steady(t0, 5, 50_000);
        for _ in 0..CURRENT_FILTER {
            cc.on_delay_sample(t0, 500_005);
        }
        cc.on_ack(MSS, 10_000);
        assert!(cc.window() < 50_000);
    }

    #[test]
    fn slow_start_doubles_then_exits_near_target() {
        let t0 = Instant::now();
        let mut cc = Ledbat::new(params());
        assert!(cc.in_slow_start());
        assert_eq!(cc.window(), 2 * MSS);
        cc.on_delay_sample(t0, 1_000);
        for _ in 0..4 {
            let w = cc.window();
            ack_window(&mut cc);
            assert!(cc.in_slow_start());
            assert!(cc.window() >= 2 * w, "{w} → {}", cc.window());
        }
        // The delay reaches 90 % of the target: slow start ends, LEDBAT
        // takes over (below the target it still grows a little).
        for _ in 0..CURRENT_FILTER {
            cc.on_delay_sample(t0, 1_000 + 95_000);
        }
        let w = cc.window();
        ack_window(&mut cc);
        assert!(!cc.in_slow_start());
        assert!(cc.window() > w && cc.window() < w + 400);
    }

    #[test]
    fn loss_halves_once_per_window() {
        let t0 = Instant::now();
        let mut cc = steady(t0, 5, 64_000);
        // Two packets of the same window lost: one decrease.
        assert!(cc.on_loss(t0 + ms(100), t0 + ms(10)));
        assert_eq!(cc.window(), 32_000);
        assert!(!cc.on_loss(t0 + ms(101), t0 + ms(20)));
        assert_eq!(cc.window(), 32_000);
        // A packet sent after the decrease: a new event.
        assert!(cc.on_loss(t0 + ms(300), t0 + ms(150)));
        assert_eq!(cc.window(), 16_000);
        assert_eq!(cc.stats().loss_decreases, 2);
        assert!(!cc.in_slow_start());
    }

    #[test]
    fn timeout_restarts_slow_start_to_half_the_window() {
        let t0 = Instant::now();
        let mut cc = steady(t0, 5, 40_000);
        cc.on_timeout(t0 + ms(1));
        assert_eq!(cc.window(), MSS);
        assert!(cc.in_slow_start());
        let mut rounds = 0;
        while cc.in_slow_start() {
            ack_window(&mut cc);
            rounds += 1;
            assert!(rounds < 20);
        }
        // 1400 → 20000 by doubling: 4 or 5 round trips, then LEDBAT.
        assert!(rounds <= 5, "{rounds}");
        assert!((20_000..23_500).contains(&cc.window()), "{}", cc.window());
        assert_eq!(cc.stats().timeout_decreases, 1);
    }

    #[test]
    fn base_delay_is_the_minimum_of_the_last_two_minutes() {
        let t0 = Instant::now();
        let mut cc = Ledbat::new(params());
        assert_eq!(cc.our_delay(), None);
        cc.on_delay_sample(t0, 0); // "no sample"
        assert_eq!(cc.base_delay(), None);
        cc.on_delay_sample(t0, 20_000);
        cc.on_delay_sample(t0 + ms(10), 30_000);
        assert_eq!(cc.base_delay(), Some(20_000));
        // Filter: the lowest of the last three samples.
        assert_eq!(cc.our_delay(), Some(Duration::ZERO));
        for s in [70_000, 60_000, 80_000] {
            cc.on_delay_sample(t0 + ms(20), s);
        }
        assert_eq!(cc.our_delay(), Some(ms(40)));
        // A minute later the old minimum is still remembered…
        cc.on_delay_sample(t0 + Duration::from_secs(61), 50_000);
        assert_eq!(cc.base_delay(), Some(20_000));
        // …two minutes later it is gone: the path's delay went up for good.
        cc.on_delay_sample(t0 + Duration::from_secs(121), 50_000);
        assert_eq!(cc.base_delay(), Some(50_000));
        assert_eq!(cc.our_delay(), Some(Duration::ZERO));
        // A long silence clears the history.
        cc.on_delay_sample(t0 + Duration::from_secs(3600), 90_000);
        assert_eq!(cc.base_delay(), Some(90_000));
        assert_eq!(cc.our_delay(), Some(Duration::ZERO));
    }

    #[test]
    fn samples_wrap_around_the_32_bit_clock() {
        let t0 = Instant::now();
        let mut cc = Ledbat::new(params());
        // The clock offset puts samples just below 2^32; queueing pushes
        // them across the wrap.
        let base = u32::MAX - 10_000;
        cc.on_delay_sample(t0, base);
        for _ in 0..CURRENT_FILTER {
            cc.on_delay_sample(t0, base.wrapping_add(60_000));
        }
        assert_eq!(cc.base_delay(), Some(base));
        assert_eq!(cc.our_delay(), Some(ms(60)));
        // A lower sample on the other side of the wrap is the new base.
        cc.on_delay_sample(t0, base.wrapping_sub(5_000));
        assert_eq!(cc.base_delay(), Some(base.wrapping_sub(5_000)));
    }

    #[test]
    fn params_are_sanitized() {
        let cc = Ledbat::new(Params {
            target: Duration::ZERO,
            gain: 0,
            min_window: 0,
            max_window: 0,
        });
        let p = cc.params();
        assert!(p.target > Duration::ZERO);
        assert_eq!((p.min_window, p.max_window), (1, 1));
        assert_eq!(cc.window(), 1);
    }

    /// Arbitrary inputs never panic and keep the window in bounds.
    #[test]
    fn property_window_stays_in_bounds() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(0x001e_dba7);
        let t0 = Instant::now();
        let p = Params {
            max_window: 200_000,
            ..params()
        };
        let mut cc = Ledbat::new(p);
        for _ in 0..200_000u64 {
            let now = t0 + ms(rng.gen_range(0..10_000_000));
            match rng.gen_range(0..10) {
                0..=4 => cc.on_delay_sample(now, rng.gen()),
                5..=7 => cc.on_ack(rng.gen_range(0..300_000), rng.gen_range(0..300_000)),
                8 => {
                    cc.on_loss(now, t0 + ms(rng.gen_range(0..10_000_000)));
                }
                _ => cc.on_timeout(now),
            }
            assert!((p.min_window..=p.max_window).contains(&cc.window()));
            assert!(cc.cwnd.is_finite());
        }
    }
}
