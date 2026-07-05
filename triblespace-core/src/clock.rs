//! Virtualizable time — the clock seam for deterministic simulation.
//!
//! Every time read that can influence persisted facts or protocol
//! behavior goes through this module
//! instead of `std::time::Instant::now()` / `hifitime::Epoch::now()`.
//! In production the default real source is a thin shim over
//! those. Under simulation, [`install_virtual`](crate::clock::install_virtual) swaps in a
//! [`VirtualClock`](crate::clock::VirtualClock) that only moves when the simulator's
//! discrete-event scheduler advances it — so cooldown expiry, renewal
//! windows, and rebroadcast ticks become deterministic functions of
//! the event schedule rather than of wall time.
//!
//! Two kinds of time, deliberately distinct:
//!
//! - [`mono_now`](crate::clock::mono_now) → [`Mono`](crate::clock::Mono): monotonic nanoseconds since an arbitrary
//!   per-process origin. Replaces `std::time::Instant` for durations
//!   and timeouts (redispatch cooldowns, quiescence tracking, the
//!   gossip rebroadcast period). `Mono` is plain data (`u64` ns) so it
//!   can cross thread and serialization boundaries freely, which
//!   `Instant` cannot.
//! - [`epoch_now`](crate::clock::epoch_now) → `hifitime::Epoch`: wall-clock TAI time. Used
//!   where the *absolute* date matters and ends up in persisted facts:
//!   cap expiry checks, renewal-policy timestamps, retraction marks.
//!
//! A discrete-event simulation has exactly one global timeline, so the
//! source is process-global rather than per-node. Per-node clock skew
//! (pre-mortem #47) is modeled *above* this seam — a skewed node adds
//! its offset at the call site — keeping the substrate simple.
//!
//! The source is a `OnceLock`: it can be installed at most once, before
//! first use, and stays for the process lifetime. Simulation tests live
//! in their own integration-test binaries (one process each), so a
//! global install doesn't leak across tests. `tokio::time::sleep` is
//! NOT routed through here — simulation runtimes use
//! `tokio::runtime::Builder::new_current_thread().start_paused(true)`,
//! whose auto-advance handles sleeps; this module covers the
//! *measurements* tokio can't see.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

/// A monotonic instant: nanoseconds since the process clock origin.
///
/// Plain data replacement for `std::time::Instant`. Ordering and
/// arithmetic are exactly u64-ns ordering; the origin is arbitrary
/// (process start for the real clock, simulation start for a virtual
/// one) so only differences are meaningful.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mono(u64);

impl Mono {
    /// Nanoseconds since the clock origin. Exposed for logging and
    /// for simulators that want to inspect the raw timeline.
    pub fn as_nanos(self) -> u64 {
        self.0
    }

    /// Duration from `earlier` to `self`, saturating to zero if
    /// `earlier` is actually later (mirrors
    /// `Instant::saturating_duration_since`).
    pub fn duration_since(self, earlier: Mono) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    /// Duration from `self` to the current clock reading.
    pub fn elapsed(self) -> Duration {
        mono_now().duration_since(self)
    }
}

impl std::ops::Add<Duration> for Mono {
    type Output = Mono;
    /// Saturating add — a deadline past u64::MAX nanoseconds (584
    /// years of uptime) clamps rather than wraps.
    fn add(self, rhs: Duration) -> Mono {
        Mono(self.0.saturating_add(rhs.as_nanos() as u64))
    }
}

/// A virtual clock for simulation: time is a counter the scheduler
/// advances, plus a fixed wall-clock base so `epoch_now` stays
/// meaningful for expiry math.
pub struct VirtualClock {
    /// Virtual nanoseconds since simulation start.
    now_ns: AtomicU64,
    /// Wall-clock (TAI) instant corresponding to virtual zero.
    epoch_base: hifitime::Epoch,
}

impl VirtualClock {
    /// A virtual clock starting at `epoch_base` (the simulated wall
    /// time at virtual zero).
    pub fn new(epoch_base: hifitime::Epoch) -> Arc<Self> {
        Arc::new(Self {
            now_ns: AtomicU64::new(0),
            epoch_base,
        })
    }

    /// Advance virtual time by `d`. Called only by the simulation
    /// scheduler, between event deliveries.
    pub fn advance(&self, d: Duration) {
        self.now_ns.fetch_add(d.as_nanos() as u64, Ordering::SeqCst);
    }

    /// Current virtual nanoseconds.
    pub fn now_ns(&self) -> u64 {
        self.now_ns.load(Ordering::SeqCst)
    }

    /// Rewind virtual time to zero. ONLY sound between independent
    /// simulation runs in one process (each run constructs its whole
    /// world fresh, so no live state carries `Mono` values across the
    /// reset). Lets a test binary execute the same seeded scenario
    /// twice and get bit-identical wall-clock-dependent artifacts
    /// (commit timestamps, cap expiries) — the determinism-replay
    /// contract.
    pub fn reset(&self) {
        self.now_ns.store(0, Ordering::SeqCst);
    }
}

enum Source {
    Real { origin: std::time::Instant },
    Virtual(Arc<VirtualClock>),
}

static SOURCE: OnceLock<Source> = OnceLock::new();

fn source() -> &'static Source {
    SOURCE.get_or_init(|| Source::Real {
        origin: std::time::Instant::now(),
    })
}

/// Install a virtual clock as the process-wide time source.
///
/// Must run before the first time read anywhere in the process —
/// returns `Err(())` if a source (real or virtual) is already
/// installed. Simulation harnesses call this first thing in `main`/
/// the test body.
pub fn install_virtual(clock: Arc<VirtualClock>) -> Result<(), ()> {
    SOURCE.set(Source::Virtual(clock)).map_err(|_| ())
}

/// Current monotonic instant.
pub fn mono_now() -> Mono {
    match source() {
        Source::Real { origin } => Mono(origin.elapsed().as_nanos() as u64),
        Source::Virtual(vc) => Mono(vc.now_ns()),
    }
}

/// Current wall-clock instant (TAI).
///
/// Real source: `hifitime::Epoch::now()` — panics only if the system
/// clock is unreadable, which is unrecoverable misconfiguration.
/// Virtual source: `epoch_base + virtual elapsed`.
pub fn epoch_now() -> hifitime::Epoch {
    match source() {
        Source::Real { .. } => hifitime::Epoch::now().expect("system wall clock unreadable"),
        Source::Virtual(vc) => {
            vc.epoch_base + hifitime::Duration::from_total_nanoseconds(vc.now_ns() as i128)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: install_virtual is process-global, and unit tests share a
    // process — so these tests only exercise the real source and the
    // VirtualClock struct in isolation. End-to-end virtual-time
    // behavior is covered by the simulation integration tests (one
    // process each).

    #[test]
    fn mono_is_monotonic() {
        let a = mono_now();
        let b = mono_now();
        assert!(b >= a);
        assert_eq!(b.duration_since(a), b.duration_since(a));
    }

    #[test]
    fn duration_since_saturates() {
        let a = Mono(100);
        let b = Mono(50);
        assert_eq!(b.duration_since(a), Duration::ZERO);
        assert_eq!(a.duration_since(b), Duration::from_nanos(50));
    }

    #[test]
    fn virtual_clock_advances() {
        let vc = VirtualClock::new(hifitime::Epoch::from_tai_seconds(0.0));
        assert_eq!(vc.now_ns(), 0);
        vc.advance(Duration::from_millis(5));
        assert_eq!(vc.now_ns(), 5_000_000);
    }
}
