//! Hybrid logical clock: 48 bits of wall-clock milliseconds, 16 bits of
//! logical counter. Total order, roughly tracks real time, never goes
//! backwards on a node even if the wall clock does.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const COUNTER_BITS: u32 = 16;
const COUNTER_MASK: u64 = (1 << COUNTER_BITS) - 1;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hlc(pub u64);

impl Hlc {
    pub const ZERO: Hlc = Hlc(0);

    pub fn from_parts(wall_ms: u64, counter: u16) -> Hlc {
        Hlc((wall_ms << COUNTER_BITS) | counter as u64)
    }
    pub fn wall_ms(self) -> u64 {
        self.0 >> COUNTER_BITS
    }
    pub fn counter(self) -> u16 {
        (self.0 & COUNTER_MASK) as u16
    }
}

impl fmt::Debug for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hlc({}:{})", self.wall_ms(), self.counter())
    }
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.wall_ms(), self.counter())
    }
}

/// A clock that issues strictly increasing timestamps and can observe
/// remote timestamps so that causally later local events sort later.
pub struct Clock {
    last: Mutex<Hlc>,
    wall: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new()
    }
}

impl Clock {
    pub fn new() -> Clock {
        Clock::with_wall(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        })
    }

    pub fn with_wall(wall: impl Fn() -> u64 + Send + Sync + 'static) -> Clock {
        Clock { last: Mutex::new(Hlc::ZERO), wall: Box::new(wall) }
    }

    /// Issue a new timestamp greater than every timestamp issued or
    /// observed so far.
    pub fn now(&self) -> Hlc {
        let mut last = self.last.lock().unwrap();
        let wall = (self.wall)();
        let next = if wall > last.wall_ms() {
            Hlc::from_parts(wall, 0)
        } else {
            Hlc(last.0 + 1)
        };
        *last = next;
        next
    }

    /// Record that a timestamp from elsewhere has been seen.
    pub fn observe(&self, remote: Hlc) {
        let mut last = self.last.lock().unwrap();
        if remote > *last {
            *last = remote;
        }
    }

    pub fn last(&self) -> Hlc {
        *self.last.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[test]
    fn parts_round_trip() {
        let h = Hlc::from_parts(1_700_000_000_000, 7);
        assert_eq!(h.wall_ms(), 1_700_000_000_000);
        assert_eq!(h.counter(), 7);
        assert_eq!(h.to_string(), "1700000000000:7");
    }

    #[test]
    fn strictly_increasing_when_wall_is_frozen() {
        let c = Clock::with_wall(|| 1000);
        let a = c.now();
        let b = c.now();
        let d = c.now();
        assert!(a < b && b < d);
        assert_eq!(a.wall_ms(), 1000);
        assert_eq!(d.counter(), 2);
    }

    #[test]
    fn never_goes_backwards_when_wall_does() {
        let wall = Arc::new(AtomicU64::new(5000));
        let w = wall.clone();
        let c = Clock::with_wall(move || w.load(Ordering::SeqCst));
        let a = c.now();
        wall.store(4000, Ordering::SeqCst);
        let b = c.now();
        assert!(b > a);
        assert_eq!(b.wall_ms(), 5000);
    }

    #[test]
    fn observe_pushes_local_clock_forward() {
        let c = Clock::with_wall(|| 1000);
        let remote = Hlc::from_parts(9000, 3);
        c.observe(remote);
        let next = c.now();
        assert!(next > remote);
        assert_eq!(next.wall_ms(), 9000);
        assert_eq!(next.counter(), 4);
    }

    #[test]
    fn observe_ignores_older() {
        let c = Clock::with_wall(|| 1000);
        let a = c.now();
        c.observe(Hlc::from_parts(1, 1));
        assert_eq!(c.last(), a);
    }
}
