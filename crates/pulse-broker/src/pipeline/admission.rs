use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Resource-aware admission control.
///
/// Tracks WAL write latency and rejects publishes when latency exceeds threshold.
pub struct AdmissionController {
    wal_latency_ema_us: AtomicU64,
    wal_latency_threshold_us: u64,
    paused: AtomicBool,
}

impl AdmissionController {
    pub fn new(wal_latency_threshold_us: u64) -> Self {
        Self {
            wal_latency_ema_us: AtomicU64::new(0),
            wal_latency_threshold_us,
            paused: AtomicBool::new(false),
        }
    }

    pub fn should_accept(&self) -> bool {
        if self.paused.load(Ordering::Relaxed) {
            return false;
        }
        self.wal_latency_ema_us.load(Ordering::Relaxed) < self.wal_latency_threshold_us
    }

    /// Record a WAL write latency sample. Uses EMA with alpha=0.1.
    pub fn record_wal_latency(&self, latency: std::time::Duration) {
        let sample_us = latency.as_micros() as u64;
        let current = self.wal_latency_ema_us.load(Ordering::Relaxed);
        let new_ema = if current == 0 {
            sample_us
        } else {
            (current * 9 + sample_us) / 10
        };
        self.wal_latency_ema_us.store(new_ema, Ordering::Relaxed);
    }

    pub fn wal_latency_us(&self) -> u64 {
        self.wal_latency_ema_us.load(Ordering::Relaxed)
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn accepts_when_below_threshold() {
        let ac = AdmissionController::new(10_000); // 10ms threshold
        ac.record_wal_latency(Duration::from_micros(500));
        assert!(ac.should_accept());
    }

    #[test]
    fn rejects_when_above_threshold() {
        let ac = AdmissionController::new(1_000); // 1ms threshold
        for _ in 0..50 {
            ac.record_wal_latency(Duration::from_micros(5_000)); // 5ms samples
        }
        assert!(!ac.should_accept());
    }

    #[test]
    fn ema_converges() {
        let ac = AdmissionController::new(100_000);
        // First sample sets EMA directly
        ac.record_wal_latency(Duration::from_micros(1_000));
        assert_eq!(ac.wal_latency_us(), 1_000);

        // Second sample: (1000*9 + 2000)/10 = 1100
        ac.record_wal_latency(Duration::from_micros(2_000));
        assert_eq!(ac.wal_latency_us(), 1_100);
    }

    #[test]
    fn pause_rejects_all() {
        let ac = AdmissionController::new(100_000);
        assert!(ac.should_accept());

        ac.pause();
        assert!(!ac.should_accept());

        ac.resume();
        assert!(ac.should_accept());
    }
}
