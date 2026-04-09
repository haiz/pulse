use std::time::Duration;

use crate::config::BackoffConfig;

/// Exponential backoff retry scheduler.
pub struct RetryScheduler {
    pub config: BackoffConfig,
}

impl RetryScheduler {
    pub fn new(config: BackoffConfig) -> Self {
        Self { config }
    }

    /// Calculate the delay before the next retry attempt.
    pub fn next_delay(&self, attempt: u32) -> Duration {
        let delay_secs = (self.config.initial_secs as f64)
            * self
                .config
                .multiplier
                .powi(attempt.saturating_sub(1) as i32);
        let capped = delay_secs.min(self.config.max_secs as f64);
        Duration::from_secs_f64(capped)
    }

    /// Check if the event should be moved to the DLQ.
    pub fn should_dlq(&self, attempt: u32, max_redeliveries: u32) -> bool {
        attempt >= max_redeliveries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_scheduler() -> RetryScheduler {
        RetryScheduler::new(BackoffConfig {
            initial_secs: 1,
            max_secs: 60,
            multiplier: 2.0,
        })
    }

    #[test]
    fn first_retry_uses_initial() {
        let s = default_scheduler();
        assert_eq!(s.next_delay(1), Duration::from_secs(1));
    }

    #[test]
    fn exponential_backoff() {
        let s = default_scheduler();
        assert_eq!(s.next_delay(1), Duration::from_secs(1));
        assert_eq!(s.next_delay(2), Duration::from_secs(2));
        assert_eq!(s.next_delay(3), Duration::from_secs(4));
        assert_eq!(s.next_delay(4), Duration::from_secs(8));
    }

    #[test]
    fn capped_at_max() {
        let s = default_scheduler();
        assert_eq!(s.next_delay(10), Duration::from_secs(60));
        assert_eq!(s.next_delay(20), Duration::from_secs(60));
    }

    #[test]
    fn should_dlq_at_max_retries() {
        let s = default_scheduler();
        assert!(!s.should_dlq(1, 5));
        assert!(!s.should_dlq(4, 5));
        assert!(s.should_dlq(5, 5));
        assert!(s.should_dlq(6, 5));
    }
}
