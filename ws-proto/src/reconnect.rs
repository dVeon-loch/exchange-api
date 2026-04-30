//! Automatic reconnection with exponential backoff.

use crate::config::ReconnectConfig;
use crate::error::Error;
use std::future::Future;
use std::time::Duration;
use tracing::info;

/// State tracker for reconnection attempts.
#[derive(Clone, Debug)]
pub struct Reconnect {
    config: ReconnectConfig,
    attempt: u32,
}

impl Reconnect {
    pub fn new(config: ReconnectConfig) -> Self {
        Self { config, attempt: 0 }
    }

    /// Returns the delay before the next reconnect attempt.
    ///
    /// Returns `None` if the maximum number of retries has been reached.
    pub fn next_delay(&mut self) -> Option<Duration> {
        if self.attempt >= self.config.max_retries {
            return None;
        }

        let delay = self.config.delay(self.attempt);
        self.attempt += 1;
        Some(delay)
    }

    /// Reset the attempt counter (e.g. after a successful connection).
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// The current attempt number (0-based).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Whether the maximum number of retries has been reached.
    pub fn is_exhausted(&self) -> bool {
        self.attempt >= self.config.max_retries
    }
}

/// Run a fallible async operation with retry + backoff.
///
/// The closure receives the current attempt number and should return
/// `Ok(())` on success or `Err(Error)` on failure.
pub async fn retry_with_backoff<F, Fut, T>(
    config: ReconnectConfig,
    mut operation: F,
) -> Result<T, Error>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut reconnect = Reconnect::new(config);

    loop {
        match operation(reconnect.attempt()).await {
            Ok(value) => return Ok(value),
            Err(e) => {
                match reconnect.next_delay() {
                    Some(delay) => {
                        info!(
                            attempt = reconnect.attempt(),
                            delay_ms = delay.as_millis(),
                            "connection failed, retrying: {e}"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    None => return Err(e),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_increases() {
        let config = ReconnectConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            jitter: false,
        };

        let mut r = Reconnect::new(config);
        let d1 = r.next_delay().unwrap();
        let d2 = r.next_delay().unwrap();
        let d3 = r.next_delay().unwrap();

        assert!(d2 > d1);
        assert!(d3 > d2);
    }

    #[test]
    fn exhausts_retries() {
        let config = ReconnectConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(1),
            jitter: false,
        };

        let mut r = Reconnect::new(config);
        assert!(r.next_delay().is_some());
        assert!(r.next_delay().is_some());
        assert!(r.next_delay().is_some());
        assert!(r.next_delay().is_none());
    }
}
