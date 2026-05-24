use std::time::Duration;

/// Configuration for a WebSocket connection.
#[derive(Clone, Debug)]
pub struct WsConfig {
    /// WebSocket endpoint URL (e.g. `wss://stream.binance.com:9443/ws`).
    pub url: String,

    /// Optional HTTP headers sent with the upgrade request.
    pub headers: Vec<(String, String)>,

    /// How often to send pings to keep the connection alive.
    ///
    /// `None` (the default) disables automatic pings entirely.
    /// Set via [`WsConfig::with_ping_interval`].
    pub ping_interval: Option<Duration>,

    /// Maximum time to wait for a pong response before considering
    /// the connection dead.
    ///
    /// Default: 10 seconds.
    pub pong_timeout: Duration,

    /// Reconnection policy when the connection drops.
    pub reconnect: ReconnectConfig,
}

impl WsConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
            ping_interval: None,
            pong_timeout: Duration::from_secs(10),
            reconnect: ReconnectConfig::default(),
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    pub fn with_ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = Some(interval);
        self
    }

    pub fn with_reconnect(mut self, config: ReconnectConfig) -> Self {
        self.reconnect = config;
        self
    }
}

/// Reconnection policy with exponential backoff.
#[derive(Clone, Debug)]
pub struct ReconnectConfig {
    /// Maximum number of reconnection attempts. 0 = no reconnect.
    pub max_retries: u32,

    /// Initial delay before the first reconnect.
    pub initial_delay: Duration,

    /// Maximum delay between reconnect attempts.
    pub max_delay: Duration,

    /// If true, adds random jitter to each delay to avoid thundering herds.
    pub jitter: bool,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            jitter: true,
        }
    }
}

impl ReconnectConfig {
    /// Calculate the delay for the nth retry (0-indexed).
    pub fn delay(&self, retry: u32) -> Duration {
        let exp = 2u64.saturating_pow(retry);
        let base = self.initial_delay.as_millis() as u64 * exp;
        let clamped = base.min(self.max_delay.as_millis() as u64);

        if self.jitter {
            let jitter = fastrand::u64(0..=clamped / 4);
            Duration::from_millis(clamped + jitter)
        } else {
            Duration::from_millis(clamped)
        }
    }
}
