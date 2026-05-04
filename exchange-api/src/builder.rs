use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use crate::output::{self, OutputConfig};
use crate::runtime::ExchangeName;
use crate::traits::Exchange;
use crate::types::{StreamKind, UpdateRate};
use crate::StreamData;

/// Builds an [`ExchangeApi`] instance via a fluent API.
pub struct ExchangeApiBuilder {
    exchanges: HashMap<ExchangeName, Arc<dyn Exchange>>,
    streams: Vec<StreamKind>,
    symbols: Vec<String>,
    output: OutputConfig,
    update_rate: Option<UpdateRate>,
}

impl ExchangeApiBuilder {
    pub fn new() -> Self {
        Self {
            exchanges: HashMap::new(),
            streams: Vec::new(),
            symbols: Vec::new(),
            output: OutputConfig::default(),
            update_rate: None,
        }
    }

    /// Set the trading pair symbol, e.g. "btcusdt".
    pub fn symbols(mut self, symbols: impl Into<Vec<String>>) -> Self {
        self.symbols = symbols.into();
        self
    }

    /// Register an exchange implementation.
    pub fn add_exchange(mut self, exchange: impl Exchange) -> Self {
        self.exchanges.insert(exchange.name(), Arc::new(exchange));
        self
    }

    /// Register a data stream to subscribe to on every exchange.
    pub fn register_task(mut self, stream: StreamKind) -> Self {
        self.streams.push(stream);
        self
    }

    /// Set the target update rate for streamed data.
    /// Exchanges will match this to their supported rates where possible.
    pub fn update_rate(mut self, duration: Duration) -> Self {
        self.update_rate = Some(UpdateRate {
            duration,
        });
        self
    }

    /// Attach a Kafka producer configuration.
    #[cfg(feature = "kafka")]
    pub fn add_kafka_producer(mut self, config: output::kafka::KafkaConfig) -> Self {
        self.output.kafka = Some(config);
        self
    }

    /// Attach a Redis writer configuration.
    #[cfg(feature = "redis")]
    pub fn add_redis(mut self, config: output::redis::RedisConfig) -> Self {
        self.output.redis = Some(config);
        self
    }

    /// Attach a File writer configuration.
    pub fn add_file(mut self, config: output::FileConfig) -> Self {
        self.output.file = Some(config);
        self
    }

    /// Attach a broadcast channel sender as an output.
    pub fn add_broadcast_channel(mut self, sender: broadcast::Sender<StreamData>) -> Self {
        self.output.custom_channel = Some(sender);
        self
    }

    /// Consume the builder and produce a ready-to-run [`ExchangeApi`].
    pub fn build(self) -> Result<crate::runtime::ExchangeApi, crate::Error> {
        if self.exchanges.is_empty() {
            return Err(crate::Error::Config(
                "at least one exchange must be registered".into(),
            ));
        }
        if self.streams.is_empty() {
            return Err(crate::Error::Config(
                "at least one stream must be registered".into(),
            ));
        }
        if self.symbols.is_empty() {
            return Err(crate::Error::Config("symbol must be set".into()));
        }

        Ok(crate::runtime::ExchangeApi {
            exchanges: self.exchanges,
            streams: self.streams,
            symbols: self.symbols,
            output: self.output,
            update_rate: self.update_rate,
        })
    }
}

impl Default for ExchangeApiBuilder {
    fn default() -> Self {
        Self::new()
    }
}
