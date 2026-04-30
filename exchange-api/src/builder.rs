use crate::output;
use crate::traits::Exchange;
use crate::types::StreamKind;

/// Configuration for an output sink.
/// Will need better abstraction when there are more than two outputs.
#[derive(Clone)]
#[non_exhaustive]
pub struct OutputConfig {
    #[cfg(feature = "kafka")]
    pub kafka: Option<output::kafka::KafkaConfig>,
    #[cfg(feature = "redis")]
    pub redis: Option<output::redis::RedisConfig>,
}

/// Builds an [`ExchangeApi`] instance via a fluent API.
pub struct ExchangeApiBuilder {
    exchanges: Vec<Box<dyn Exchange>>,
    streams: Vec<StreamKind>,
    symbols: Vec<String>,
    output: OutputConfig,
}

impl ExchangeApiBuilder {
    pub fn new() -> Self {
        Self {
            exchanges: Vec::new(),
            streams: Vec::new(),
            symbols: Vec::new(),
            output: OutputConfig {
                #[cfg(feature = "kafka")]
                kafka: None,
                #[cfg(feature = "redis")]
                redis: None,
            },
        }
    }

    /// Set the trading pair symbol, e.g. "btcusdt".
    pub fn symbol(mut self, symbol: impl Into<Vec<String>>) -> Self {
        self.symbols = symbol.into();
        self
    }

    /// Register an exchange implementation.
    pub fn add_exchange(mut self, exchange: impl Exchange) -> Self {
        self.exchanges.push(Box::new(exchange));
        self
    }

    /// Register a data stream to subscribe to on every exchange.
    pub fn register_task(mut self, stream: StreamKind) -> Self {
        self.streams.push(stream);
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

    /// Consume the builder and produce a ready-to-run [`ExchangeApi`].
    pub async fn build(self) -> Result<crate::runtime::ExchangeApi, crate::Error> {
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
        })
    }
}

impl Default for ExchangeApiBuilder {
    fn default() -> Self {
        Self::new()
    }
}
