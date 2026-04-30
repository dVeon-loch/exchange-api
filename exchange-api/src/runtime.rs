use crate::builder::OutputConfig;
use crate::traits::Exchange;
use crate::types::StreamKind;

/// A fully-configured exchange data pipeline.
///
/// Created via [`ExchangeApiBuilder`](crate::builder::ExchangeApiBuilder).
/// Call [`init`](ExchangeApi::init) to start all WebSocket connections and
/// begin streaming data to the configured outputs.
pub struct ExchangeApi {
    pub(crate) exchanges: Vec<Box<dyn Exchange>>,
    pub(crate) streams: Vec<StreamKind>,
    pub(crate) symbols: Vec<String>,
    pub(crate) output: OutputConfig,
}

impl ExchangeApi {
    /// Start all exchange connections and begin streaming.
    ///
    /// Spawns one tokio task per exchange. Each task:
    ///   1. Connects to the exchange WebSocket
    ///   2. Sends subscription messages
    ///   3. Reads and parses incoming messages
    ///   4. Routes parsed data to configured outputs (Kafka, Redis)
    pub async fn init(self) -> Result<ExchangeApiHandle, crate::Error> {
        // TODO: spawn tokio tasks, connect WS, parse, route to outputs
        let _ = self.exchanges;
        let _ = self.streams;
        let _ = self.symbols;
        let _ = self.output;
        Ok(ExchangeApiHandle)
    }
}

/// Handle returned by [`ExchangeApi::init`].
///
/// Dropping this handle will abort all exchange tasks.
pub struct ExchangeApiHandle;

impl ExchangeApiHandle {
    /// Wait for all exchange tasks to complete (runs indefinitely under
    /// normal operation).
    pub async fn join_all(self) {
        // TODO: Join all spawned tasks
    }
}
