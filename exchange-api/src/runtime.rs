use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use futures::future::join_all;
use serde::Deserialize;
use tokio::sync::broadcast;
use ws_proto::WsClient;

use crate::output::{ChannelOutput, FileOutput, OutputConfig, OutputsSink};
use crate::traits::Exchange;
use crate::types::{StreamKind, UpdateRate};
use crate::SymbolList;

#[derive(Eq, Hash, PartialEq, Deserialize)]
pub enum ExchangeName {
    #[serde(alias = "BINANCE", alias = "binance")]
    Binance,
}

impl Display for ExchangeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangeName::Binance => write!(f, "Binance"),
        }
    }
}

/// A fully-configured exchange data pipeline.
///
/// Created via [`ExchangeApiBuilder`](crate::builder::ExchangeApiBuilder).
/// Call [`init`](ExchangeApi::init) to start all WebSocket connections and
/// begin streaming data to the configured outputs.
pub struct ExchangeApi {
    pub(crate) exchanges: HashMap<ExchangeName, Arc<dyn Exchange>>,
    pub(crate) streams: Vec<StreamKind>,
    pub(crate) symbols: Vec<String>,
    pub(crate) output: OutputConfig,
    pub(crate) update_rate: Option<UpdateRate>,
}

impl ExchangeApi {
    /// Start all exchange connections and begin streaming.
    ///
    /// Spawns one tokio task per exchange. Each task:
    ///   1. Connects to the exchange WebSocket
    ///   2. Sends subscription messages
    ///   3. Reads and parses incoming messages
    ///   4. Routes parsed data to configured outputs (Kafka, Redis)
    pub async fn init(mut self) -> Result<ExchangeApiHandle, crate::Error> {
        let mut handles: Vec<tokio::task::JoinHandle<Result<(), crate::Error>>> = vec![];
        let update_rate = self.update_rate;

        for exchange in self.exchanges.values().cloned() {
            let subscription_url = exchange.ws_url(&self.symbols, &self.streams, update_rate);

            let (tx, rx) = broadcast::channel(100);
            // let _kafka_output = if let Some(config) = self.output.kafka {todo!()} else {None};
            // let _redis_output = if let Some(config) = self.output.redis {todo!()} else {None};
            if let Some(config) = &self.output.file {
                handles.push(FileOutput::new(config, rx.resubscribe()).recv_handle);
            }

            if let Some(channel_out) = self.output.custom_channel.take() {
                handles.push(ChannelOutput::new(channel_out, rx).recv_handle);
            }

            handles.push(tokio::spawn(async move {
                let config = ws_proto::WsConfig::new(&subscription_url);
                let mut client = WsClient::connect(config).await?;

                let outputs_sink = OutputsSink::new(tx);
                let mut last_emitted: std::collections::HashMap<String, std::time::Instant> = std::collections::HashMap::new();

                while let Some(msg) = client.recv().await? {
                    let text = match msg {
                        ws_proto::WsMessage::Text(text) => text,
                        ws_proto::WsMessage::Binary(utf8_text) => {
                            String::from_utf8(utf8_text).map_err(|err| err.utf8_error())?
                        }
                        _ => continue, // Pings and Pongs are handled within recv
                    };

                    if let Some(data) = exchange.parse_stream(&text)? {
                        // Apply throttling if update_rate is set
                        let should_emit = update_rate.map_or(true, |rate| {
                            let stream_key = match &data {
                                crate::StreamData::Trade(_) => "Trade",
                                crate::StreamData::OrderBook(_) => "OrderBook",
                                crate::StreamData::Ticker(_) => "Ticker",
                            };

                            last_emitted.get(stream_key)
                                .map_or(true, |t| t.elapsed() >= rate.duration)
                        });

                        if should_emit {
                            if update_rate.is_some() {
                                let stream_key = match &data {
                                    crate::StreamData::Trade(_) => "Trade",
                                    crate::StreamData::OrderBook(_) => "OrderBook",
                                    crate::StreamData::Ticker(_) => "Ticker",
                                };
                                last_emitted.insert(stream_key.to_string(), std::time::Instant::now());
                            }

                            match outputs_sink.route_to_sinks(data) {
                                Ok(count) => tracing::debug!(
                                    output_count = count,
                                    "Successfully routed data to outputs"
                                ),
                                Err(err) => tracing::error!(error=%err,"Error routing data to outputs"),
                            }
                        }
                    }
                }
                Ok(())
            }));
        }

        Ok(ExchangeApiHandle {
            exchanges: self.exchanges,
            handles,
        })
    }
}

/// Handle returned by [`ExchangeApi::init`].
///
/// Dropping this handle will abort all exchange tasks.
pub struct ExchangeApiHandle {
    pub(crate) exchanges: HashMap<ExchangeName, Arc<dyn Exchange>>,
    handles: Vec<tokio::task::JoinHandle<Result<(), crate::Error>>>,
}

impl ExchangeApiHandle {
    /// Gets the symbol list for the requested exchange
    /// It is not guaranteed that the requested exchange has been configured
    /// Requesting an unconfigured exchange will result in a [crate::Error::Exchange] error
    pub async fn get_symbol_list(
        &self,
        exchange: ExchangeName,
    ) -> Result<SymbolList, crate::Error> {
        self.exchanges
            .get(&exchange)
            .ok_or_else(|| {
                crate::Error::Exchange(
                    "Requested exchange has not been configured at startup".to_string(),
                )
            })?
            .fetch_symbol_list()
            .await
    }
    /// Wait for all exchange tasks to complete (runs indefinitely under
    /// normal operation).
    pub async fn join_all(self) {
        for result in join_all(self.handles).await {
            match result {
                Ok(task_result) => {
                    if let Err(err) = task_result {
                        tracing::error!(error=%err,"Exchange task returned an error after completion");
                    }
                }
                Err(join_err) => {
                    tracing::error!(error=%join_err,"Error joining exchange task handle")
                }
            }
        }
    }
}
