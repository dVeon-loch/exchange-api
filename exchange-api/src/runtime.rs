use std::fmt::Display;

use serde::Deserialize;

#[derive(Eq, Hash, PartialEq, Deserialize, Clone, Debug)]
pub enum ExchangeName {
    #[serde(alias = "BINANCE", alias = "binance")]
    Binance,
    #[serde(alias = "BYBIT", alias = "bybit")]
    Bybit,
}

impl Display for ExchangeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangeName::Binance => write!(f, "Binance"),
            ExchangeName::Bybit => write!(f, "Bybit"),
        }
    }
}

// ── Native-only runtime ───────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::collections::HashMap;
    use std::sync::Arc;

    use futures::future::join_all;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;
    use ws_proto::WsClient;

    use crate::output::{ChannelOutput, FileOutput, OutputConfig, OutputsSink};
    use crate::traits::Exchange;
    use crate::types::{StreamKind, UpdateRate};
    use crate::SymbolList;

    use super::ExchangeName;

    pub struct ExchangeApi {
        pub(crate) exchanges: HashMap<ExchangeName, Arc<dyn Exchange>>,
        pub(crate) streams: Vec<StreamKind>,
        pub(crate) symbols: Vec<String>,
        pub(crate) output: OutputConfig,
        pub(crate) update_rate: Option<UpdateRate>,
    }

    impl ExchangeApi {
        pub async fn init(mut self) -> Result<ExchangeApiHandle, crate::Error> {
            let mut handles: Vec<tokio::task::JoinHandle<Result<(), crate::Error>>> = vec![];
            let update_rate = self.update_rate;
            let cancel_token = CancellationToken::new();

            for exchange in self.exchanges.values().cloned() {
                let endpoints = exchange.ws_endpoints(&self.symbols, &self.streams, update_rate);

                let (tx, rx) = broadcast::channel(100);

                #[cfg(feature = "redis")]
                if let Some(config) = &self.output.redis {
                    use crate::output::redis::RedisOutput;
                    handles.push(
                        RedisOutput::with_config(rx.resubscribe(), config.clone())
                            .await?
                            .recv_handle,
                    )
                }

                if let Some(config) = &self.output.file {
                    handles.push(FileOutput::new(config, rx.resubscribe()).recv_handle);
                }

                if let Some(ref channel_out) = self.output.custom_channel {
                    handles.push(ChannelOutput::new(channel_out.clone(), rx).recv_handle);
                }

                for endpoint in endpoints {
                    let exchange = Arc::clone(&exchange);
                    let tx = tx.clone();
                    let cancel_token_clone = cancel_token.clone();

                    handles.push(tokio::spawn(async move {
                        let mut config = ws_proto::WsConfig::new(&endpoint.url);
                        if let Some(interval) = exchange.ping_interval() {
                            config = config.with_ping_interval(interval);
                        }
                        let mut client = WsClient::connect(config).await?;

                        match endpoint.subscription {
                            crate::SubscriptionMethod::UrlEncoded => {}
                            crate::SubscriptionMethod::JsonArgs(args) => {
                                let msg =
                                    serde_json::json!({ "op": "subscribe", "args": args });
                                client
                                    .send(ws_proto::WsMessage::Text(msg.to_string()))
                                    .await?;
                            }
                        }

                        let outputs_sink = OutputsSink::new(tx);

                        let mut last_emitted: std::collections::HashMap<
                            String,
                            std::time::Instant,
                        > = std::collections::HashMap::new();

                        loop {
                            tokio::select! {
                                _ = cancel_token_clone.cancelled() => break,
                                msg = client.recv() => match msg {
                                    Ok(msg) => if let Some(msg) = msg {
                                        let text = match msg {
                                            ws_proto::WsMessage::Text(text) => text,
                                            ws_proto::WsMessage::Binary(utf8_text) => {
                                                String::from_utf8(utf8_text).map_err(|err| err.utf8_error())?
                                            }
                                            _ => continue,
                                        };

                                        for data in exchange.parse_stream(&text)? {
                                            let stream_key = match &data {
                                                crate::StreamData::Trade(_) => "Trade",
                                                crate::StreamData::OrderBook(_) => "OrderBook",
                                                crate::StreamData::OrderBookDelta(_) => "OrderBook",
                                                crate::StreamData::Ticker(_) => "Ticker",
                                            };

                                            let should_emit = update_rate.map_or(true, |rate| {
                                                last_emitted
                                                    .get(stream_key)
                                                    .map_or(true, |t| t.elapsed() >= rate.duration)
                                            });

                                            if should_emit {
                                                if update_rate.is_some() {
                                                    last_emitted.insert(
                                                        stream_key.to_string(),
                                                        std::time::Instant::now(),
                                                    );
                                                }

                                                match outputs_sink.route_to_sinks(data) {
                                                    Ok(count) => tracing::debug!(
                                                        output_count = count,
                                                        "Successfully routed data to outputs"
                                                    ),
                                                    Err(err) => {
                                                        tracing::error!(error=%err,"Error routing data to outputs")
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        break
                                    },
                                    Err(err) => return Err(err.into()),
                                },
                            }
                        }

                        Ok(())
                    }));
                }
            }

            Ok(ExchangeApiHandle {
                exchanges: self.exchanges,
                handles,
                cancel_token,
            })
        }
    }

    pub struct ExchangeApiHandle {
        pub(crate) exchanges: HashMap<ExchangeName, Arc<dyn Exchange>>,
        handles: Vec<tokio::task::JoinHandle<Result<(), crate::Error>>>,
        cancel_token: CancellationToken,
    }

    impl ExchangeApiHandle {
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

        pub async fn cancel_all(&self) {
            self.cancel_token.cancel();
        }

        #[allow(dead_code)]
        async fn join_all(self) {
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
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::{ExchangeApi, ExchangeApiHandle};
