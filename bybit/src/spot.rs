use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use exchange_api::prelude::*;
use exchange_api::runtime::ExchangeName;
use exchange_api::types::UpdateRate;
use exchange_api::{Exchange, SubscriptionMethod, WsEndpoint};

use crate::local_order_book::LocalOrderBook;
use crate::parsers::CombinedStreamRaw;
use crate::parsers::{CombinedStreamEvent, ExchangeInfoPayload};

// When deployed in netherlands
// https://api.bybit.nl
static STREAM_BASE_URL: &str = "wss://stream.bybit.com/v5/public/spot";
#[expect(dead_code)]
static STREAM_BASE_URL_TESTNET: &str = "wss://stream-testnet.bybit.com/v5/public/spot";

/// Bybit Spot exchange implementation.
///
/// Connects to `wss://stream.bybit.com/v5/public/spot`. Supports trade,
/// depth, and ticker streams for one or more symbols on the same connection.
pub struct BybitSpot {
    http_client: reqwest::Client,
    order_books: Arc<Mutex<HashMap<String, LocalOrderBook>>>,
}

impl BybitSpot {
    pub fn new() -> Self {
        Self {
            order_books: Arc::new(Mutex::new(HashMap::new())),
            http_client: reqwest::Client::new(),
        }
    }

    fn build_subscriptions(symbols: &[String], streams: &[StreamKind]) -> Vec<String> {
        if symbols.is_empty() || streams.is_empty() {
            return Vec::new();
        }

        let mut subscriptions = vec![];

        for stream in streams {
            match stream {
                StreamKind::Trade => {
                    for symbol in symbols {
                        subscriptions.push(format!("publicTrade.{}", symbol.to_uppercase()));
                    }
                }
                StreamKind::OrderBook { depth } => {
                    // Valid Bybit spot depths: 1 (10ms), 50 (20ms), 200 (100ms), 1000 (200ms).
                    // Snap the requested depth to the nearest valid level.
                    const VALID_DEPTHS: [usize; 4] = [1, 50, 200, 1000];
                    let snapped = *VALID_DEPTHS
                        .iter()
                        .min_by_key(|&&d| d.abs_diff(*depth))
                        .unwrap();
                    for symbol in symbols {
                        subscriptions.push(format!(
                            "orderbook.{}.{}",
                            snapped,
                            symbol.to_uppercase()
                        ));
                    }
                }
                StreamKind::Ticker => {
                    for symbol in symbols {
                        subscriptions.push(format!("tickers.{}", symbol.to_uppercase()));
                    }
                }
            }
        }

        subscriptions
    }
}

impl Default for BybitSpot {
    fn default() -> Self {
        Self::new()
    }
}

#[expect(dead_code)]
static WS_OUTGOING_LIMIT_PER_S: u8 = 5;
#[expect(dead_code)]
static MAX_STREAMS_PER_CONN: u16 = 1024;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Exchange for BybitSpot {
    fn name(&self) -> ExchangeName {
        ExchangeName::Bybit
    }

    fn ping_interval(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_secs(20))
    }

    fn ws_endpoints(
        &self,
        symbols: &[String],
        streams: &[StreamKind],
        _update_rate: Option<UpdateRate>,
    ) -> Vec<WsEndpoint> {
        vec![WsEndpoint {
            url: STREAM_BASE_URL.to_string(),
            subscription: SubscriptionMethod::JsonArgs(Self::build_subscriptions(symbols, streams)),
        }]
    }

    fn parse_stream(
        &self,
        raw: &str,
    ) -> Result<Vec<exchange_api::StreamData>, exchange_api::Error> {
        // Control messages (sub acks, ping responses) don't have a `topic`
        // field and fail envelope deserialization — return empty vec, not an error.
        let msg: CombinedStreamRaw = match serde_json::from_str(raw) {
            Ok(m) => m,
            Err(_) => return Ok(vec![]),
        };
        match msg.parse()? {
            CombinedStreamEvent::Ticker(ticker) => {
                Ok(vec![exchange_api::StreamData::Ticker(ticker.try_into()?)])
            }
            CombinedStreamEvent::Trade(trades) => trades
                .into_iter()
                .map(|t| t.try_into().map(exchange_api::StreamData::Trade))
                .collect(),
            CombinedStreamEvent::DepthUpdate(update_type, update) => {
                let mut books = self.order_books.lock().unwrap();
                let ob = books
                    .entry(update.symbol.clone())
                    .or_insert_with(|| LocalOrderBook::new(update.symbol.clone()));
                match ob.handle_update(update_type, &update) {
                    Ok(Some(event)) => Ok(vec![event]),
                    Ok(None) => Ok(vec![]),
                    Err(_) => {
                        ob.reset();
                        Ok(vec![])
                    }
                }
            }
        }
    }

    async fn fetch_symbol_list(&self) -> Result<exchange_api::SymbolList, exchange_api::Error> {
        static BASE_URL: &str = "https://api.bybit.com";
        static ENDPOINT: &str = "/v5/market/instruments-info";

        let res = self
            .http_client
            .get(format!("{BASE_URL}{ENDPOINT}"))
            .query(&[("category", "spot")])
            .send()
            .await?;

        Ok(serde_json::from_slice::<ExchangeInfoPayload>(&res.bytes().await?)?.into())
    }
}
