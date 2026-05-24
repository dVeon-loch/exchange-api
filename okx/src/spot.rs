use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use exchange_api::runtime::ExchangeName;
use exchange_api::types::UpdateRate;
use exchange_api::{Exchange, SubscriptionMethod, WsEndpoint};
use exchange_api::{prelude::*, SymbolList};

use crate::local_order_book::LocalOrderBook;
use crate::parsers::CombinedStreamRaw;
use crate::parsers::{CombinedStreamEvent, ExchangeInfoPayload};

/// Binance Spot exchange implementation.
///
/// Connects to `wss://stream.binance.com:9443/ws/` using the combined
/// stream endpoint. Supports trade, depth, and ticker streams
/// for one or more symbols on the same connection.
pub struct BinanceSpot {
    http_client: reqwest::Client,
    order_books: Arc<Mutex<HashMap<String, LocalOrderBook>>>,
}

impl BinanceSpot {
    pub fn new() -> Self {
        Self {
            order_books: Arc::new(Mutex::new(HashMap::new())),
            http_client: reqwest::Client::new(),
        }
    }

    fn build_url_suffix(
        symbols: &[String],
        streams: &[StreamKind],
        update_rate: Option<UpdateRate>,
    ) -> String {
        if symbols.is_empty() || streams.is_empty() {
            return String::new();
        }

        let mut suffix = "/stream?streams=".to_string();

        // Supported Binance depth update speeds: 100ms, 1000ms
        let supported_depths = [Duration::from_millis(100), Duration::from_millis(1000)];
        let depth_speed = update_rate
            .and_then(|rate| rate.best_match(&supported_depths))
            .unwrap_or(Duration::from_millis(100));
        let depth_speed_str = if depth_speed.as_millis() >= 1000 {
            "1000ms"
        } else {
            "100ms"
        };

        for stream in streams {
            match stream {
                StreamKind::Trade => {
                    for symbol in symbols {
                        suffix.push_str(&format!("{}@trade/", symbol));
                    }
                }
                StreamKind::OrderBook { .. } => {
                    for symbol in symbols {
                        suffix.push_str(&format!("{}@depth@{}/", symbol, depth_speed_str));
                    }
                }
                StreamKind::Ticker => {
                    for symbol in symbols {
                        suffix.push_str(&format!("{}@ticker/", symbol));
                    }
                }
            }
        }

        // Binance rejects a trailing `/` on the combined-stream URL.
        suffix.truncate(suffix.len() - 1);
        suffix
    }
}

impl Default for BinanceSpot {
    fn default() -> Self {
        Self::new()
    }
}

static STREAM_BASE_URL: &str = "wss://stream.binance.com:9443";
#[expect(dead_code)]
static WS_OUTGOING_LIMIT_PER_S: u8 = 5;
#[expect(dead_code)]
static MAX_STREAMS_PER_CONN: u16 = 1024;

#[async_trait::async_trait]
impl Exchange for BinanceSpot {
    fn name(&self) -> ExchangeName {
        ExchangeName::Binance
    }

    fn ws_endpoints(
        &self,
        symbols: &[String],
        streams: &[StreamKind],
        update_rate: Option<UpdateRate>,
    ) -> Vec<WsEndpoint> {
        let suffix = Self::build_url_suffix(symbols, streams, update_rate);
        vec![WsEndpoint {
            url: format!("{}{}", STREAM_BASE_URL, suffix),
            subscription: SubscriptionMethod::UrlEncoded,
        }]
    }

    fn parse_stream(
        &self,
        raw: &str,
    ) -> Result<Vec<exchange_api::StreamData>, exchange_api::Error> {
        let msg: CombinedStreamRaw = serde_json::from_str(raw)?;
        match msg.parse()? {
            CombinedStreamEvent::Ticker(ticker) => {
                Ok(vec![exchange_api::StreamData::Ticker(ticker.try_into()?)])
            }
            CombinedStreamEvent::Trade(trade) => {
                Ok(vec![exchange_api::StreamData::Trade(trade.try_into()?)])
            }
            CombinedStreamEvent::DepthUpdate(update) => {
                let mut books = self.order_books.lock().unwrap();
                let ob = books
                    .entry(update.symbol.clone())
                    .or_insert_with(|| LocalOrderBook::new(update.symbol.clone()));
                match ob.handle_update(&update) {
                    Ok(Some(snapshot)) => Ok(vec![exchange_api::StreamData::OrderBook(snapshot)]),
                    Ok(None) => Ok(vec![]),
                    Err(_) => {
                        ob.reset();
                        let _ = ob.handle_update(&update);
                        Ok(vec![])
                    }
                }
            }
        }
    }

    async fn fetch_symbol_list(&self) -> Result<exchange_api::SymbolList, exchange_api::Error> {
        static BASE_URL: &str = "https://data-api.binance.vision";

        static ENDPOINT: &str = "/api/v3/exchangeInfo";

        let res = self
            .http_client
            .get(format!("{BASE_URL}{ENDPOINT}"))
            .send()
            .await?;

        Ok(serde_json::from_slice::<ExchangeInfoPayload>(&res.bytes().await?)?.into())
    }
}
