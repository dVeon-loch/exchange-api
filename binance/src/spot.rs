use exchange_api::prelude::*;
use exchange_api::Exchange;

/// Binance Spot exchange implementation.
///
/// Connects to `wss://stream.binance.com:9443/ws/` using the combined
/// stream endpoint. Supports trade, depth, and ticker streams
/// for one or more symbols on the same connection.
pub struct BinanceSpot;

static STREAM_BASE_URL: &str = "wss://stream.binance.com:9443";
#[expect(dead_code)]
static WS_OUTGOING_LIMIT_PER_S: u8 = 5;
#[expect(dead_code)]
static MAX_STREAMS_PER_CONN: u16 = 1024;

impl Exchange for BinanceSpot {
    fn name(&self) -> &'static str {
        "binance"
    }

    fn ws_url(&self, symbols: &[&str], streams: &[StreamKind]) -> String {
        let suffix = Self::build_url_suffix(symbols, streams);
        format!("{}{}", STREAM_BASE_URL, suffix)
    }

    fn subscriptions(&self, symbols: &[&str], streams: &[StreamKind]) -> Vec<String> {
        let mut params = Vec::new();
        for stream in streams {
            match stream {
                StreamKind::Trade => {
                    for symbol in symbols {
                        params.push(format!("{}@trade", symbol));
                    }
                }
                StreamKind::OrderBook { depth } => {
                    let level = match *depth {
                        0..=5 => 5,
                        6..=10 => 10,
                        _ => 20,
                    };
                    for symbol in symbols {
                        params.push(format!("{}@depth{}@100ms", symbol, level));
                    }
                }
                StreamKind::Ticker => {
                    for symbol in symbols {
                        params.push(format!("{}@ticker", symbol));
                    }
                }
            }
        }
        params
    }

    fn parse(&self, raw: &str) -> Result<Option<exchange_api::StreamData>, exchange_api::Error> {
        // TODO: Route raw message to the correct parser based on event type field
        let _ = raw;
        todo!("parse binance spot ws message")
    }
}

impl BinanceSpot {
    fn build_url_suffix(symbols: &[&str], streams: &[StreamKind]) -> String {
        if symbols.is_empty() || streams.is_empty() {
            return String::new();
        }

        let mut suffix = "/stream?streams=".to_string();

        for stream in streams {
            match stream {
                StreamKind::Trade => {
                    for symbol in symbols {
                        suffix.push_str(&format!("{}@trade/", symbol));
                    }
                }
                StreamKind::OrderBook { depth } => {
                    let level = match *depth {
                        0..=5 => 5,
                        6..=10 => 10,
                        _ => 20,
                    };
                    for symbol in symbols {
                        suffix.push_str(&format!("{}@depth{}@100ms/", symbol, level));
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
