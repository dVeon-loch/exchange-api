use exchange_api::prelude::*;
use exchange_api::Exchange;

/// Binance USDⓈ-M Futures exchange implementation.
///
/// Connects to `wss://fstream.binance.com/ws/`. Stream names differ from
/// spot (e.g. `btcusdt@markPrice` instead of `btcusdt@ticker`), and
/// messages include extra fields (funding rate, mark price).
pub struct BinanceFuturesUsd;

// TODO: impl Exchange for BinanceFuturesUsd
//
// fn name(&self) -> &'static str { "binance-futures" }
// fn ws_url(&self, symbols: &[&str], streams: &[StreamKind]) -> String { todo!() }
// fn subscriptions(&self, symbols: &[&str], streams: &[StreamKind]) -> Vec<String> { todo!() }
// fn parse(&self, raw: &str) -> Result<Option<StreamData>, exchange_api::Error> { todo!() }
