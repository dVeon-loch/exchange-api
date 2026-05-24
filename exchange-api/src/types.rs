use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Target update rate for streaming data. Exchanges match this to their
/// supported rates and select the closest supported rate.
#[derive(Clone, Copy, Debug)]
pub struct UpdateRate {
    pub duration: Duration,
}

impl UpdateRate {
    pub fn from_millis(ms: u64) -> Self {
        Self {
            duration: Duration::from_millis(ms),
        }
    }

    /// Find the best supported rate from the given options.
    /// Returns the supported rate with minimum absolute difference from target.
    pub fn best_match(&self, supported: &[Duration]) -> Option<Duration> {
        supported
            .iter()
            .min_by_key(|&&rate| {
                let target_ms = self.duration.as_millis();
                let rate_ms = rate.as_millis();
                (target_ms as i128 - rate_ms as i128).abs()
            })
            .copied()
    }
}

/// Describes a data stream to subscribe to on every registered exchange.
#[derive(Clone, Debug)]
pub enum StreamKind {
    Trade,
    OrderBook { depth: usize }, // TODO: Refactor this to make it impossible to select an incorrect depth level per-exchange
    Ticker,
}

impl StreamKind {
    pub fn orderbook(depth: usize) -> Self {
        Self::OrderBook { depth }
    }
}

/// A single trade execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trade {
    pub exchange: String,
    pub symbol: String,
    pub price: f64,
    pub size: f64,
    pub side: Side,
    pub trade_id: String,
    pub timestamp: DateTime<Utc>,
}

/// A price level in the order book.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: f64,
    pub size: f64,
}

/// An order book snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub exchange: String,
    pub symbol: String,
    pub time: DateTime<Utc>,
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread: f64,
    pub bid_depth: f64,
    pub ask_depth: f64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

/// A ticker update.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ticker {
    pub exchange: String,
    pub symbol: String,
    pub last_price: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

/// Unified stream event produced by parsers and consumed by outputs.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamData {
    Trade(Trade),
    OrderBook(OrderBookSnapshot),
    Ticker(Ticker),
}

impl StreamData {
    pub fn to_string_pretty(&self) -> String {
        match self {
            StreamData::Trade(trade) => format!(
                "[{}] trade {}  price={}  qty={}\n",
                trade.timestamp.to_rfc3339(),
                trade.symbol,
                trade.price,
                trade.size
            ),
            StreamData::OrderBook(orderbook) => format!(
                "[{}] orderbook snapshot  | bids={:?} asks={:?}  best_bid={} best_ask={}\n",
                orderbook.time.to_rfc3339(),
                orderbook.bids,
                orderbook.asks,
                orderbook.best_bid,
                orderbook.best_ask
            ),
            StreamData::Ticker(ticker) => {
                format!(
                    "[{}] ticker | {}  last={}\n",
                    ticker.timestamp.to_rfc3339(),
                    ticker.symbol,
                    ticker.last_price
                )
            }
        }
    }

    pub fn metadata(&self) -> (&str, &str, &str) {
        match self {
            StreamData::Trade(t) => (&t.exchange, &t.symbol, "trade"),
            StreamData::OrderBook(o) => (&o.exchange, &o.symbol, "orderbook"),
            StreamData::Ticker(t) => (&t.exchange, &t.symbol, "ticker"),
        }
    }
}

/// Unified symbol list retrieved from exchanges periodically
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolList {
    pub exchange: String,
    pub updated_at: DateTime<Utc>,
    pub symbols: Vec<Symbol>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Symbol {
    pub symbol: String,
    pub base: String,
    pub quote: String,
}
