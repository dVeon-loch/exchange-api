use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Describes a data stream to subscribe to on every registered exchange.
#[derive(Clone, Debug)]
pub enum StreamKind {
    Trade,
    OrderBook { depth: usize },
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
