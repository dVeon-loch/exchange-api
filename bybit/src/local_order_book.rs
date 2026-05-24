use chrono::{DateTime, Utc};
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;

use crate::parsers::{DepthUpdatePayload, OrderbookUpdateType, PriceLevel};

pub struct LocalOrderBook {
    pub symbol: String,
    pub last_update_id: u64,
    pub last_event_time: u64,
    pub bids: BTreeMap<OrderedFloat<f64>, f64>,
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
}

impl LocalOrderBook {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            last_update_id: 0,
            last_event_time: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.last_update_id = 0;
        self.last_event_time = 0;
        self.bids.clear();
        self.asks.clear();
    }

    pub fn handle_update(
        &mut self,
        update_type: OrderbookUpdateType,
        update: &DepthUpdatePayload<'_>,
    ) -> Result<Option<exchange_api::OrderBookSnapshot>, exchange_api::Error> {
        if matches!(update_type, OrderbookUpdateType::Snapshot) {
            self.reset();
        }
        apply_side(&mut self.bids, &update.bids)?;
        apply_side(&mut self.asks, &update.asks)?;
        self.last_update_id = update.update_id;
        self.last_event_time = update.timestamp_ms;
        Ok(Some(self.snapshot()))
    }

    pub fn snapshot(&self) -> exchange_api::OrderBookSnapshot {
        let bids: Vec<exchange_api::PriceLevel> = self
            .bids
            .iter()
            .rev()
            .map(|(p, &q)| exchange_api::PriceLevel {
                price: p.0,
                size: q,
            })
            .collect();
        let asks: Vec<exchange_api::PriceLevel> = self
            .asks
            .iter()
            .map(|(p, &q)| exchange_api::PriceLevel {
                price: p.0,
                size: q,
            })
            .collect();

        let best_bid = bids.first().map_or(0.0, |l| l.price);
        let best_ask = asks.first().map_or(0.0, |l| l.price);
        let spread = best_ask - best_bid;
        let bid_depth: f64 = bids.iter().map(|l| l.size).sum();
        let ask_depth: f64 = asks.iter().map(|l| l.size).sum();

        let time = DateTime::from_timestamp_millis(self.last_event_time as i64)
            .unwrap_or_else(Utc::now);

        exchange_api::OrderBookSnapshot {
            exchange: "bybit".to_string(),
            symbol: self.symbol.clone(),
            time,
            best_bid,
            best_ask,
            spread,
            bid_depth,
            ask_depth,
            bids,
            asks,
        }
    }
}

fn apply_side(
    side: &mut BTreeMap<OrderedFloat<f64>, f64>,
    levels: &[PriceLevel<'_>],
) -> Result<(), exchange_api::Error> {
    for level in levels {
        let price = level.0.parse::<f64>().map_err(|e| {
            exchange_api::Error::Exchange(format!("bad price '{}': {e}", level.0))
        })?;
        let qty = level.1.parse::<f64>().map_err(|e| {
            exchange_api::Error::Exchange(format!("bad qty '{}': {e}", level.1))
        })?;
        if qty == 0.0 {
            side.remove(&OrderedFloat(price));
        } else {
            side.insert(OrderedFloat(price), qty);
        }
    }
    Ok(())
}
