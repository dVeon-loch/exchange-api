use chrono::{DateTime, Utc};
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;
use tokio::sync::oneshot;

use crate::parsers::{DepthUpdatePayload, PriceLevel};
use exchange_api::http::{HttpClient, HttpRequest, ReqwestBackend};

/// REST depth snapshot response from Binance.
#[derive(serde::Deserialize)]
pub struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

struct BufferedUpdate {
    first_update_id: i64,
    final_update_id: i64,
    event_time: i64,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

enum ObState {
    Buffering {
        buffer: Vec<BufferedUpdate>,
        snapshot_rx: Option<oneshot::Receiver<Result<DepthSnapshot, exchange_api::Error>>>,
    },
    Live,
}

pub struct LocalOrderBook {
    pub symbol: String,
    pub last_update_id: i64,
    pub last_event_time: i64,
    pub bids: BTreeMap<OrderedFloat<f64>, f64>,
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
    state: ObState,
}

impl LocalOrderBook {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            last_update_id: 0,
            last_event_time: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            state: ObState::Buffering {
                buffer: Vec::new(),
                snapshot_rx: None,
            },
        }
    }

    pub fn reset(&mut self) {
        self.last_update_id = 0;
        self.last_event_time = 0;
        self.bids.clear();
        self.asks.clear();
        self.state = ObState::Buffering {
            buffer: Vec::new(),
            snapshot_rx: None,
        };
    }

    /// Apply an incoming diff depth event.
    ///
    /// While buffering, events are queued until the REST snapshot arrives.
    /// Returns `Some(snapshot)` once the OB is live and up to date, or
    /// `None` while still synchronising.
    pub fn handle_update(
        &mut self,
        update: &DepthUpdatePayload<'_>,
    ) -> Result<Option<exchange_api::OrderBookSnapshot>, exchange_api::Error> {
        let owned = owned_update(update)?;

        // Swap state out to avoid borrow conflicts during Live → Buffering transitions.
        let state = std::mem::replace(&mut self.state, ObState::Live);

        match state {
            ObState::Live => {
                self.state = ObState::Live;
                self.apply_owned_update(&owned)?;
                Ok(Some(self.snapshot()))
            }
            ObState::Buffering {
                mut buffer,
                mut snapshot_rx,
            } => {
                // Kick off the REST snapshot fetch on the first event.
                if snapshot_rx.is_none() {
                    let symbol = self.symbol.clone();
                    let (tx, rx) = oneshot::channel();
                    tokio::spawn(async move {
                        let _ = tx.send(fetch_depth_snapshot(&symbol).await);
                    });
                    snapshot_rx = Some(rx);
                }

                buffer.push(owned);

                match snapshot_rx.as_mut().unwrap().try_recv() {
                    Ok(Ok(snapshot)) => {
                        let first_u = buffer.first().map(|u| u.first_update_id).unwrap_or(0);

                        // Snapshot predates our buffer — re-fetch.
                        if snapshot.last_update_id < first_u {
                            let symbol = self.symbol.clone();
                            let (tx, rx) = oneshot::channel();
                            tokio::spawn(async move {
                                let _ = tx.send(fetch_depth_snapshot(&symbol).await);
                            });
                            self.state = ObState::Buffering {
                                buffer,
                                snapshot_rx: Some(rx),
                            };
                            return Ok(None);
                        }

                        // Drop events already covered by the snapshot.
                        buffer.retain(|u| u.final_update_id > snapshot.last_update_id);

                        // Verify continuity between snapshot and first buffered event.
                        if let Some(first) = buffer.first() {
                            if first.first_update_id > snapshot.last_update_id + 1 {
                                return Err(exchange_api::Error::Exchange(
                                    "order book desync: gap between snapshot and buffered stream"
                                        .into(),
                                ));
                            }
                        }

                        // Load snapshot into the OB.
                        self.bids.clear();
                        self.asks.clear();
                        self.last_update_id = snapshot.last_update_id;
                        for [price_str, qty_str] in &snapshot.bids {
                            let (price, qty) = parse_level(price_str, qty_str)?;
                            if qty > 0.0 {
                                self.bids.insert(OrderedFloat(price), qty);
                            }
                        }
                        for [price_str, qty_str] in &snapshot.asks {
                            let (price, qty) = parse_level(price_str, qty_str)?;
                            if qty > 0.0 {
                                self.asks.insert(OrderedFloat(price), qty);
                            }
                        }

                        // Go live and drain the buffer.
                        self.state = ObState::Live;
                        for u in buffer {
                            self.apply_owned_update(&u)?;
                        }

                        Ok(Some(self.snapshot()))
                    }
                    Ok(Err(_)) => {
                        // Fetch failed — re-attempt.
                        let symbol = self.symbol.clone();
                        let (tx, rx) = oneshot::channel();
                        tokio::spawn(async move {
                            let _ = tx.send(fetch_depth_snapshot(&symbol).await);
                        });
                        self.state = ObState::Buffering {
                            buffer,
                            snapshot_rx: Some(rx),
                        };
                        Ok(None)
                    }
                    Err(_) => {
                        // Still waiting for snapshot.
                        self.state = ObState::Buffering {
                            buffer,
                            snapshot_rx,
                        };
                        Ok(None)
                    }
                }
            }
        }
    }

    fn apply_owned_update(&mut self, u: &BufferedUpdate) -> Result<(), exchange_api::Error> {
        if u.final_update_id < self.last_update_id {
            return Ok(()); // stale, ignore
        }
        if u.first_update_id > self.last_update_id + 1 {
            return Err(exchange_api::Error::Exchange(
                "order book desync: missed events".into(),
            ));
        }

        for &(price, qty) in &u.bids {
            if qty == 0.0 {
                self.bids.remove(&OrderedFloat(price));
            } else {
                self.bids.insert(OrderedFloat(price), qty);
            }
        }
        for &(price, qty) in &u.asks {
            if qty == 0.0 {
                self.asks.remove(&OrderedFloat(price));
            } else {
                self.asks.insert(OrderedFloat(price), qty);
            }
        }

        self.last_update_id = u.final_update_id;
        self.last_event_time = u.event_time;
        Ok(())
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

        // Timestamp from the most recent depth update event (E field).
        let time =
            DateTime::from_timestamp_millis(self.last_event_time).unwrap_or_else(|| Utc::now());

        exchange_api::OrderBookSnapshot {
            exchange: "binance".to_string(),
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

pub async fn fetch_depth_snapshot(symbol: &str) -> Result<DepthSnapshot, exchange_api::Error> {
    let client = HttpClient::new(ReqwestBackend::new()?);
    let resp = client
        .send(
            HttpRequest::get("https://api.binance.com/api/v3/depth")
                .with_query("symbol", symbol.to_uppercase())
                .with_query("limit", "5000"),
        )
        .await?;
    resp.json()
}

fn owned_update(u: &DepthUpdatePayload<'_>) -> Result<BufferedUpdate, exchange_api::Error> {
    Ok(BufferedUpdate {
        first_update_id: u.first_update_id,
        final_update_id: u.final_update_id,
        event_time: u.event_time,
        bids: parse_levels(&u.bids)?,
        asks: parse_levels(&u.asks)?,
    })
}

fn parse_levels(levels: &[PriceLevel<'_>]) -> Result<Vec<(f64, f64)>, exchange_api::Error> {
    levels.iter().map(|l| parse_level(&l.0, &l.1)).collect()
}

fn parse_level(price_str: &str, qty_str: &str) -> Result<(f64, f64), exchange_api::Error> {
    let price = price_str
        .parse::<f64>()
        .map_err(|e| exchange_api::Error::Exchange(format!("bad price '{}': {}", price_str, e)))?;
    let qty = qty_str
        .parse::<f64>()
        .map_err(|e| exchange_api::Error::Exchange(format!("bad qty '{}': {}", qty_str, e)))?;
    Ok((price, qty))
}
