//! Connect to Binance combined streams and print the first few parsed events.
//!
//! This exercises the real data path: WS connect → CombinedStreamRaw → parse()
//! → CombinedStreamEvent.  The remaining wiring (CombinedStreamEvent → generic
//! StreamData, output routing) belongs in ExchangeApi::init() — see TODOs below.

use binance::parsers::{CombinedStreamRaw, CombinedStreamEvent};
use binance::BinanceSpot;
use exchange_api::prelude::*;
use ws_proto::WsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Build exchange configuration ──────────────────────────────────────────
    // TODO: This should use ExchangeApiBuilder + init() once implemented.

    let exchange = BinanceSpot;
    let symbols = &["bnbbtc"];
    let streams = &[
        StreamKind::Ticker,
        StreamKind::Trade,
        StreamKind::OrderBook { depth: 10 },
    ];

    // ── Connect ───────────────────────────────────────────────────────────────
    // TODO: This connection loop belongs in ExchangeApi::init().

    let url = exchange.ws_url(symbols, streams);
    println!("Connecting to: {url}");

    let config = ws_proto::WsConfig::new(&url);
    let mut client = WsClient::connect(config).await?;

    // Binance combined stream encodes all subscriptions in the URL, so no
    // subscribe message is needed.

    // ── Receive and parse ─────────────────────────────────────────────────────
    // TODO: Replace raw event with Exchange::parse() + StreamData routing.
    static MAX_COUNT: usize = 25;
    let mut count = 0usize;
    while let Some(msg) = client.recv().await? {
        let text = match msg {
            ws_proto::WsMessage::Text(t) => t.to_string(),
            ws_proto::WsMessage::Binary(b) => String::from_utf8(b)?,
            _ => continue,
        };

        let raw: CombinedStreamRaw = serde_json::from_str(&text)?;
        let event: CombinedStreamEvent<'_> = raw.parse()?;

        match &event {
            CombinedStreamEvent::Ticker(p) => {
                println!("[{count}] ticker | {}  last={}", p.symbol, p.last_price);
            }
            CombinedStreamEvent::Trade(p) => {
                println!("[{count}] trade  | {}  price={}  qty={}",
                    p.symbol, p.price, p.quantity);
            }
            CombinedStreamEvent::OrderBook(p) => {
                let best_bid = p.bids.first().map(|l| &l.0);
                let best_ask = p.asks.first().map(|l| &l.0);
                println!("[{count}] depth  | bids={} asks={}  best_bid={best_bid:?} best_ask={best_ask:?}",
                    p.bids.len(), p.asks.len());
            }
        }

        count += 1;
        if count >= MAX_COUNT {
            break;
        }
    }

    // TODO: Route parsed StreamData to configured outputs (Kafka, Redis).

    println!("Done — received {count} messages.");
    Ok(())
}
