//! Connect to Binance combined streams and print the first few parsed events.
//!
//! This exercises the real data path: WS connect → CombinedStreamRaw → parse()
//! → CombinedStreamEvent.  The remaining wiring (CombinedStreamEvent → generic
//! StreamData, output routing) belongs in ExchangeApi::init() — see TODOs below.

use bybit::parsers::{CombinedStreamEvent, CombinedStreamRaw};
use bybit::{BybitSpot, SubscriptionRequest};
use exchange_api::prelude::*;
use exchange_api::Exchange;
use ws_proto::WsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Build exchange configuration ──────────────────────────────────────────
    // TODO: This should use ExchangeApiBuilder + init() once implemented.

    let exchange = BybitSpot::new();
    let symbols = vec!["BTCUSDT".to_string()];
    let streams = &[
        StreamKind::Ticker,
        StreamKind::Trade,
        StreamKind::OrderBook { depth: 50 },
    ];

    // ── Connect ───────────────────────────────────────────────────────────────
    // TODO: This connection loop belongs in ExchangeApi::init().

    let endpoint = &exchange.ws_endpoints(&symbols, streams, None)[0];
    let url = &endpoint.url;
    println!("Connecting to: {url}");

    let config = ws_proto::WsConfig::new(url);
    let mut client = WsClient::connect(config).await?;

    // Example subscription object:
    // {
    //     "req_id": "test", // optional
    //     "op": "subscribe",
    //     "args": [
    //         "orderbook.1.BTCUSDT",
    //         "publicTrade.BTCUSDT",
    //         "orderbook.1.ETHUSDT"
    //     ]
    // }

    if let exchange_api::SubscriptionMethod::JsonArgs(args) = &endpoint.subscription {
        println!("Sending subscriptions: {:?}", args);
        client
            .send(ws_proto::WsMessage::Text(serde_json::to_string(
                &SubscriptionRequest::new(args.clone()),
            )?))
            .await?;
        if let Some(initial_message) = client.recv().await? {
            let text = match initial_message {
                ws_proto::WsMessage::Text(t) => t.to_string(),
                ws_proto::WsMessage::Binary(b) => String::from_utf8(b)?,
                _ => "Invalid message received".to_string(),
            };
            println!("Initial subscription message: {text}")
        }
    }

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

        // println!("Text received: {text}");

        let raw: CombinedStreamRaw = serde_json::from_str(&text)?;
        let event: CombinedStreamEvent<'_> = raw.parse()?;

        match &event {
            CombinedStreamEvent::Ticker(p) => {
                println!("[{count}] ticker | {}  last={}", p.symbol, p.last_price);
            }
            CombinedStreamEvent::Trade(trades) => {
                if let Some(p) = trades.first() {
                    println!(
                        "[{count}] trade  | {}  price={}  qty={}",
                        p.symbol, p.price, p.size
                    );
                }
            }
            CombinedStreamEvent::DepthUpdate(_, p) => {
                println!(
                    "[{count}] orderbook   | {}  bid_updates={}  ask_updates={}  update_id={}  seq={}",
                    p.symbol,
                    p.bids.len(),
                    p.asks.len(),
                    p.update_id,
                    p.seq
                );
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
