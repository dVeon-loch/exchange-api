//! Stream normalised market data from Binance in the browser.
//!
//! Subscribes to BTCUSDT trades and tickers, parses events via the
//! `binance` crate, and logs each event to the browser developer console.
//!
//! # Build
//!
//!   wasm-pack build --target web
//!
//! Then serve the examples-wasm directory and open index.html.

use binance::BinanceSpot;
use exchange_api::{Exchange, StreamData, StreamKind, SubscriptionMethod, WsEndpoint};
use futures::{SinkExt, StreamExt};
use gloo_net::websocket::{futures::WebSocket, Message};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

macro_rules! log {
    ($($t:tt)*) => (log(&format!($($t)*)))
}

/// Entry point — called automatically when the WASM module loads.
#[wasm_bindgen(start)]
pub async fn run() {
    let exchange = BinanceSpot::new();
    let symbols = vec!["btcusdt".to_string()];
    let streams = vec![StreamKind::Trade, StreamKind::Ticker];
    let endpoints = exchange.ws_endpoints(&symbols, &streams, None);

    for endpoint in endpoints {
        wasm_bindgen_futures::spawn_local(async move {
            let exchange = BinanceSpot::new();
            if let Err(e) = stream_loop(exchange, endpoint).await {
                log!("[error] {e:?}");
            }
        });
    }
}

async fn stream_loop(exchange: BinanceSpot, endpoint: WsEndpoint) -> Result<(), JsValue> {
    let ws = WebSocket::open(&endpoint.url)
        .map_err(|e| JsValue::from_str(&format!("connect: {e:?}")))?;
    let (mut write, mut read) = ws.split();

    if let SubscriptionMethod::JsonArgs(args) = endpoint.subscription {
        let msg = serde_json::json!({ "op": "subscribe", "args": args }).to_string();
        write
            .send(Message::Text(msg))
            .await
            .map_err(|e| JsValue::from_str(&format!("subscribe: {e:?}")))?;
    }

    while let Some(result) = read.next().await {
        let text = match result.map_err(|e| JsValue::from_str(&format!("recv: {e:?}")))? {
            Message::Text(t) => t,
            Message::Bytes(b) => {
                String::from_utf8(b).map_err(|e| JsValue::from_str(&e.to_string()))?
            }
        };

        match exchange.parse_stream(&text) {
            Ok(events) => events.iter().for_each(print_event),
            Err(e) => log!("[parse] {e}"),
        }
    }

    Ok(())
}

fn print_event(event: &StreamData) {
    match event {
        StreamData::Trade(t) => log!(
            "[trade]  {} | price={:.2}  size={:.6}",
            t.symbol,
            t.price,
            t.size
        ),
        StreamData::Ticker(t) => log!("[ticker] {} | last={:.2}", t.symbol, t.last_price),
        StreamData::OrderBook(ob) => log!(
            "[book]   {} | bid={:.2}  ask={:.2}  spread={:.2}",
            ob.symbol,
            ob.best_bid,
            ob.best_ask,
            ob.spread
        ),
        StreamData::OrderBookDelta(d) => log!(
            "[delta]  {} | bid={:.2}  ask={:.2}  spread={:.2}",
            d.symbol,
            d.best_bid,
            d.best_ask,
            d.spread
        ),
    }
}
