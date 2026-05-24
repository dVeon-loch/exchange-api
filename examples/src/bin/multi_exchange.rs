//! Stream unified market data from Binance and Bybit simultaneously.
//!
//! Subscribes to trades, tickers, and order book snapshots for BTCUSDT on
//! both exchanges and prints each event to stdout in a normalised format.
//!
//! Run with:
//!   cargo run -p examples --bin multi-exchange

use binance::BinanceSpot;
use bybit::BybitSpot;
use exchange_api::prelude::*;
use exchange_api::StreamData;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = broadcast::channel::<StreamData>(512);

    // Binance expects lowercase symbols; Bybit uppercases internally.
    let symbols = vec!["btcusdt".to_string()];

    let _handle = ExchangeApiBuilder::new()
        .add_exchange(BinanceSpot::new())
        .add_exchange(BybitSpot::new())
        .symbols(symbols)
        .register_task(StreamKind::Trade)
        .register_task(StreamKind::Ticker)
        .register_task(StreamKind::orderbook(50))
        .add_broadcast_channel(tx)
        .build()?
        .init()
        .await?;

    println!("{:<8}  {:<9}  {:<10}  {}", "exchange", "kind", "symbol", "data");
    println!("{}", "-".repeat(72));

    loop {
        match rx.recv().await {
            Ok(data) => {
                let (exchange, symbol, _) = data.metadata();
                let sym = symbol.to_uppercase();
                match &data {
                    StreamData::Trade(t) => println!(
                        "{:<8}  {:<9}  {:<10}  price={:<12.2}  size={:.6}",
                        exchange, "trade", sym, t.price, t.size,
                    ),
                    StreamData::Ticker(t) => println!(
                        "{:<8}  {:<9}  {:<10}  last={:.2}",
                        exchange, "ticker", sym, t.last_price,
                    ),
                    StreamData::OrderBook(ob) => println!(
                        "{:<8}  {:<9}  {:<10}  bid={:<12.2}  ask={:<12.2}  spread={:.2}",
                        exchange, "orderbook", sym, ob.best_bid, ob.best_ask, ob.spread,
                    ),
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("warn: dropped {n} messages (receiver too slow)");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    Ok(())
}
