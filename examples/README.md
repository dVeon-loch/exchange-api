# examples

Native examples. Run from the workspace root with `cargo run -p examples --bin <name>`.

## [multi-exchange](src/bin/multi_exchange.rs)

Streams trades, tickers, and order book events for BTCUSDT from Binance and Bybit
simultaneously. Prints each normalised event to stdout.

```sh
cargo run -p examples --bin multi-exchange
```
