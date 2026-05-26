# examples-wasm

Browser (WASM) examples. Each subdirectory is a self-contained crate built with
[wasm-pack](https://rustwasm.github.io/wasm-pack/installer/).

## [binance-stream](binance-stream/)

Streams BTCUSDT trades and tickers from Binance. Parses events via the `binance`
crate and logs each one to the browser developer console.

```sh
cd binance-stream
wasm-pack build --target web
python3 -m http.server   # or any static file server
# open http://localhost:8000 and check the developer console
```

### Recommended: Trunk

[Trunk](https://trunkrs.dev) builds and hot-reloads in one command. It expects a
different `index.html` entry point — copy the provided template first:

```sh
cargo install trunk --locked # or `cargo binstall trunk` for precompiled binary
cd binance-stream
trunk serve                                # builds, serves, and watches
# open http://localhost:8080 and check the developer console
```
