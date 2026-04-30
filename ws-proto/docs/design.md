# WebSocket Protocol & Client Design

## Overview

WebSockets provide full-duplex communication over a single TCP connection,
defined in **RFC 6455**. Crypto exchanges universally use WebSockets as
the transport for real-time market data (trades, order book, ticker) and
often for order updates too.

Unlike REST APIs where you poll for data, WebSockets let the server push
data to you as it happens — latency measured in milliseconds rather than
poll intervals.

## Protocol Basics (RFC 6455)

### Handshake

The WebSocket connection starts with an HTTP upgrade request:

```
GET /ws/btcusdt@trade HTTP/1.1
Host: stream.binance.com:9443
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
```

Server responds with:

```
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
```

After the handshake, the connection switches from HTTP to WebSocket
framing protocol.

### Message Framing

Each WebSocket message is wrapped in a frame:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-------+-+-------------+-------------------------------+
|F|R|R|R| opcode|M| Payload len |    Extended payload length    |
|I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
|N|V|V|V|       |S|             |   (if payload len==126/127)   |
| |1|2|3|       |K|             |                               |
+-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - -+
|     Extended payload length continued, if payload len == 127  |
+ - - - - - - - - - - - - - - -+-------------------------------+
|                               |Masking-key, if MASK set to 1  |
+-------------------------------+-------------------------------+
|       Masking-key (continued)         |    Payload Data       |
+--------------------------------------+ - - - - - - - - - - - +
:                     Payload Data continued ...                :
+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -+
|                     Payload Data (continued)                  |
+---------------------------------------------------------------+
```

**Key fields:**
- **FIN**: Final fragment flag (1 = complete message)
- **opcode**: `0x1` = text, `0x2` = binary, `0x8` = close, `0x9` = ping, `0xA` = pong
- **MASK**: Client-to-server messages MUST be masked; server-to-client MUST NOT
- **Payload length**: 7 bits, 7+16 bits, or 7+64 bits depending on value

### Control Frames

| Opcode | Name | Purpose |
|--------|------|---------|
| 0x8 | Close | Initiate close handshake (may include status code + reason) |
| 0x9 | Ping | Liveness check — peer MUST respond with Pong |
| 0xA | Pong | Response to Ping, or unsolicited (for latency measurement) |

### Close Handshake

Either side sends a Close frame. The receiving side echoes a Close frame
back. After sending Close, the sender MUST NOT send further data. After
receiving the echo, the connection is fully closed.

Standard close codes:
- `1000`: Normal closure
- `1001`: Going away (server restart, client navigating away)
- `1006`: Abnormal closure (no close frame received)
- `1011`: Unexpected error

## Exchange WebSocket Patterns

### 1. Binance — Combined Streams

Binance offers a "combined streams" endpoint where multiple streams are
multiplexed on a single connection:

```
URL: wss://stream.binance.com:9443/ws/btcusdt@trade/btcusdt@depth20@100ms/btcusdt@ticker
```

The path contains all stream names separated by `/`. After connecting,
the exchange immediately starts sending data — no subscription message
is needed (though Binance also supports a JSON subscribe API).

Messages are tagged with the stream name:
```json
{"stream":"btcusdt@trade","data":{...}}
{"stream":"btcusdt@depth20@100ms","data":{...}}
```

### 2. Binance — Multiplex Stream (Recommended)

A cleaner approach when using multiple symbols:
```
URL: wss://stream.binance.com:9443/stream
```

Send:
```json
{"method":"SUBSCRIBE","params":["btcusdt@trade","btcusdt@depth20@100ms"],"id":1}
```

Receive per-stream:
```json
{"stream":"btcusdt@trade","data":{...}}
```

### 3. OKX — Subscription Model

OKX uses a JSON request/response model:

```
URL: wss://ws.okx.com:8443/ws/v5/public
```

Send:
```json
{"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}
```

Receive:
```json
{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[...]}
```

Channels are fully specified — no path-level stream selection.

### 4. Coinbase — Subscription Model

```
URL: wss://ws-feed.exchange.coinbase.com
```

Send:
```json
{
  "type": "subscribe",
  "product_ids": ["BTC-USD"],
  "channels": ["level2", "matches", "ticker"]
}
```

Receive:
```json
{"type":"snapshot","product_id":"BTC-USD","bids":[...],"asks":[...]}
{"type":"l2update","product_id":"BTC-USD","changes":[...]}
```

## Reconnection Strategy

Exchange WebSocket connections drop. A robust client must reconnect:

### Exponential Backoff

```
delay(n) = min(initial_delay * 2^n, max_delay)
```

With jitter:
```
delay(n) = min(initial_delay * 2^n, max_delay) + random(0, delay/4)
```

### State Recovery

After reconnecting, the client must restore any active subscriptions.
This means the exchange implementation needs to remember what streams
it subscribed to and re-subscribe on each reconnect.

### Idle Detection

Some exchanges (e.g. Binance) drop idle connections after a few minutes.
A ping interval of 30s with a 10s pong timeout is standard.

## TLS Considerations

All crypto exchanges use WSS (WebSocket over TLS). The `tokio-tungstenite`
crate handles TLS via `native-tls` or `rustls`. Key points:

- **rustls** is preferred (pure Rust, no OpenSSL dependency)
- **native-tls** uses the system TLS library (required for some enterprise
  environments)
- Certificate verification should NOT be disabled in production

## Message Size Limits

- Binance sends depth snapshots up to ~100KB
- Most exchange messages are < 10KB
- The client should not impose artificial size limits
- `tokio-tungstenite` has a default message size limit of 64MB —
  more than adequate

## Implementation Notes (this crate)

### Architecture

```
┌──────────────┐     ┌──────────────┐     ┌─────────────┐
│  Exchange    │────▶│  WsClient    │────▶│  WsStream   │
│  (binance)   │     │  (reconnect) │     │  (tungstenite)│
└──────────────┘     └──────────────┘     └─────────────┘
```

- **WsStream**: Thin wrapper around `tokio_tungstenite::WebSocketStream`.
  Handles the raw byte-level I/O, ping/pong forwarding, and close detection.

- **WsClient**: Wraps WsStream with automatic reconnect. If the underlying
  stream drops, it reconnects using exponential backoff and returns a new
  WsStream transparently.

- **Exchange trait**: Each exchange crate (e.g. `binance`) receives a
  `WsClient` and handles subscription management, message parsing, and
  re-subscription on reconnect.

### Thread Safety

`WsClient` methods take `&mut self` — a single client should be used
from one task. For multi-stream scenarios, create one `WsClient` per
connection (one per exchange, or one per symbol if the exchange doesn't
support multiplexed streams).

### Future Enhancements

- **Connection pooling**: Reuse TCP/TLS connections for multiple streams
  to the same host (HTTP/1.1 keep-alive + WebSocket multiplex).
- **Automatic ping/pong**: Some exchanges send pings; we should respond
  automatically at the stream level.
- **Per-message deflate**: Some exchanges support permessage-deflate
  compression for bandwidth reduction.
