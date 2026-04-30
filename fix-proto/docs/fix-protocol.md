# FIX Protocol Reference

## Overview

FIX (Financial Information eXchange) is an open, text-based protocol for
real-time electronic trading. It was developed in 1992 and has become the
de-facto standard across the financial industry.

Unlike the WebSocket JSON APIs that most crypto exchanges expose (which
tend to differ wildly between exchanges), FIX is standardized — the same
message format, session logic, and field identifiers work across hundreds
of brokers and exchanges worldwide.

## FIX Versions

| Version | Code | Notes |
|---------|------|-------|
| FIX 4.0 | `FIX.4.0` | Original, rarely seen |
| FIX 4.1 | `FIX.4.1` | Added execution report enhancements |
| FIX 4.2 | `FIX.4.2` | Widely adopted, market data added |
| FIX 4.3 | `FIX.4.3` | Added repeating groups, security types |
| FIX 4.4 | `FIX.4.4` | **Most common** — default for most APIs |
| FIXT 1.1 | `FIXT.1.1` | Transport layer split from application (used with 5.0) |
| FIX 5.0 | `FIX.5.0` | Uses FIXT 1.1 transport, adds AppData |
| FIX 5.0 SP1 | `FIX.5.0SP1` | Minor updates |
| FIX 5.0 SP2 | `FIX.5.0SP2` | Latest version |

**Which version to use?** Most crypto exchanges use **FIX 4.4**.
Binance uses **FIX 4.4** for its FIX API.

## Message Format

FIX messages are text-based, using tag=value pairs separated by the SOH
(Start Of Heading) character — byte `0x01`, represented as `|` or `\x01`
in documentation.

```
8=FIX.4.4\x019=45\x0135=A\x0149=EXECUTOR\x0156=CLIENT1\x0134=1\x0152=20240101-00:00:00\x0198=0\x01108=30\x0110=000\x01
```

Each message is composed of three sections:

### 1. Standard Header (tags 8, 9, 35, 49, 56, 34, 52)

| Tag | Name | Description |
|-----|------|-------------|
| 8 | BeginString | FIX version (e.g. `FIX.4.4`) |
| 9 | BodyLength | Number of bytes in the body (after tag 9, before tag 10) |
| 35 | MsgType | Message type code (see below) |
| 49 | SenderCompID | Our identifier (assigned by exchange) |
| 56 | TargetCompID | Exchange/venue identifier |
| 34 | MsgSeqNum | Sequence number (starts at 1, increments per message) |
| 52 | SendingTime | UTC timestamp in `YYYYMMDD-HH:MM:SS` format |
| 43 | PossDupFlag | Y/N — set to Y when retransmitting after a gap fill |
| 369 | LastMsgSeqNum | Used in resend requests |

### 2. Body (message-specific fields)

Varies by message type. Common body fields include:

| Tag | Name | Usage |
|-----|------|-------|
| 55 | Symbol | Instrument (e.g. `BTCUSDT`) |
| 54 | Side | `1`=Buy, `2`=Sell, `5`=SellShort |
| 38 | OrderQty | Order quantity |
| 40 | OrdType | `1`=Market, `2`=Limit, `3`=Stop, `4`=StopLimit |
| 44 | Price | Order price |
| 59 | TimeInForce | `0`=Day, `1`=GTC, `3`=IOC, `4`=FOK |
| 37 | OrderID | Exchange-assigned order ID |
| 17 | ExecID | Unique execution ID |
| 150 | ExecType | `0`=New, `1`=Partial Fill, `2`=Fill, `4`=Cancelled, `8`=Rejected |
| 39 | OrdStatus | Same codes as ExecType |
| 14 | CumQty | Total filled quantity |
| 6 | AvgPx | Average fill price |
| 31 | LastPx | Last fill price |
| 32 | LastQty | Last fill quantity |

### 3. Trailer (tag 10)

| Tag | Name | Description |
|-----|------|-------------|
| 10 | CheckSum | Sum of all bytes modulo 256, formatted as 3 digits |

**Checksum calculation:**
1. Take the raw bytes of the message from tag 8 through the last field
   *before* tag 10 (including all SOH delimiters, not including tag 10 itself)
2. Sum all byte values
3. Compute `sum % 256`
4. Format as zero-padded 3-digit decimal string (e.g. `010`, `222`, `003`)

```
Example: message "8=FIX.4.4\x019=12\x0135=0\x01"
  sum = 56+61+70+73+88+46+52+46+52+1+57+61+49+50+1+51+53+61+48+1 = 888
  checksum = 888 % 256 = 120
  → "10=120\x01"
```

## Message Types (MsgType)

| Code | Name | Direction | Purpose |
|------|------|-----------|---------|
| 0 | Heartbeat | Both | Keep-alive, sent every HeartBtInt seconds |
| 1 | TestRequest | Both | Verify peer is alive; peer responds with Heartbeat |
| 2 | ResendRequest | Both | Request retransmission of messages in a sequence range |
| 3 | Reject | Both | Message was rejected (bad format, invalid seq, etc.) |
| 4 | SequenceReset | Both | Reset sequence numbers (gap fill) |
| 5 | Logout | Both | End session |
| A | Logon | Both | Authenticate and establish session |
| D | NewOrderSingle | Initiator | Submit a new order |
| 8 | ExecutionReport | Acceptor | Order status update (new, fill, cancel, reject) |
| F | OrderCancelRequest | Initiator | Cancel an existing order |
| 9 | OrderCancelReject | Acceptor | Cancel request was rejected |
| V | MarketDataRequest | Initiator | Subscribe to market data |
| W | MarketDataSnapshot | Acceptor | Full market data snapshot |
| X | MarketDataIncremental | Acceptor | Incremental market data update |

## Session Lifecycle

```
Disconnected
    │
    ├─ (initiator) ── TCP/WS connect ──▶ LogonSent
    │                                        │
    │                                    receive Logon
    │                                        │
    │                                    ◀── Active ──▶ Heartbeat (every HeartBtInt)
    │                                            │
    │                                       send/receive TestRequest/Heartbeat
    │                                            │
    │                                       send Logout
    │                                            │
    │                                        LogoutSent
    │                                            │
    │                                    receive Logout
    │                                            │
    │                                        Disconnected
```

### Logon Sequence
1. **Initiator** opens TCP/WS connection
2. **Initiator** sends Logon (MsgType A) with EncryptMethod, HeartBtInt,
   DefaultApplVerID, and sometimes credentials in a separate tag
3. **Acceptor** responds with Logon to confirm
4. Session is now Active — both sides send heartbeats

### Sequence Numbers
- Each side maintains its own `MsgSeqNum` (outgoing and incoming)
- Starts at 1, increments by 1 for each message sent
- Cannot skip — a missing sequence number triggers a ResendRequest
- On disconnect/reconnect, the sequence continues unless `ResetSeqNumFlag=Y`

### Gap Fill / Resend
- If a message is missed (sequence number gap), receiver sends ResendRequest
- Sender retransmits from its message store
- If retransmission is not possible, sender sends SequenceReset with
  `GapFillFlag=Y` and `NewSeqNo` to skip

## Exchange-Specific FIX Implementations

### Binance FIX
- **Version:** FIX 4.4
- **Transport:** WebSocket only (not raw TCP)
- **Endpoint:** `wss://fix-ws.binance.com/`
- **Credentials:** API key + private key (RSA signed Logon)
- **Custom tags:** Binance adds exchange-specific tags (e.g., for
  order types, filters, account details)
- **Market data:** Separate WebSocket JSON stream (not FIX)
- **Docs:** https://binance-docs.github.io/apidocs/fix/en/

### Coinbase FIX
- **Version:** FIX 4.4
- **Transport:** Raw TCP
- **Endpoint:** `fix.coinbase.com:4198`
- **Credentials:** API key + passphrase + signature in Logon
- **Market data:** FIX MarketDataRequest + Snapshot/Incremental

### Kraken FIX
- **Version:** FIX 4.4
- **Transport:** WebSocket
- **Endpoint:** `wss://ws.fix.kraken.com/`
- **Credentials:** API key + private key (WebSocket auth prior to Logon)

## Implementation Plan (this crate)

### Phase 1 — Core Protocol (MVP)
- [x] Tag constants (`tags.rs`)
- [x] FIX data types (`types.rs`)
- [ ] FIX message parser/generator (`message.rs`)
- [ ] Session state machine (`session.rs`)
- [ ] Checksum validation

### Phase 2 — Transport
- [ ] WebSocket transport for Binance FIX
- [ ] TCP transport for Coinbase FIX
- [ ] Reconnect with backoff
- [ ] Sequence number persistence

### Phase 3 — Integration
- [ ] Binance FIX exchange implementation (`impl Exchange` for `fix-proto`)
- [ ] Order placement via FIX
- [ ] Execution report handling
- [ ] Market data via FIX (for exchanges that support it)

## Design Decisions

1. **No external FIX library.** We write the parser, session, and
   transport ourselves to keep the dependency tree minimal and avoid
   fighting with library assumptions.

2. **Exchange trait integration.** `fix-proto` will implement the
   `Exchange` trait from `exchange-api`, allowing a FIX connection
   to be used alongside WebSocket JSON connections seamlessly.

3. **Messages as Vec<(u32, String)>.** Raw tag-value pairs are stored
   in order and indexed by HashMap for O(1) lookup. This avoids a
   rigid struct-per-message-type design and handles any exchange's
   custom tags without schema changes.

4. **Session safety.** Sequence numbers are tracked for both directions.
   Gap detection, resend handling, and poss-dup detection are built
   into the session layer.
