use chrono::DateTime;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::fmt;
use std::num::ParseFloatError;

/// Combined stream wrapper from Bybit WebSocket.
///
/// Bybit uses a "topic" identifier field on all responses
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct CombinedStreamRaw<'a> {
    pub topic: String,
    /// Used for the orderbook only, everything else is "snapshot"
    #[serde(rename = "type")]
    pub update_type: Cow<'a, str>,
    #[serde(rename = "ts")]
    pub timestamp_ms: u64,
    pub data: Box<RawValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderbookUpdateType {
    Snapshot,
    Delta,
}

#[allow(dead_code)]
impl CombinedStreamRaw<'_> {
    /// Parse the raw inner payload into the correct typed variant.
    pub fn parse(&self) -> Result<CombinedStreamEvent<'_>, exchange_api::Error> {
        let data = self.data.get();
        if self.topic.contains("tickers") {
            let mut payload: TickerPayload<'_> = serde_json::from_str(data)?;
            payload.timestamp_ms = self.timestamp_ms;
            Ok(CombinedStreamEvent::Ticker(payload))
        } else if self.topic.contains("publicTrade") {
            let mut trades: Vec<TradePayload<'_>> = serde_json::from_str(data)?;
            for t in &mut trades {
                t.timestamp_ms = self.timestamp_ms;
            }
            Ok(CombinedStreamEvent::Trade(trades))
        } else if self.topic.contains("orderbook") {
            let update_type = match self.update_type.as_ref() {
                "snapshot" => OrderbookUpdateType::Snapshot,
                "delta" => OrderbookUpdateType::Delta,
                other => {
                    return Err(exchange_api::Error::Config(format!(
                        "unknown orderbook update type: {other}"
                    )))
                }
            };
            let mut payload: DepthUpdatePayload<'_> = serde_json::from_str(data)?;
            payload.timestamp_ms = self.timestamp_ms;
            Ok(CombinedStreamEvent::DepthUpdate(update_type, payload))
        } else {
            Err(exchange_api::Error::Config(format!(
                "unknown stream type: {}",
                self.topic,
            )))
        }
    }
}

/// A combined stream event with its inner payload already dispatched to the
/// correct optimized type.
#[allow(dead_code)]
#[derive(Debug)]
pub enum CombinedStreamEvent<'a> {
    Ticker(TickerPayload<'a>),
    /// Bybit batches multiple trades per message.
    Trade(Vec<TradePayload<'a>>),
    DepthUpdate(OrderbookUpdateType, DepthUpdatePayload<'a>),
}

// Topic: tickers.{symbol}
// `data` is a single object (always "snapshot" type for spot).

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TickerPayload<'a> {
    /// Set from the outer CombinedStreamRaw.timestamp_ms after deserialization.
    #[serde(skip)]
    pub timestamp_ms: u64,
    #[serde(borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(borrow)]
    pub last_price: Cow<'a, str>,
    #[serde(borrow)]
    pub high_price_24h: Cow<'a, str>,
    #[serde(borrow)]
    pub low_price_24h: Cow<'a, str>,
    #[serde(borrow)]
    pub prev_price_24h: Cow<'a, str>,
    #[serde(borrow)]
    pub volume_24h: Cow<'a, str>,
    #[serde(borrow)]
    pub turnover_24h: Cow<'a, str>,
    #[serde(borrow)]
    pub price_24h_pcnt: Cow<'a, str>,
    #[serde(borrow)]
    pub usd_index_price: Cow<'a, str>,
}

impl TryInto<exchange_api::Ticker> for TickerPayload<'_> {
    type Error = exchange_api::Error;

    fn try_into(self) -> Result<exchange_api::Ticker, Self::Error> {
        Ok(exchange_api::Ticker {
            exchange: "bybit".to_string(),
            symbol: self.symbol.to_string(),
            last_price: self
                .last_price
                .parse::<f64>()
                .map_err(|err| parse_float_error("last_price", err))?,
            timestamp: DateTime::from_timestamp_millis(self.timestamp_ms as i64).ok_or_else(
                || {
                    exchange_api::Error::Exchange(
                        "could not parse Bybit ts as valid DateTime<Utc>".to_string(),
                    )
                },
            )?,
        })
    }
}

fn parse_float_error(field: &'static str, error: ParseFloatError) -> exchange_api::Error {
    exchange_api::Error::Exchange(format!(
        "could not parse Bybit returned {field} as valid f64. Error: {error}"
    ))
}

// Topic: publicTrade.{symbol}
// `data` is an array of trade entries (ascending fill time).

//     "data": [
//         {
//             "T": 1672304486865,
//             "s": "BTCUSDT",
//             "S": "Buy",
//             "v": "0.001",
//             "p": "16578.50",
//             "i": "20f43950-d8dd-5b31-9112-a178eb6023af",
//             "BT": false,
//             "seq": 1783284617
//         }
//     ]

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct TradePayload<'a> {
    /// Set from the outer CombinedStreamRaw.timestamp_ms after deserialization.
    #[serde(skip)]
    pub timestamp_ms: u64,
    /// Fill timestamp (milliseconds).
    #[serde(rename = "T")]
    pub fill_timestamp_ms: u64,
    #[serde(rename = "s", borrow)]
    pub symbol: Cow<'a, str>,
    /// Taker side: `"Buy"` or `"Sell"`.
    #[serde(rename = "S", borrow)]
    pub side: Cow<'a, str>,
    /// Trade size.
    #[serde(rename = "v", borrow)]
    pub size: Cow<'a, str>,
    /// Trade price.
    #[serde(rename = "p", borrow)]
    pub price: Cow<'a, str>,
    /// Trade ID (UUID string).
    #[serde(rename = "i", borrow)]
    pub trade_id: Cow<'a, str>,
    /// Block trade indicator.
    #[serde(rename = "BT")]
    pub is_block_trade: bool,
    /// Cross sequence number.
    #[serde(rename = "seq")]
    pub seq: u64,
}

impl TryInto<exchange_api::Trade> for TradePayload<'_> {
    type Error = exchange_api::Error;

    fn try_into(self) -> Result<exchange_api::Trade, Self::Error> {
        Ok(exchange_api::Trade {
            exchange: "bybit".to_string(),
            symbol: self.symbol.to_string(),
            price: self
                .price
                .parse::<f64>()
                .map_err(|err| parse_float_error("price", err))?,
            size: self
                .size
                .parse::<f64>()
                .map_err(|err| parse_float_error("size", err))?,
            side: match self.side.as_ref() {
                "Buy" => exchange_api::Side::Buy,
                "Sell" => exchange_api::Side::Sell,
                other => {
                    return Err(exchange_api::Error::Exchange(format!(
                        "unknown side: {other}"
                    )))
                }
            },
            trade_id: self.trade_id.to_string(),
            timestamp: DateTime::from_timestamp_millis(self.fill_timestamp_ms as i64).ok_or_else(
                || {
                    exchange_api::Error::Exchange(
                        "could not parse Bybit T as valid DateTime<Utc>".to_string(),
                    )
                },
            )?,
        })
    }
}

// Stream name: <symbol>@depth OR <symbol>@depth@100ms

/// A single price level — deserialized from a JSON array `[price, quantity]`.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct PriceLevel<'a>(
    #[serde(borrow)] pub Cow<'a, str>,
    #[serde(borrow)] pub Cow<'a, str>,
);

// Stream name: orderbook
//
// "data": {
//     "s": "BTCUSDT",
//     "b": [
//         ...,
//         [
//             "16493.50",
//             "0.006"
//         ],
//         [
//             "16493.00",
//             "0.100"
//         ]
//     ],
//     "a": [
//         [
//             "16611.00",
//             "0.029"
//         ],
//         [
//             "16612.00",
//             "0.213"
//         ],
//         ...,
//     ],
// "u": 18521288,
// "seq": 7961638724
// },

// Topic: orderbook.{depth}.{symbol}
// `data` is the same structure for both snapshot and delta; the outer
// `type` field (OrderbookUpdateType) distinguishes them.

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct DepthUpdatePayload<'a> {
    /// Set from the outer CombinedStreamRaw.timestamp_ms after deserialization.
    #[serde(skip)]
    pub timestamp_ms: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b", borrow)]
    pub bids: Vec<PriceLevel<'a>>,
    #[serde(rename = "a", borrow)]
    pub asks: Vec<PriceLevel<'a>>,
    /// Update ID — monotonically increasing within a symbol.
    #[serde(rename = "u")]
    pub update_id: u64,
    /// Cross sequence number.
    #[serde(rename = "seq")]
    pub seq: u64,
}

// GET /v5/market/instruments-info?category=spot
//
// {
//   "retCode": 0,
//   "result": {
//     "category": "spot",
//     "list": [
//       { "symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "status": "Trading", ... }
//     ],
//     "nextPageCursor": ""
//   },
//   "time": 1672712468011
// }

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ExchangeInfoPayload<'a> {
    #[serde(rename = "retCode")]
    pub ret_code: i32,
    #[serde(borrow)]
    pub result: ExchangeInfoResult<'a>,
    pub time: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ExchangeInfoResult<'a> {
    #[serde(borrow)]
    pub list: Vec<SymbolPayload<'a>>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct SymbolPayload<'a> {
    #[serde(borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(rename = "baseCoin", borrow)]
    pub base_coin: Cow<'a, str>,
    #[serde(rename = "quoteCoin", borrow)]
    pub quote_coin: Cow<'a, str>,
    #[serde(borrow)]
    pub status: Cow<'a, str>,
}

impl Into<exchange_api::SymbolList> for ExchangeInfoPayload<'_> {
    fn into(self) -> exchange_api::SymbolList {
        exchange_api::SymbolList {
            exchange: "bybit".to_string(),
            updated_at: DateTime::from_timestamp_millis(self.time).unwrap_or_default(),
            symbols: self
                .result
                .list
                .into_iter()
                .filter(|s| s.status == "Trading")
                .map(SymbolPayload::into)
                .collect(),
        }
    }
}

impl Into<exchange_api::Symbol> for SymbolPayload<'_> {
    fn into(self) -> exchange_api::Symbol {
        exchange_api::Symbol {
            symbol: self.symbol.to_string(),
            base: self.base_coin.to_string(),
            quote: self.quote_coin.to_string(),
        }
    }
}

// Returned when a SUBSCRIBE/UNSUBSCRIBE/etc. command receives an error.

/// Raw error response from a Binance WebSocket command.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct WsErrorPayload<'a> {
    pub code: i64,
    #[serde(borrow)]
    pub msg: &'a str,
}

#[allow(dead_code)]
impl<'a> WsErrorPayload<'a> {
    /// Convert into a typed [`WsError`], discarding the raw `msg` for
    /// single-variant codes (0, 1, 3) and cheaply disambiguating code 2.
    ///
    /// Allocates only for [`WsError::MissingField`] and [`WsError::Unknown`].
    pub fn classify(self) -> WsError {
        match self.code {
            0 => WsError::UnknownProperty,
            1 => WsError::InvalidValueType,
            2 => {
                if self.msg.contains("property name must be a string") {
                    WsError::InvalidPropertyName
                } else if self.msg.contains("request ID must be an unsigned integer") {
                    WsError::InvalidRequestId
                } else if self.msg.contains("unknown variant") {
                    WsError::UnknownMethod
                } else if self.msg.contains("too many parameters") {
                    WsError::TooManyParameters
                } else if self.msg.contains("missing field") {
                    let field = self.msg.split('`').nth(1).unwrap_or("?").to_string();
                    WsError::MissingField(field)
                } else {
                    WsError::Unknown {
                        code: 2,
                        msg: self.msg.to_string(),
                    }
                }
            }
            3 => WsError::InvalidJson,
            code => WsError::Unknown {
                code,
                msg: self.msg.to_string(),
            },
        }
    }
}

/// Typed WebSocket command error for matchable error handling.
///
/// Construct by deserializing [`WsErrorPayload`] from a JSON response, then
/// calling `.classify()`:
///
/// ```ignore
/// let payload: WsErrorPayload = serde_json::from_str(raw).unwrap();
/// let err = payload.classify();
/// ```
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsError {
    /// code 0: unknown/malformed property in SET_PROPERTY / GET_PROPERTY
    UnknownProperty,
    /// code 1: value was not a boolean
    InvalidValueType,
    /// code 2: "property name must be a string"
    InvalidPropertyName,
    /// code 2: "request ID must be an unsigned integer"
    InvalidRequestId,
    /// code 2: "unknown variant X, expected SUBSCRIBE|UNSUBSCRIBE|..."
    UnknownMethod,
    /// code 2: "too many parameters"
    TooManyParameters,
    /// code 2: "missing field `<name>` at line N column M"
    MissingField(String),
    /// code 3: "Invalid JSON: expected value at line N column M"
    InvalidJson,
    /// Any unrecognised code/msg pair.
    Unknown { code: i64, msg: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── TickerPayload ────────────────────────────────────────────────────────

    const TICKER_JSON: &str = r#"{
        "symbol": "BTCUSDT",
        "lastPrice": "30000.00",
        "highPrice24h": "31000.00",
        "lowPrice24h": "29000.00",
        "prevPrice24h": "29500.00",
        "volume24h": "1234.56",
        "turnover24h": "37000000.00",
        "price24hPcnt": "1.69",
        "usdIndexPrice": "30001.00"
    }"#;

    #[test]
    fn ticker_deserializes_all_fields() {
        let p: TickerPayload = serde_json::from_str(TICKER_JSON).unwrap();
        assert_eq!(p.symbol, "BTCUSDT");
        assert_eq!(p.last_price, "30000.00");
        assert_eq!(p.high_price_24h, "31000.00");
        assert_eq!(p.low_price_24h, "29000.00");
        assert_eq!(p.prev_price_24h, "29500.00");
        assert_eq!(p.volume_24h, "1234.56");
        assert_eq!(p.turnover_24h, "37000000.00");
        assert_eq!(p.price_24h_pcnt, "1.69");
        assert_eq!(p.usd_index_price, "30001.00");
        assert_eq!(p.timestamp_ms, 0); // set from envelope, not JSON
    }

    #[test]
    fn ticker_zero_copy_borrows() {
        let input = TICKER_JSON.to_owned();
        let p: TickerPayload = serde_json::from_str(&input).unwrap();
        let base = input.as_ptr() as usize;
        let limit = base + input.len();
        for (field, cow) in [
            ("symbol", &p.symbol),
            ("last_price", &p.last_price),
            ("high_price_24h", &p.high_price_24h),
        ] {
            match cow {
                std::borrow::Cow::Borrowed(s) => {
                    let ptr = s.as_ptr() as usize;
                    assert!(ptr >= base && ptr < limit, "{field} was not borrowed");
                }
                _ => panic!("{field} was not borrowed"),
            }
        }
    }

    // ── TradePayload ─────────────────────────────────────────────────────────

    const TRADE_ARRAY_JSON: &str = r#"[
        {
            "T": 1672304486865,
            "s": "BTCUSDT",
            "S": "Buy",
            "v": "0.001",
            "p": "16578.50",
            "i": "20f43950-d8dd-5b31-9112-a178eb6023af",
            "BT": false,
            "seq": 1783284617
        }
    ]"#;

    #[test]
    fn trade_deserializes_all_fields() {
        let trades: Vec<TradePayload> = serde_json::from_str(TRADE_ARRAY_JSON).unwrap();
        assert_eq!(trades.len(), 1);
        let p = &trades[0];
        assert_eq!(p.fill_timestamp_ms, 1672304486865);
        assert_eq!(p.symbol, "BTCUSDT");
        assert_eq!(p.side, "Buy");
        assert_eq!(p.size, "0.001");
        assert_eq!(p.price, "16578.50");
        assert_eq!(p.trade_id, "20f43950-d8dd-5b31-9112-a178eb6023af");
        assert!(!p.is_block_trade);
        assert_eq!(p.seq, 1783284617);
        assert_eq!(p.timestamp_ms, 0); // set from envelope
    }

    #[test]
    fn trade_zero_copy_borrows() {
        let input = TRADE_ARRAY_JSON.to_owned();
        let trades: Vec<TradePayload> = serde_json::from_str(&input).unwrap();
        let base = input.as_ptr() as usize;
        let limit = base + input.len();
        let p = &trades[0];
        match &p.price {
            std::borrow::Cow::Borrowed(s) => {
                let ptr = s.as_ptr() as usize;
                assert!(ptr >= base && ptr < limit, "price was not borrowed");
            }
            _ => panic!("price was not borrowed"),
        }
    }

    // ── DepthUpdatePayload ───────────────────────────────────────────────────

    const DEPTH_SNAPSHOT_JSON: &str = r#"{
        "s": "BTCUSDT",
        "b": [["16493.50", "0.006"], ["16493.00", "0.100"]],
        "a": [["16611.00", "0.029"], ["16612.00", "0.213"]],
        "u": 18521288,
        "seq": 7961638724
    }"#;

    #[test]
    fn depth_snapshot_deserializes() {
        let p: DepthUpdatePayload = serde_json::from_str(DEPTH_SNAPSHOT_JSON).unwrap();
        assert_eq!(p.symbol, "BTCUSDT");
        assert_eq!(p.bids.len(), 2);
        assert_eq!(p.asks.len(), 2);
        assert_eq!(p.bids[0].0, "16493.50");
        assert_eq!(p.bids[0].1, "0.006");
        assert_eq!(p.asks[0].0, "16611.00");
        assert_eq!(p.update_id, 18521288);
        assert_eq!(p.seq, 7961638724);
        assert_eq!(p.timestamp_ms, 0); // set from envelope
    }

    #[test]
    fn depth_delta_empty_arrays() {
        let json = r#"{"s":"BTCUSDT","b":[],"a":[],"u":18521289,"seq":7961638725}"#;
        let p: DepthUpdatePayload = serde_json::from_str(json).unwrap();
        assert!(p.bids.is_empty());
        assert!(p.asks.is_empty());
        assert_eq!(p.update_id, 18521289);
    }

    // ── ExchangeInfoPayload ──────────────────────────────────────────────────

    const EXCHANGE_INFO_JSON: &str = r#"{
        "retCode": 0,
        "result": {
            "category": "spot",
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "status": "Trading"
                },
                {
                    "symbol": "ETHUSDT",
                    "baseCoin": "ETH",
                    "quoteCoin": "USDT",
                    "status": "Closed"
                }
            ],
            "nextPageCursor": ""
        },
        "time": 1672712468011
    }"#;

    #[test]
    fn exchange_info_deserializes() {
        let p: ExchangeInfoPayload = serde_json::from_str(EXCHANGE_INFO_JSON).unwrap();
        assert_eq!(p.ret_code, 0);
        assert_eq!(p.time, 1672712468011);
        assert_eq!(p.result.list.len(), 2);
        assert_eq!(p.result.list[0].symbol, "BTCUSDT");
        assert_eq!(p.result.list[0].base_coin, "BTC");
        assert_eq!(p.result.list[0].quote_coin, "USDT");
        assert_eq!(p.result.list[0].status, "Trading");
    }

    #[test]
    fn exchange_info_into_symbol_list_filters_non_trading() {
        let p: ExchangeInfoPayload = serde_json::from_str(EXCHANGE_INFO_JSON).unwrap();
        let list: exchange_api::SymbolList = p.into();
        assert_eq!(list.exchange, "bybit");
        // "Closed" symbol should be filtered out
        assert_eq!(list.symbols.len(), 1);
        assert_eq!(list.symbols[0].symbol, "BTCUSDT");
        assert_eq!(list.symbols[0].base, "BTC");
        assert_eq!(list.symbols[0].quote, "USDT");
    }

    // ── WsErrorPayload ───────────────────────────────────────────────────────

    #[test]
    fn error_code_0_unknown_property() {
        let payload = WsErrorPayload {
            code: 0,
            msg: "Unknown property",
        };
        assert_eq!(payload.classify(), WsError::UnknownProperty);
    }

    #[test]
    fn error_code_1_invalid_value_type() {
        let payload = WsErrorPayload {
            code: 1,
            msg: "Invalid value type: expected Boolean",
        };
        assert_eq!(payload.classify(), WsError::InvalidValueType);
    }

    #[test]
    fn error_code_3_invalid_json() {
        let payload = WsErrorPayload {
            code: 3,
            msg: "Invalid JSON: expected value at line 1 column 28",
        };
        assert_eq!(payload.classify(), WsError::InvalidJson);
    }

    #[test]
    fn error_code_2_invalid_property_name() {
        let json = r#"{"code":2,"msg":"Invalid request: property name must be a string","id":1}"#;
        let payload: WsErrorPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.classify(), WsError::InvalidPropertyName);
    }

    #[test]
    fn error_code_2_invalid_request_id() {
        let json =
            r#"{"code":2,"msg":"Invalid request: request ID must be an unsigned integer","id":1}"#;
        let payload: WsErrorPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.classify(), WsError::InvalidRequestId);
    }

    #[test]
    fn error_code_2_unknown_method() {
        let json = r#"{"code":2,"msg":"Invalid request: unknown variant `FOOBAR`, expected one of `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_PROPERTY`, `GET_PROPERTY` at line 1 column 28","id":1}"#;
        let payload: WsErrorPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.classify(), WsError::UnknownMethod);
    }

    #[test]
    fn error_code_2_too_many_parameters() {
        let json = r#"{"code":2,"msg":"Invalid request: too many parameters","id":1}"#;
        let payload: WsErrorPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.classify(), WsError::TooManyParameters);
    }

    #[test]
    fn error_code_2_missing_field() {
        let json = r#"{"code":2,"msg":"Invalid request: missing field `method` at line 1 column 73","id":1}"#;
        let payload: WsErrorPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.classify(), WsError::MissingField("method".into()));
    }

    #[test]
    fn error_code_2_missing_field_unknown_name() {
        let json = r#"{"code":2,"msg":"missing field `foobar`","id":1}"#;
        let payload: WsErrorPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.classify(), WsError::MissingField("foobar".into()));
    }

    #[test]
    fn error_code_2_unrecognized_msg_returns_unknown() {
        let payload = WsErrorPayload {
            code: 2,
            msg: "some weird error",
        };
        let err = payload.classify();
        assert_eq!(
            err,
            WsError::Unknown {
                code: 2,
                msg: "some weird error".into()
            }
        );
    }

    #[test]
    fn error_unknown_code() {
        let payload = WsErrorPayload {
            code: 99,
            msg: "custom error",
        };
        let err = payload.classify();
        assert_eq!(
            err,
            WsError::Unknown {
                code: 99,
                msg: "custom error".into()
            }
        );
    }

    #[test]
    fn error_negative_code() {
        let payload = WsErrorPayload {
            code: -1,
            msg: "negative",
        };
        let err = payload.classify();
        assert_eq!(
            err,
            WsError::Unknown {
                code: -1,
                msg: "negative".into()
            }
        );
    }

    #[test]
    fn error_deserialized_from_inline_json() {
        let raw = r#"{"code":0,"msg":"Unknown property","id":null}"#;
        let payload: WsErrorPayload = serde_json::from_str(raw).unwrap();
        assert_eq!(payload.code, 0);
        assert_eq!(payload.msg, "Unknown property");
    }

    #[test]
    fn error_classify_after_deserialize_from_str() {
        let raw = r#"{"code":2,"msg":"too many parameters","id":1}"#;
        let payload: WsErrorPayload = serde_json::from_str(raw).unwrap();
        assert_eq!(payload.classify(), WsError::TooManyParameters);
    }

    #[test]
    fn error_zero_copy_msg_borrows() {
        let input = r#"{"code":2,"msg":"too many parameters","id":1}"#.to_owned();
        let payload: WsErrorPayload = serde_json::from_str(&input).unwrap();

        let ptr = payload.msg.as_ptr() as usize;
        let base = input.as_ptr() as usize;
        let limit = base + input.len();
        assert!(
            ptr >= base && ptr < limit,
            "msg ptr {ptr:#x} outside [{base:#x}, {limit:#x})"
        );
    }

    #[test]
    fn display_unknown_property() {
        assert_eq!(
            WsError::UnknownProperty.to_string(),
            "unknown property in SET_PROPERTY / GET_PROPERTY"
        );
    }

    #[test]
    fn display_invalid_value_type() {
        assert_eq!(
            WsError::InvalidValueType.to_string(),
            "invalid value type: expected boolean"
        );
    }

    #[test]
    fn display_missing_field() {
        let err = WsError::MissingField("symbol".into());
        assert_eq!(err.to_string(), "missing field `symbol`");
    }

    #[test]
    fn display_unknown_code() {
        let err = WsError::Unknown {
            code: 42,
            msg: "something broke".into(),
        };
        assert_eq!(err.to_string(), "[42] something broke");
    }

    #[test]
    fn malformed_ticker_returns_error() {
        let result: Result<TickerPayload, _> = serde_json::from_str("this is not json");
        assert!(result.is_err());
    }

    #[test]
    fn missing_required_field_returns_error() {
        let result: Result<TradePayload, _> = serde_json::from_str(r#"{"e":"trade"}"#);
        assert!(result.is_err());
    }
}

impl fmt::Display for WsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownProperty => {
                write!(f, "unknown property in SET_PROPERTY / GET_PROPERTY")
            }
            Self::InvalidValueType => {
                write!(f, "invalid value type: expected boolean")
            }
            Self::InvalidPropertyName => {
                write!(f, "property name must be a string")
            }
            Self::InvalidRequestId => {
                write!(f, "request ID must be an unsigned integer")
            }
            Self::UnknownMethod => {
                write!(f, "unknown method — expected SUBSCRIBE, UNSUBSCRIBE, LIST_SUBSCRIPTIONS, SET_PROPERTY, or GET_PROPERTY")
            }
            Self::TooManyParameters => {
                write!(f, "too many parameters")
            }
            Self::MissingField(field) => {
                write!(f, "missing field `{field}`")
            }
            Self::InvalidJson => {
                write!(f, "invalid JSON")
            }
            Self::Unknown { code, msg } => {
                write!(f, "[{code}] {msg}")
            }
        }
    }
}
