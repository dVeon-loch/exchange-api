use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::fmt;

/// Combined stream wrapper from Binance WebSocket.
///
/// Binance wraps individual stream events inside a JSON object with the format
/// `{"stream":"<streamName>","data":<rawPayload>}`.  This intermediate struct
/// captures both fields so we can dispatch to the correct typed payload based
/// on the stream name without intermediate `serde_json::Value` allocations.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct CombinedStreamRaw {
    pub stream: String,
    pub data: Box<RawValue>,
}

#[allow(dead_code)]
impl CombinedStreamRaw {
    /// Parse the raw inner payload into the correct typed variant.
    pub fn parse(&self) -> Result<CombinedStreamEvent<'_>, exchange_api::Error> {
        let data = self.data.get();
        if self.stream.contains("@ticker") {
            Ok(
                CombinedStreamEvent::Ticker(
                    serde_json::from_str(data)?,
                ),
            )
        } else if self.stream.contains("@trade") {
            Ok(
                CombinedStreamEvent::Trade(
                    serde_json::from_str(data)?,
                ),
            )
        } else if self.stream.contains("@depth") {
            Ok(
                CombinedStreamEvent::OrderBook(
                    serde_json::from_str(data)?,
                ),
            )
        } else {
            Err(exchange_api::Error::Config(format!(
                "unknown stream type: {}",
                self.stream,
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
    Trade(TradePayload<'a>),
    OrderBook(OrderBookPayload<'a>),
}

// Stream name: <symbol>@ticker

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct TickerPayload<'a> {
    #[serde(rename = "e", borrow)]
    pub event_type: Cow<'a, str>,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s", borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(rename = "p", borrow)]
    pub price_change: Cow<'a, str>,
    #[serde(rename = "P", borrow)]
    pub price_change_percent: Cow<'a, str>,
    #[serde(rename = "w", borrow)]
    pub weighted_avg_price: Cow<'a, str>,
    #[serde(rename = "x", borrow)]
    pub first_trade_price: Cow<'a, str>,
    #[serde(rename = "c", borrow)]
    pub last_price: Cow<'a, str>,
    #[serde(rename = "Q", borrow)]
    pub last_quantity: Cow<'a, str>,
    #[serde(rename = "b", borrow)]
    pub best_bid_price: Cow<'a, str>,
    #[serde(rename = "B", borrow)]
    pub best_bid_quantity: Cow<'a, str>,
    #[serde(rename = "a", borrow)]
    pub best_ask_price: Cow<'a, str>,
    #[serde(rename = "A", borrow)]
    pub best_ask_quantity: Cow<'a, str>,
    #[serde(rename = "o", borrow)]
    pub open_price: Cow<'a, str>,
    #[serde(rename = "h", borrow)]
    pub high_price: Cow<'a, str>,
    #[serde(rename = "l", borrow)]
    pub low_price: Cow<'a, str>,
    #[serde(rename = "v", borrow)]
    pub base_volume: Cow<'a, str>,
    #[serde(rename = "q", borrow)]
    pub quote_volume: Cow<'a, str>,
    #[serde(rename = "O")]
    pub stats_open_time: i64,
    #[serde(rename = "C")]
    pub stats_close_time: i64,
    #[serde(rename = "F")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "n")]
    pub total_trades: i64,
}

// Stream name: <symbol>@trade

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct TradePayload<'a> {
    #[serde(rename = "e", borrow)]
    pub event_type: Cow<'a, str>,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s", borrow)]
    pub symbol: Cow<'a, str>,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "p", borrow)]
    pub price: Cow<'a, str>,
    #[serde(rename = "q", borrow)]
    pub quantity: Cow<'a, str>,
    #[serde(rename = "T")]
    pub trade_time: i64,
    #[serde(rename = "m")]
    pub is_buyer_market_maker: bool,
    #[serde(rename = "M")]
    pub ignore: bool,
}

// Stream name: <symbol>@depth<levels>@100ms

/// A single price level — deserialized from a JSON array `[price, quantity]`.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct PriceLevel<'a>(
    #[serde(borrow)] pub Cow<'a, str>,
    #[serde(borrow)] pub Cow<'a, str>,
);

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct OrderBookPayload<'a> {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,
    #[serde(borrow)]
    pub bids: Vec<PriceLevel<'a>>,
    #[serde(borrow)]
    pub asks: Vec<PriceLevel<'a>>,
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
                    let field = self
                        .msg
                        .split('`')
                        .nth(1)
                        .unwrap_or("?")
                        .to_string();
                    WsError::MissingField(field)
                } else {
                    WsError::Unknown { code: 2, msg: self.msg.to_string() }
                }
            }
            3 => WsError::InvalidJson,
            code => WsError::Unknown { code, msg: self.msg.to_string() },
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

    const TICKER_JSON: &str = r#"{
        "e": "24hrTicker",
        "E": 1672515782136,
        "s": "BNBBTC",
        "p": "0.0015",
        "P": "250.00",
        "w": "0.0018",
        "x": "0.0009",
        "c": "0.0025",
        "Q": "10",
        "b": "0.0024",
        "B": "10",
        "a": "0.0026",
        "A": "100",
        "o": "0.0010",
        "h": "0.0025",
        "l": "0.0010",
        "v": "10000",
        "q": "18",
        "O": 0,
        "C": 86400000,
        "F": 0,
        "L": 18150,
        "n": 18151
    }"#;

    #[test]
    fn ticker_deserializes_all_fields() {
        let p: TickerPayload = serde_json::from_str(TICKER_JSON).unwrap();
        assert_eq!(p.event_type, "24hrTicker");
        assert_eq!(p.event_time, 1672515782136);
        assert_eq!(p.symbol, "BNBBTC");
        assert_eq!(p.price_change, "0.0015");
        assert_eq!(p.price_change_percent, "250.00");
        assert_eq!(p.weighted_avg_price, "0.0018");
        assert_eq!(p.first_trade_price, "0.0009");
        assert_eq!(p.last_price, "0.0025");
        assert_eq!(p.last_quantity, "10");
        assert_eq!(p.best_bid_price, "0.0024");
        assert_eq!(p.best_bid_quantity, "10");
        assert_eq!(p.best_ask_price, "0.0026");
        assert_eq!(p.best_ask_quantity, "100");
        assert_eq!(p.open_price, "0.0010");
        assert_eq!(p.high_price, "0.0025");
        assert_eq!(p.low_price, "0.0010");
        assert_eq!(p.base_volume, "10000");
        assert_eq!(p.quote_volume, "18");
        assert_eq!(p.stats_open_time, 0);
        assert_eq!(p.stats_close_time, 86400000);
        assert_eq!(p.first_trade_id, 0);
        assert_eq!(p.last_trade_id, 18150);
        assert_eq!(p.total_trades, 18151);
    }

    #[test]
    fn ticker_zero_copy_borrows() {
        let input = TICKER_JSON.to_owned();
        let p: TickerPayload = serde_json::from_str(&input).unwrap();

        let base = input.as_ptr() as usize;
        let limit = base + input.len();

        // Every Cow field should borrow from the input buffer.
        fn assert_borrowed<'a>(cow: &Cow<'a, str>, base: usize, limit: usize, label: &str) {
            match cow {
                Cow::Borrowed(s) => {
                    let ptr = s.as_ptr() as usize;
                    assert!(ptr >= base && ptr < limit, "{label} ptr {ptr:#x} outside [{base:#x}, {limit:#x})");
                }
                _ => panic!("{label} was not borrowed"),
            }
        }

        assert_borrowed(&p.event_type, base, limit, "event_type");
        assert_borrowed(&p.symbol, base, limit, "symbol");
        assert_borrowed(&p.price_change, base, limit, "price_change");
        assert_borrowed(&p.last_price, base, limit, "last_price");
        assert_borrowed(&p.best_bid_price, base, limit, "best_bid_price");
        assert_borrowed(&p.best_ask_price, base, limit, "best_ask_price");
    }

    const TRADE_JSON: &str = r#"{
        "e": "trade",
        "E": 1672515782136,
        "s": "BNBBTC",
        "t": 12345,
        "p": "0.001",
        "q": "100",
        "T": 1672515782136,
        "m": true,
        "M": true
    }"#;

    #[test]
    fn trade_deserializes_all_fields() {
        let p: TradePayload = serde_json::from_str(TRADE_JSON).unwrap();
        assert_eq!(p.event_type, "trade");
        assert_eq!(p.event_time, 1672515782136);
        assert_eq!(p.symbol, "BNBBTC");
        assert_eq!(p.trade_id, 12345);
        assert_eq!(p.price, "0.001");
        assert_eq!(p.quantity, "100");
        assert_eq!(p.trade_time, 1672515782136);
        assert!(p.is_buyer_market_maker);
        assert!(p.ignore);
    }

    #[test]
    fn trade_m_is_buyer_market_maker() {
        let json = r#"{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"1","T":1,"m":false,"M":false}"#;
        let p: TradePayload = serde_json::from_str(json).unwrap();
        assert!(!p.is_buyer_market_maker);
    }

    #[test]
    fn trade_zero_copy_borrows() {
        let input = TRADE_JSON.to_owned();
        let p: TradePayload = serde_json::from_str(&input).unwrap();

        let base = input.as_ptr() as usize;
        let limit = base + input.len();

        match &p.price {
            Cow::Borrowed(s) => {
                let ptr = s.as_ptr() as usize;
                assert!(ptr >= base && ptr < limit, "price ptr {ptr:#x} outside range");
            }
            _ => panic!("price was not borrowed"),
        }
        match &p.quantity {
            Cow::Borrowed(s) => {
                let ptr = s.as_ptr() as usize;
                assert!(ptr >= base && ptr < limit, "quantity ptr {ptr:#x} outside range");
            }
            _ => panic!("quantity was not borrowed"),
        }
    }

    const ORDERBOOK_JSON: &str = r#"{
        "lastUpdateId": 160,
        "bids": [["0.0024", "10"], ["0.0023", "5"]],
        "asks": [["0.0026", "100"]]
    }"#;

    #[test]
    fn orderbook_deserializes_all_fields() {
        let p: OrderBookPayload = serde_json::from_str(ORDERBOOK_JSON).unwrap();
        assert_eq!(p.last_update_id, 160);
        assert_eq!(p.bids.len(), 2);
        assert_eq!(p.asks.len(), 1);

        assert_eq!(p.bids[0].0, "0.0024");
        assert_eq!(p.bids[0].1, "10");
        assert_eq!(p.bids[1].0, "0.0023");
        assert_eq!(p.bids[1].1, "5");

        assert_eq!(p.asks[0].0, "0.0026");
        assert_eq!(p.asks[0].1, "100");
    }

    #[test]
    fn orderbook_empty_arrays() {
        let json = r#"{"lastUpdateId":0,"bids":[],"asks":[]}"#;
        let p: OrderBookPayload = serde_json::from_str(json).unwrap();
        assert!(p.bids.is_empty());
        assert!(p.asks.is_empty());
    }

    #[test]
    fn orderbook_zero_copy_borrows() {
        let input = ORDERBOOK_JSON.to_owned();
        let p: OrderBookPayload = serde_json::from_str(&input).unwrap();

        let base = input.as_ptr() as usize;
        let limit = base + input.len();

        #[allow(clippy::needless_range_loop)]
        for i in 0..p.bids.len() {
            let cow0 = &p.bids[i].0;
            let cow1 = &p.bids[i].1;
            match cow0 {
                Cow::Borrowed(s) => {
                    let ptr = s.as_ptr() as usize;
                    assert!(ptr >= base && ptr < limit,
                        "bids[{i}].0 ptr {ptr:#x} outside range");
                }
                _ => panic!("bids[{i}].0 was not borrowed"),
            }
            match cow1 {
                Cow::Borrowed(s) => {
                    let ptr = s.as_ptr() as usize;
                    assert!(ptr >= base && ptr < limit,
                        "bids[{i}].1 ptr {ptr:#x} outside range");
                }
                _ => panic!("bids[{i}].1 was not borrowed"),
            }
        }
    }

    #[test]
    fn error_code_0_unknown_property() {
        let payload = WsErrorPayload { code: 0, msg: "Unknown property" };
        assert_eq!(payload.classify(), WsError::UnknownProperty);
    }

    #[test]
    fn error_code_1_invalid_value_type() {
        let payload = WsErrorPayload { code: 1, msg: "Invalid value type: expected Boolean" };
        assert_eq!(payload.classify(), WsError::InvalidValueType);
    }

    #[test]
    fn error_code_3_invalid_json() {
        let payload = WsErrorPayload { code: 3, msg: "Invalid JSON: expected value at line 1 column 28" };
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
        let json = r#"{"code":2,"msg":"Invalid request: request ID must be an unsigned integer","id":1}"#;
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
        let payload = WsErrorPayload { code: 2, msg: "some weird error" };
        let err = payload.classify();
        assert_eq!(err, WsError::Unknown { code: 2, msg: "some weird error".into() });
    }

    #[test]
    fn error_unknown_code() {
        let payload = WsErrorPayload { code: 99, msg: "custom error" };
        let err = payload.classify();
        assert_eq!(err, WsError::Unknown { code: 99, msg: "custom error".into() });
    }

    #[test]
    fn error_negative_code() {
        let payload = WsErrorPayload { code: -1, msg: "negative" };
        let err = payload.classify();
        assert_eq!(err, WsError::Unknown { code: -1, msg: "negative".into() });
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
        assert!(ptr >= base && ptr < limit, "msg ptr {ptr:#x} outside [{base:#x}, {limit:#x})");
    }

    #[test]
    fn display_unknown_property() {
        assert_eq!(WsError::UnknownProperty.to_string(), "unknown property in SET_PROPERTY / GET_PROPERTY");
    }

    #[test]
    fn display_invalid_value_type() {
        assert_eq!(WsError::InvalidValueType.to_string(), "invalid value type: expected boolean");
    }

    #[test]
    fn display_missing_field() {
        let err = WsError::MissingField("symbol".into());
        assert_eq!(err.to_string(), "missing field `symbol`");
    }

    #[test]
    fn display_unknown_code() {
        let err = WsError::Unknown { code: 42, msg: "something broke".into() };
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
