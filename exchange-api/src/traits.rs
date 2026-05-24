use async_trait::async_trait;

use crate::error::Error;
use crate::runtime::ExchangeName;
use crate::types::{StreamData, StreamKind, UpdateRate};
use crate::SymbolList;

/// How an exchange expects subscriptions to be expressed after a WebSocket
/// connection is established.
pub enum SubscriptionMethod {
    /// Subscriptions are encoded in the URL itself — no message is sent after
    /// connect (Binance combined-stream pattern).
    UrlEncoded,
    /// A single JSON message is sent: `{"op":"subscribe","args":[...]}`.
    /// Each string in the vec is one topic arg (Bybit pattern).
    JsonArgs(Vec<String>),
}

/// A single WebSocket connection descriptor returned by [`Exchange::ws_endpoints`].
pub struct WsEndpoint {
    pub url: String,
    pub subscription: SubscriptionMethod,
}

/// An exchange implementation.
///
/// Each exchange crate implements this trait to provide the WebSocket URL,
/// subscription messages, and message parsers. The `exchange-api` runtime
/// handles connection lifecycle, reconnection, and output routing.
#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    /// Exchange name enum
    fn name(&self) -> ExchangeName;

    /// WebSocket endpoints for the given symbols and streams.
    ///
    /// Returns one [`WsEndpoint`] per connection that should be opened. The
    /// runtime spawns an independent task for each endpoint. The exchange may
    /// use `update_rate` to select protocol-level rate parameters.
    fn ws_endpoints(
        &self,
        symbols: &[String],
        streams: &[StreamKind],
        update_rate: Option<UpdateRate>,
    ) -> Vec<WsEndpoint>;

    /// Parse a raw WebSocket text message into zero or more stream events.
    ///
    /// Returns an empty vec for non-data messages (subscription acknowledgements,
    /// heartbeats). Returns multiple items when the exchange batches events (e.g.
    /// Bybit trades).
    fn parse_stream(&self, raw: &str) -> Result<Vec<StreamData>, Error>;

    /// WebSocket ping interval for this exchange.
    ///
    /// Returns `None` to use ws-proto's default (no auto-ping).
    /// Override to enable keepalive pings at the specified interval.
    fn ping_interval(&self) -> Option<std::time::Duration> {
        None
    }

    /// Retrieve a list of available symbols from the exchange
    async fn fetch_symbol_list(&self) -> Result<SymbolList, Error>;
}
