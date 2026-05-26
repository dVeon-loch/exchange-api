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
///
/// On native targets the trait requires `Send + Sync` (for multi-threaded tokio).
/// On WASM targets those bounds are dropped — WASM is single-threaded.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
pub trait Exchange: Send + Sync + 'static {
    fn name(&self) -> ExchangeName;

    fn ws_endpoints(
        &self,
        symbols: &[String],
        streams: &[StreamKind],
        update_rate: Option<UpdateRate>,
    ) -> Vec<WsEndpoint>;

    fn parse_stream(&self, raw: &str) -> Result<Vec<StreamData>, Error>;

    fn ping_interval(&self) -> Option<std::time::Duration> {
        None
    }

    async fn fetch_symbol_list(&self) -> Result<SymbolList, Error>;
}

#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
pub trait Exchange: 'static {
    fn name(&self) -> ExchangeName;

    fn ws_endpoints(
        &self,
        symbols: &[String],
        streams: &[StreamKind],
        update_rate: Option<UpdateRate>,
    ) -> Vec<WsEndpoint>;

    fn parse_stream(&self, raw: &str) -> Result<Vec<StreamData>, Error>;

    fn ping_interval(&self) -> Option<std::time::Duration> {
        None
    }

    async fn fetch_symbol_list(&self) -> Result<SymbolList, Error>;
}
