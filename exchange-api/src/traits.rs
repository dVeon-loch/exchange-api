use async_trait::async_trait;

use crate::error::Error;
use crate::runtime::ExchangeName;
use crate::types::{StreamData, StreamKind, UpdateRate};
use crate::SymbolList;

/// An exchange implementation.
///
/// Each exchange crate implements this trait to provide the WebSocket URL,
/// subscription messages, and message parsers. The `exchange-api` runtime
/// handles connection lifecycle, reconnection, and output routing.
#[async_trait]
pub trait Exchange: Send + Sync + 'static {
    /// Exchange name enum
    fn name(&self) -> ExchangeName;

    /// WebSocket endpoint URL for the given symbol and streams.
    /// The exchange may use `update_rate` to select protocol-level rate parameters.
    fn ws_url(&self, symbols: &[String], streams: &[StreamKind], update_rate: Option<UpdateRate>) -> String;

    /// Subscription messages to send after WebSocket connect.
    fn subscriptions(&self, symbols: &[&str], streams: &[StreamKind]) -> Vec<String>;

    /// Parse a raw WebSocket text message into generic stream data.
    ///
    /// Returns `None` if the message is not a data event (e.g. a subscription
    /// acknowledgement or heartbeat).
    fn parse_stream(&self, raw: &str) -> Result<Option<StreamData>, Error>;

    /// Retrieve a list of available symbols from the exchange
    async fn fetch_symbol_list(&self) -> Result<SymbolList, Error>;
}
