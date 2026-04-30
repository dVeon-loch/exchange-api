use crate::error::Error;
use crate::types::{StreamData, StreamKind};

/// An exchange implementation.
///
/// Each exchange crate implements this trait to provide the WebSocket URL,
/// subscription messages, and message parsers. The `exchange-api` runtime
/// handles connection lifecycle, reconnection, and output routing.
pub trait Exchange: Send + Sync + 'static {
    /// Human-readable exchange name, e.g. "binance".
    fn name(&self) -> &'static str;

    /// WebSocket endpoint URL for the given symbol and streams.
    fn ws_url(&self, symbols: &[&str], streams: &[StreamKind]) -> String;

    /// Subscription messages to send after WebSocket connect.
    fn subscriptions(&self, symbols: &[&str], streams: &[StreamKind]) -> Vec<String>;

    /// Parse a raw WebSocket text message into generic stream data.
    ///
    /// Returns `None` if the message is not a data event (e.g. a subscription
    /// acknowledgement or heartbeat).
    fn parse(&self, raw: &str) -> Result<Option<StreamData>, Error>;
}
