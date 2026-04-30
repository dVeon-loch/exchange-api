//! WebSocket stream wrapper.
//!
//! Wraps a `tokio_tungstenite::WebSocketStream` in our own message type,
//! handling the low-level read/write and ping/pong internally.

use crate::error::Error;
use futures_util::stream::FusedStream;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as TungsteniteMsg;

/// Generic WebSocket message type.
#[derive(Clone, Debug)]
pub enum WsMessage {
    /// UTF-8 text message.
    Text(String),
    /// Binary payload.
    Binary(Vec<u8>),
    /// Ping frame.
    Ping(Vec<u8>),
    /// Pong frame.
    Pong(Vec<u8>),
}

impl From<TungsteniteMsg> for WsMessage {
    fn from(msg: TungsteniteMsg) -> Self {
        match msg {
            TungsteniteMsg::Text(s) => Self::Text(s.to_string()),
            TungsteniteMsg::Binary(data) => Self::Binary(data.to_vec()),
            TungsteniteMsg::Ping(data) => Self::Ping(data.to_vec()),
            TungsteniteMsg::Pong(data) => Self::Pong(data.to_vec()),
            TungsteniteMsg::Frame(_) => unreachable!(),
            TungsteniteMsg::Close(_) => Self::Pong(Vec::new()),
        }
    }
}

/// Wraps a tokio-tungstenite WebSocket stream with ping/pong handling.
/// TODO: Abstract this so that it does not depend on tungstenite types
pub struct WsStream {
    inner: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    _ping_interval: Duration,
    _pong_timeout: Duration,
}

impl WsStream {
    /// Connect to the given URL and return a `WsStream`.
    pub async fn connect(
        url: &str,
        headers: &[(String, String)],
        ping_interval: Duration,
        pong_timeout: Duration,
    ) -> Result<Self, Error> {
        let (inner, _) = if headers.is_empty() {
            tokio_tungstenite::connect_async(url).await?
        } else {
            let mut req = http::Request::builder()
                .uri(url)
                .body(())
                .map_err(|e| Error::InvalidUrl(e.to_string()))?;
            for (key, value) in headers {
                req.headers_mut().insert(
                    http::HeaderName::from_bytes(key.as_bytes())
                        .map_err(|e| Error::InvalidUrl(e.to_string()))?,
                    http::HeaderValue::from_str(value)
                        .map_err(|e| Error::InvalidUrl(e.to_string()))?,
                );
            }
            tokio_tungstenite::connect_async(req).await?
        };

        Ok(Self {
            inner,
            _ping_interval: ping_interval,
            _pong_timeout: pong_timeout,
        })
    }

    /// Send a message on the stream.
    pub async fn send(&mut self, msg: WsMessage) -> Result<(), Error> {
        let tungstenite_msg = match msg {
            WsMessage::Text(s) => TungsteniteMsg::Text(s.into()),
            WsMessage::Binary(data) => TungsteniteMsg::Binary(data.into()),
            WsMessage::Ping(data) => TungsteniteMsg::Ping(data.into()),
            WsMessage::Pong(data) => TungsteniteMsg::Pong(data.into()),
        };
        self.inner.send(tungstenite_msg).await?;
        Ok(())
    }

    /// Receive a message from the stream.
    ///
    /// Returns `None` when the stream is closed cleanly.
    pub async fn recv(&mut self) -> Result<Option<WsMessage>, Error> {
        loop {
            tokio::select! {
                msg = self.inner.next() => {
                    match msg {
                        Some(Ok(TungsteniteMsg::Close(_))) => return Ok(None),
                        Some(Ok(TungsteniteMsg::Ping(data))) => {
                            return Ok(Some(WsMessage::Ping(data.to_vec())));
                        }
                        Some(Ok(other)) => return Ok(Some(other.into())),
                        Some(Err(e)) => return Err(e.into()),
                        None => return Ok(None),
                    }
                }
                _ = tokio::time::sleep(self._pong_timeout) => {
                    continue;
                }
            }
        }
    }

    /// Send a ping.
    pub async fn ping(&mut self) -> Result<(), Error> {
        self.inner
            .send(TungsteniteMsg::Ping(Vec::new().into()))
            .await?;
        Ok(())
    }

    /// Close the WebSocket connection with a normal closure code.
    pub async fn close(mut self) -> Result<(), Error> {
        self.inner.close(None).await?;
        Ok(())
    }

    /// Returns true if the underlying stream is terminated (EOF or error).
    pub fn is_closed(&self) -> bool {
        self.inner.is_terminated()
    }
}
