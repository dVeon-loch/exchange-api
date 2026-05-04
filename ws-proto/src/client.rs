//! High-level WebSocket client with automatic reconnect.

use crate::config::WsConfig;
use crate::error::Error;
use crate::reconnect;
use crate::stream::{WsMessage, WsStream};

/// A generic WebSocket client with built-in reconnect.
///
/// `WsClient` wraps the underlying [`WsStream`] and provides automatic
/// reconnection with exponential backoff when the connection drops.
pub struct WsClient {
    config: WsConfig,
    stream: Option<WsStream>,
}

impl WsClient {
    /// Connect to the configured endpoint.
    ///
    /// On failure, retries according to the configured `ReconnectConfig`.
    pub async fn connect(config: WsConfig) -> Result<Self, Error> {
        let reconnect = config.reconnect.clone();
        let stream = reconnect::retry_with_backoff(reconnect, |attempt| {
            let url = config.url.clone();
            let headers = config.headers.clone();
            let ping_interval = config.ping_interval;
            let pong_timeout = config.pong_timeout;
            async move {
                tracing::info!(%url, attempt, "connecting");
                let stream = WsStream::connect(&url, &headers, ping_interval, pong_timeout).await?;
                tracing::info!(%url, "connected");
                Ok(stream)
            }
        })
        .await?
        .into();

        Ok(Self {
            config,
            stream: Some(stream),
        })
    }

    /// Send a message on the current connection.
    ///
    /// Returns an error if the connection is closed.
    pub async fn send(&mut self, msg: WsMessage) -> Result<(), Error> {
        match &mut self.stream {
            Some(stream) => stream.send(msg).await,
            None => Err(Error::Closed("not connected".into())),
        }
    }

    /// Receive the next message from the current connection.
    ///
    /// Returns `None` when the connection is cleanly closed and reconnect
    /// is not configured, or when all reconnect attempts are exhausted.
    pub async fn recv(&mut self) -> Result<Option<WsMessage>, Error> {
        loop {
            match &mut self.stream {
                Some(stream) => match stream.recv().await {
                    Ok(Some(msg)) => return Ok(Some(msg)),
                    Ok(None) => {
                        // Clean close — attempt reconnect
                        tracing::warn!("connection closed cleanly");
                        self.stream.take();
                        if let Some(new_stream) = self.try_reconnect().await? {
                            self.stream = Some(new_stream);
                            continue;
                        }
                        return Ok(None);
                    }
                    Err(e) => {
                        tracing::warn!(%e, "connection error");
                        self.stream.take();
                        if let Some(new_stream) = self.try_reconnect().await? {
                            self.stream = Some(new_stream);
                            continue;
                        }
                        return Err(e);
                    }
                },
                None => {
                    if let Some(new_stream) = self.try_reconnect().await? {
                        self.stream = Some(new_stream);
                        continue;
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Attempt to reconnect if configured, returning a new stream.
    async fn try_reconnect(&mut self) -> Result<Option<WsStream>, Error> {
        let config = self.config.reconnect.clone();
        let url = self.config.url.clone();
        let headers = self.config.headers.clone();
        let ping_interval = self.config.ping_interval;
        let pong_timeout = self.config.pong_timeout;

        if config.max_retries == 0 {
            return Ok(None);
        }

        reconnect::retry_with_backoff(config, |attempt| {
            let url = url.clone();
            let headers = headers.clone();
            async move {
                tracing::info!(%url, attempt, "reconnecting");
                let stream = WsStream::connect(&url, &headers, ping_interval, pong_timeout).await?;
                tracing::info!(%url, "reconnected");
                Ok(stream)
            }
        })
        .await
        .map(Some)
    }

    /// Send a ping on the current connection.
    pub async fn ping(&mut self) -> Result<(), Error> {
        match &mut self.stream {
            Some(stream) => stream.ping().await,
            None => Err(Error::Closed("not connected".into())),
        }
    }

    /// Gracefully close the connection.
    pub async fn close(mut self) -> Result<(), Error> {
        if let Some(stream) = self.stream.take() {
            stream.close().await?;
        }
        Ok(())
    }

    /// Returns true if the client is currently connected.
    pub fn is_connected(&self) -> bool {
        self.stream.is_some() && !self.stream.as_ref().is_some_and(|s| s.is_closed())
    }
}
