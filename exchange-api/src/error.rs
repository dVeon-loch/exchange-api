use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("WebSocket error: {0}")]
    Ws(#[from] ws_proto::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("String UTF8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("Kafka error: {0}")]
    Kafka(String),

    #[cfg(feature = "redis")]
    #[error("Redis error: {0}")]
    Redis(#[from] fred::error::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Exchange error: {0}")]
    Exchange(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Io error: {0}")]
    Io(#[from] io::Error),

    #[cfg(not(target_arch = "wasm32"))]
    #[error("Thread join error: {0}")]
    ThreadJoin(#[from] tokio::task::JoinError),
}

#[cfg(target_arch = "wasm32")]
impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Transport(e.to_string())
    }
}
