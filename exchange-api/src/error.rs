use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("WebSocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Kafka error: {0}")]
    Kafka(String),

    #[error("Redis error: {0}")]
    Redis(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Exchange error: {0}")]
    Exchange(String),

    #[error("Transport error: {0}")]
    Transport(String),
}
