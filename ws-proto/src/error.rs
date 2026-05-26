use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[cfg(not(target_arch = "wasm32"))]
    #[error("WebSocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Connection error: {0}")]
    Connection(String),

    #[cfg(not(target_arch = "wasm32"))]
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Timed out: {0}")]
    Timeout(String),

    #[error("Closed: {0}")]
    Closed(String),

    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
}
