use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("FIX parse error: {0}")]
    Parse(String),

    #[error("Session error: {0}")]
    Session(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
