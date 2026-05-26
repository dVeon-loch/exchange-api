//! WebSocket client abstraction for exchange connections.
//!
//! On native targets, provides a full reconnect-capable `WsClient` backed by
//! `tokio-tungstenite`. On `wasm32`, the module compiles cleanly but exposes
//! only configuration and error types — actual WebSocket I/O is handled by the
//! caller via `gloo-net`.

pub mod config;
pub mod error;

#[cfg(not(target_arch = "wasm32"))]
pub mod client;
#[cfg(not(target_arch = "wasm32"))]
pub mod reconnect;
#[cfg(not(target_arch = "wasm32"))]
pub mod stream;

#[cfg(not(target_arch = "wasm32"))]
pub use client::WsClient;
#[cfg(not(target_arch = "wasm32"))]
pub use stream::WsMessage;

pub use config::{ReconnectConfig, WsConfig};
pub use error::Error;
