//! WebSocket client abstraction for exchange connections.
//!
//! This crate provides a generic, reconnect-capable WebSocket client
//! built on top of `tokio-tungstenite`. Exchange implementations use
//! [`WsClient`] to connect, send, receive, and handle disconnects
//! without worrying about the underlying transport.
//!
//! # Features
//!
//! - Asynchronous connect with configurable URL and headers
//! - Automatic reconnect with exponential backoff + jitter
//! - Ping/pong keepalive
//! - Graceful close
//! - Message type enum (Text, Binary, Ping, Pong)
//!
//! # Example
//!
//! ```rust,no_run
//! use ws_proto::{WsClient, WsConfig};
//!
//! # async fn example() {
//! let mut client = WsClient::connect(
//!     WsConfig::new("wss://stream.binance.com:9443/ws/btcusdt@trade")
//! ).await.unwrap();
//!
//! while let Some(msg) = client.recv().await.unwrap() {
//!     match msg {
//!         ws_proto::WsMessage::Text(text) => println!("{text}"),
//!         _ => {}
//!     }
//! }
//! # }
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod reconnect;
pub mod stream;

pub use client::WsClient;
pub use config::{ReconnectConfig, WsConfig};
pub use error::Error;
pub use stream::WsMessage;
