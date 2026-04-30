//! Transport layer for FIX sessions.
//!
//! Supports both raw TCP and WebSocket as underlying transports, since
//! some exchanges (e.g. Binance) offer FIX over WebSocket.

// TODO: TCP transport — tokio::net::TcpStream with FIX framing
// TODO: WebSocket transport — tokio-tungstenite with FIX framing
// TODO: Connection pool / reconnect with backoff
