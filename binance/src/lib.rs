#[cfg(feature = "spot")]
pub mod spot;
#[cfg(feature = "futures")]
pub mod futures;

pub mod parsers;

#[cfg(feature = "spot")]
pub use spot::BinanceSpot;
#[cfg(feature = "futures")]
pub use futures::BinanceFuturesUsd;
