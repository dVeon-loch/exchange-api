#[cfg(feature = "futures")]
pub mod futures;
#[cfg(feature = "spot")]
pub mod spot;

pub mod local_order_book;
pub mod parsers;

#[cfg(feature = "futures")]
pub use futures::BybitFutures;
use serde::Serialize;
#[cfg(feature = "spot")]
pub use spot::BybitSpot;

#[derive(Serialize)]
pub struct SubscriptionRequest {
    op: &'static str,
    args: Vec<String>,
}

impl SubscriptionRequest {
    pub fn new(subscriptions: Vec<String>) -> Self {
        Self {
            op: "subscribe",
            args: subscriptions,
        }
    }
}
