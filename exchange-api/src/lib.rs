pub mod error;
pub mod runtime;
pub mod traits;
pub mod types;

#[cfg(not(target_arch = "wasm32"))]
pub mod builder;
#[cfg(not(target_arch = "wasm32"))]
pub mod http;
#[cfg(not(target_arch = "wasm32"))]
pub mod output;

pub use error::Error;

#[cfg(not(target_arch = "wasm32"))]
pub use builder::ExchangeApiBuilder;
#[cfg(not(target_arch = "wasm32"))]
pub use http::{
    DefaultHttpClient, HttpBackend, HttpClient, HttpMethod, HttpRequest, HttpResponse,
    ReqwestBackend,
};
#[cfg(not(target_arch = "wasm32"))]
pub use runtime::{ExchangeApi, ExchangeApiHandle};

pub use traits::{Exchange, SubscriptionMethod, WsEndpoint};
pub use types::*;

pub mod prelude {
    pub use crate::traits::Exchange;
    pub use crate::types::StreamKind;
    #[cfg(not(target_arch = "wasm32"))]
    pub use crate::{ExchangeApi, ExchangeApiBuilder};
}
