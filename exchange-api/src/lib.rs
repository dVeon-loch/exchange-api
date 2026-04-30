pub mod builder;
pub mod error;
pub mod http;
pub mod output;
pub mod runtime;
pub mod traits;
pub mod types;

pub use builder::ExchangeApiBuilder;
pub use error::Error;
pub use http::{
    DefaultHttpClient, HttpBackend, HttpClient, HttpMethod, HttpRequest, HttpResponse,
    ReqwestBackend,
};
pub use runtime::{ExchangeApi, ExchangeApiHandle};
pub use traits::Exchange;
pub use types::*;

/// Convenience re-exports for downstream users.
///
/// Import with `use exchange_api::prelude::*` to bring traits,
/// key types, and the builder into scope.
pub mod prelude {
    pub use crate::traits::Exchange;
    pub use crate::types::StreamKind;
    pub use crate::{ExchangeApi, ExchangeApiBuilder};
}
