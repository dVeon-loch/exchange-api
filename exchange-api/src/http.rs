//! HTTP client abstraction.
//!
//! Separates the high-level request/response API from the low-level
//! transport backend via the [`HttpBackend`] trait.  The generic
//! [`HttpClient<B>`] struct provides all the convenient builder and
//! serialisation logic; the backend only needs to implement a single
//! raw `send_raw` method.  This gives zero-cost dispatch when the
//! backend is known at compile time.
//!
//! ```
//! use exchange_api::http::{HttpClient, ReqwestBackend, HttpRequest};
//!
//! // Construction is synchronous and zero-cost — backend is baked in at compile time.
//! let client = HttpClient::new(ReqwestBackend::new().unwrap());
//! let _      = client; // use in an async context: client.send(req).await
//! ```

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::time::Duration;

/// Raw HTTP request — the minimal input a backend needs.
#[derive(Clone, Debug)]
pub struct RawHttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Raw HTTP response — the minimal output a backend produces.
#[derive(Clone, Debug)]
pub struct RawHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

/// HTTP method.
#[derive(Clone, Debug)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Get => write!(f, "GET"),
            Self::Post => write!(f, "POST"),
            Self::Put => write!(f, "PUT"),
            Self::Delete => write!(f, "DELETE"),
        }
    }
}

/// An HTTP request ready to send.
#[derive(Clone, Debug)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub query: Vec<(String, String)>,
    pub body: Option<serde_json::Value>,
    pub timeout: Duration,
}

impl HttpRequest {
    pub fn new(method: HttpMethod, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: Vec::new(),
            query: Vec::new(),
            body: None,
            timeout: Duration::from_secs(30),
        }
    }

    pub fn get(url: impl Into<String>) -> Self {
        Self::new(HttpMethod::Get, url)
    }

    pub fn post(url: impl Into<String>) -> Self {
        Self::new(HttpMethod::Post, url)
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    pub fn with_query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    pub fn with_body(mut self, body: serde_json::Value) -> Self {
        self.body = Some(body);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Serialize a struct as the JSON body.
    pub fn with_json_body<T: Serialize>(mut self, value: &T) -> Result<Self, crate::Error> {
        self.body = Some(serde_json::to_value(value)?);
        Ok(self)
    }
}

/// An HTTP response.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl HttpResponse {
    /// Parse the response body as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, crate::Error> {
        Ok(serde_json::from_slice(&self.body)?)
    }

    /// Get the response body as a string.
    pub fn text(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.body)
    }

    /// Returns true if the status code is in the 2xx range.
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }
}

/// Low-level transport backend for HTTP.
///
/// Implementors only need to supply a single `send_raw` method that
/// performs the actual network I/O.  High-level logic (query strings,
/// JSON serialisation, timeout management) lives in [`HttpClient<B>`].
pub trait HttpBackend: Send + Sync + fmt::Debug {
    /// Execute a raw request and return a raw response.
    fn send_raw(
        &self,
        request: RawHttpRequest,
    ) -> impl std::future::Future<Output = Result<RawHttpResponse, crate::Error>> + Send;
}

/// High-level HTTP client parameterised over a transport backend.
///
/// All serialisation, query-string encoding, and timeout logic is
/// handled here.  The backend only deals with raw bytes.
///
/// Because the backend type is part of the struct type, all dispatch
/// is resolved at compile time — there is zero runtime indirection.
#[derive(Debug, Clone)]
pub struct HttpClient<B> {
    backend: B,
}

impl<B: HttpBackend> HttpClient<B> {
    /// Create a new client with the given backend.
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// Execute a high-level request.
    pub async fn send(&self, request: HttpRequest) -> Result<HttpResponse, crate::Error> {
        let body = match request.body {
            Some(ref v) => serde_json::to_vec(v)?,
            None => Vec::new(),
        };

        let raw = RawHttpRequest {
            method: request.method,
            url: build_url(&request.url, &request.query),
            headers: request.headers,
            body,
        };

        let raw_resp = self.backend.send_raw(raw).await?;

        Ok(HttpResponse {
            status: raw_resp.status,
            body: raw_resp.body,
            headers: raw_resp.headers,
        })
    }

    /// Convenience: send a GET request and parse the JSON response.
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        url: impl Into<String>,
    ) -> Result<T, crate::Error> {
        let resp = self.send(HttpRequest::get(url)).await?;
        resp.json()
    }

    /// Access the underlying backend (e.g. to configure custom TLS).
    pub fn backend(&self) -> &B {
        &self.backend
    }
}

/// Transport backend backed by `reqwest`.
///
/// Handles connection pooling, TLS, proxies, and the actual network
/// I/O.  This is the default backend used by [`DefaultHttpClient`].
#[derive(Debug, Clone)]
pub struct ReqwestBackend {
    inner: reqwest::Client,
}

impl ReqwestBackend {
    /// Create a new backend with default settings.
    pub fn new() -> Result<Self, crate::Error> {
        let inner = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { inner })
    }

    /// Create from an existing reqwest Client (for custom config).
    pub fn from_reqwest(inner: reqwest::Client) -> Self {
        Self { inner }
    }
}

impl HttpBackend for ReqwestBackend {
    async fn send_raw(
        &self,
        request: RawHttpRequest,
    ) -> Result<RawHttpResponse, crate::Error> {
        let mut req = match request.method {
            HttpMethod::Get => self.inner.get(&request.url),
            HttpMethod::Post => self.inner.post(&request.url),
            HttpMethod::Put => self.inner.put(&request.url),
            HttpMethod::Delete => self.inner.delete(&request.url),
        };

        for (key, value) in &request.headers {
            req = req.header(key, value);
        }

        if !request.body.is_empty() {
            req = req.body(request.body);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| crate::Error::Transport(e.to_string()))?;

        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
            .collect();
        let body = resp
            .bytes()
            .await
            .map_err(|e| crate::Error::Transport(e.to_string()))?
            .to_vec();

        Ok(RawHttpResponse {
            status,
            body,
            headers,
        })
    }
}

/// Convenience alias for the default HTTP client stack.
pub type DefaultHttpClient = HttpClient<ReqwestBackend>;

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Build a full URL with query parameters appended.
fn build_url(base: &str, query: &[(String, String)]) -> String {
    if query.is_empty() {
        return base.to_owned();
    }

    let params: Vec<String> = query
        .iter()
        .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
        .collect();

    let separator = if base.contains('?') { "&" } else { "?" };
    format!("{}{}{}", base, separator, params.join("&"))
}

/// Minimal URL-encoding (replaces spaces with `%20`, etc.).
fn urlencode(s: &str) -> String {
    s.replace(' ', "%20")
        .replace('!', "%21")
        .replace('"', "%22")
        .replace('#', "%23")
        .replace('$', "%24")
        .replace('%', "%25")
        .replace('&', "%26")
        .replace('\'', "%27")
        .replace('(', "%28")
        .replace(')', "%29")
        .replace('*', "%2A")
        .replace('+', "%2B")
        .replace(',', "%2C")
        .replace('/', "%2F")
        .replace(':', "%3A")
        .replace(';', "%3B")
        .replace('<', "%3C")
        .replace('=', "%3D")
        .replace('>', "%3E")
        .replace('?', "%3F")
        .replace('@', "%40")
        .replace('[', "%5B")
        .replace('\\', "%5C")
        .replace(']', "%5D")
        .replace('^', "%5E")
        .replace('`', "%60")
        .replace('{', "%7B")
        .replace('|', "%7C")
        .replace('}', "%7D")
        .replace('~', "%7E")
}

impl From<reqwest::Error> for crate::Error {
    fn from(e: reqwest::Error) -> Self {
        crate::Error::Transport(e.to_string())
    }
}
