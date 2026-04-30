// Redis writer output.
// TODO: Implement StreamData → Redis hash + pub/sub writes.

#[cfg(feature = "redis")]
#[derive(Clone)]
pub struct RedisConfig {
    pub url: String,
    // TODO: add TLS, connection pool config as needed
}
