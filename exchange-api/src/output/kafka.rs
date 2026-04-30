// Kafka producer output.
// TODO: Implement StreamData → Kafka topic production.

#[cfg(feature = "kafka")]
#[derive(Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic_prefix: String,
    // TODO: add TLS, SASL, schema registry as needed
}
