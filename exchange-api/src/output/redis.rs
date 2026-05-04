// Redis writer output.

use std::time::Duration;

use fred::prelude::*;
use tokio::task::JoinHandle;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::StreamData;

#[cfg(feature = "redis")]
#[derive(Clone)]
pub struct RedisConfig {
    pub url: String,
    pub max_trades_per_symbol: usize,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: std::env::var("REDIS_URL")
                .expect("REDIS_URL env var must be set at either compile time or runtime"),
            max_trades_per_symbol: 20,
        }
    }
}

pub struct RedisOutput {
    pub client: fred::clients::Client,
    pub recv_handle: JoinHandle<Result<(), crate::Error>>,
}

impl RedisOutput {
    pub async fn new(
        broadcast_recv: tokio::sync::broadcast::Receiver<StreamData>,
    ) -> Result<Self, crate::Error> {
        Self::with_config(broadcast_recv, RedisConfig::default()).await
    }

    pub async fn with_config(
        broadcast_recv: tokio::sync::broadcast::Receiver<StreamData>,
        config: RedisConfig,
    ) -> Result<Self, crate::Error> {
        let redis_config = Config::from_url(&config.url)?;
        let client = Builder::from_config(redis_config)
            .with_connection_config(|conn_config| {
                conn_config.connection_timeout = Duration::from_secs(5);
                conn_config.tcp = TcpConfig {
                    nodelay: Some(true),
                    ..Default::default()
                };
            })
            .build()?;
        client.init().await?;

        client.on_error(|(error, server)| async move {
            tracing::error!("{:?}: Redis client connection error: {:?}", server, error);
            Ok(())
        });

        let recv_client = client.clone();
        Ok(Self {
            client,
            recv_handle: tokio::spawn(async move {
                Self::start_recv(recv_client, BroadcastStream::new(broadcast_recv), config).await
            }),
        })
    }

    async fn start_recv(
        client: fred::clients::Client,
        recv_stream: BroadcastStream<StreamData>,
        config: RedisConfig,
    ) -> Result<(), crate::Error> {
        let mut stream = Box::pin(recv_stream);
        while let Some(result) = StreamExt::next(&mut stream).await {
            match result {
                Ok(data) => {
                    if let Err(e) = Self::write_data(&client, &data, &config).await {
                        tracing::error!(error=%e, "Error writing data to Redis");
                        continue;
                    }
                }
                Err(err) => {
                    tracing::warn!(error=%err, "Broadcast receiver error");
                }
            }
        }

        Ok(())
    }

    async fn write_data(
        client: &fred::clients::Client,
        data: &StreamData,
        _config: &RedisConfig,
    ) -> Result<(), crate::Error> {
        let (exchange, symbol, data_type) = data.metadata();
        let latest_key = format!(
            "exchange:{}:symbol:{}:latest:{}",
            exchange, symbol, data_type
        );
        let json_value = serde_json::to_string(data)?;

        match data {
            StreamData::Trade(_) => {
                let stream_key = format!("exchange:{}:symbol:{}:trade:stream", exchange, symbol);

                let mut fields: std::collections::HashMap<String, String> =
                    std::collections::HashMap::new();
                fields.insert("data".to_string(), json_value.clone());

                let _: String = client
                    .xadd(stream_key.clone(), false, None, "*", fields)
                    .await?;

                let _: () = client
                    .set(latest_key, json_value.clone(), None, None, false)
                    .await?;

                let _: i64 = client
                    .publish(
                        format!("exchange:{}:symbol:{}:updates", exchange, symbol),
                        json_value,
                    )
                    .await?;
            }
            StreamData::OrderBook(_) | StreamData::Ticker(_) => {
                let _: () = client
                    .set(latest_key.clone(), json_value.clone(), None, None, false)
                    .await?;

                let _: i64 = client
                    .publish(
                        format!("exchange:{}:symbol:{}:updates", exchange, symbol),
                        json_value,
                    )
                    .await?;
            }
        }

        Ok(())
    }
}
