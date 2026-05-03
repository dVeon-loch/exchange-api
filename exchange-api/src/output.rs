use std::{future::Future, sync::Arc, time::Duration};

use futures::{SinkExt, StreamExt};
use tokio::{fs::File, io::{AsyncWriteExt as _, BufWriter}, sync::mpsc, task::{JoinError, JoinHandle}, time::interval};
use tokio_stream::{StreamExt as _, wrappers::BroadcastStream};
use tokio_util::codec::{BytesCodec, Decoder, FramedWrite, LinesCodec};

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "redis")]
pub mod redis;

/// Configuration for an output sink.
/// Will need better abstraction when there are more than two outputs.
#[derive(Clone)]
#[non_exhaustive]
pub struct OutputConfig {
    #[cfg(feature = "kafka")]
    pub kafka: Option<kafka::KafkaConfig>,
    #[cfg(feature = "redis")]
    pub redis: Option<redis::RedisConfig>,
    pub file: Option<FileConfig>,
}

pub struct OutputsSink {
    sink: tokio::sync::broadcast::Sender<String>,
}

impl OutputsSink {
    pub async fn route_to_sinks(&self, data: &str) -> Result<usize, tokio::sync::broadcast::error::SendError<String>> {
        self.sink.send(data.to_owned())
    }
}

#[derive(Clone, Debug)]
pub struct FileConfig {
    file_path: String
}

#[derive(Debug)]
pub struct FileOutput {
    config: FileConfig,
    recv_stream: BroadcastStream<String>,
}

impl FileOutput {
    pub fn new(config: FileConfig, broadcast_recv: tokio::sync::broadcast::Receiver<String>) -> Self {
        Self {
            config,
            recv_stream: BroadcastStream::new(broadcast_recv),
        }
    }

    pub async fn start_recv(self) -> Result<(), crate::Error> {
        let (tx, rx) = mpsc::channel::<String>(200);

        // Spawn writer task that buffers and flushes every 100ms
        let writer_handle = tokio::spawn(async move {
            let f = File::create("exchange_data.txt").await?;
            let mut file = BufWriter::new(f);

            let mut rx = rx;
            static FLUSH_INTERVAL_MILLIS: u64 = 200;
            let mut flush_interval = interval(Duration::from_millis(FLUSH_INTERVAL_MILLIS));

            loop {
                tokio::select! {
                    Some(data) = rx.recv() => {
                        if let Err(e) = file.write_all(data.as_bytes()).await {
                            tracing::error!(error=%e, "Error writing to file");
                            break;
                        }
                    }
                    _ = flush_interval.tick() => {
                        if let Err(e) = file.flush().await {
                            tracing::error!(error=%e, "Error flushing buffered writer");
                        }
                    }
                }
            }

            Ok::<(), crate::Error>(())
        });

        // Forward broadcast items to writer task
        let mut stream = Box::pin(self.recv_stream);
        while let Some(result) = StreamExt::next(&mut stream).await {
            match result {
                Ok(data) => {
                    if let Err(e) = tx.send(data).await {
                        tracing::error!(error=%e, "Writer task died");
                        break;
                    }
                }
                Err(err) => {
                    tracing::warn!(error=%err, "Broadcast receiver error");
                }
            }
        }

        // Flush final writes
        drop(tx);
        writer_handle.await?;

        Ok(())
    }
}
