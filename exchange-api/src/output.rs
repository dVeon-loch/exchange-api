use std::time::Duration;

use futures::StreamExt;
use tokio::{
    fs::File,
    io::{AsyncWriteExt as _, BufWriter},
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::interval,
};
use tokio_stream::wrappers::BroadcastStream;

use crate::StreamData;

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "redis")]
pub mod redis;

/// Configuration for an output sink.
/// Will need better abstraction when there are more than two outputs.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct OutputConfig {
    #[cfg(feature = "kafka")]
    pub kafka: Option<kafka::KafkaConfig>,
    #[cfg(feature = "redis")]
    pub redis: Option<redis::RedisConfig>,
    pub file: Option<FileConfig>,
    pub custom_channel: Option<broadcast::Sender<StreamData>>,
}

pub struct OutputsSink {
    sink: broadcast::Sender<StreamData>,
}

impl OutputsSink {
    pub fn new(sender: broadcast::Sender<StreamData>) -> Self {
        Self { sink: sender }
    }
    pub fn route_to_sinks(
        &self,
        data: StreamData,
    ) -> Result<usize, broadcast::error::SendError<StreamData>> {
        self.sink.send(data)
    }
}

#[derive(Clone, Debug, Default)]
pub struct FileConfig {
    _file_path: String,
}

#[derive(Debug)]
pub struct FileOutput {
    _config: FileConfig,
    pub recv_handle: JoinHandle<Result<(), crate::Error>>,
}

impl FileOutput {
    pub fn new(
        config: &FileConfig,
        broadcast_recv: tokio::sync::broadcast::Receiver<StreamData>,
    ) -> Self {
        Self {
            _config: config.clone(),
            recv_handle: tokio::spawn(async move {
                Self::start_recv(BroadcastStream::new(broadcast_recv)).await
            }),
        }
    }

    pub async fn start_recv(recv_stream: BroadcastStream<StreamData>) -> Result<(), crate::Error> {
        let (tx, rx) = mpsc::channel::<StreamData>(200);

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
                        if let Err(e) = file.write_all(data.to_string_pretty().as_bytes()).await {
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
        let mut stream = Box::pin(recv_stream);
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
        writer_handle
            .await
            .map_err(|join_err| {
                tracing::error!(error=%join_err, "Error joining BufWriter handle");
                join_err
            })?
            .map_err(|err| {
                tracing::error!(error=%err,"FileOutput task returned an error after joining");
                err
            })?;

        Ok(())
    }
}

pub struct ChannelOutput {
    pub recv_handle: JoinHandle<Result<(), crate::Error>>,
}

impl ChannelOutput {
    pub fn new(
        custom_broadcast_sender: broadcast::Sender<StreamData>,
        broadcast_recv: tokio::sync::broadcast::Receiver<StreamData>,
    ) -> Self {
        Self {
            recv_handle: tokio::spawn(async move {
                Self::start_recv(
                    custom_broadcast_sender,
                    BroadcastStream::new(broadcast_recv),
                )
                .await
            }),
        }
    }

    pub async fn start_recv(
        broadcast_sender: broadcast::Sender<StreamData>,
        recv_stream: BroadcastStream<StreamData>,
    ) -> Result<(), crate::Error> {
        // Forward broadcast items to user-provided broadcast sender
        let mut stream = Box::pin(recv_stream);
        while let Some(result) = StreamExt::next(&mut stream).await {
            match result {
                Ok(data) => {
                    if let Err(e) = broadcast_sender.send(data) {
                        tracing::error!(error=%e, "Error sending data to user-provided broadcast channel");
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
}
