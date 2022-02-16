//! Building blocks for more advanced consumer chains.
//!
//! # Usage
//! ```no_run
//! # async fn test() {
//! use futures::StreamExt;
//! use rskafka::client::{
//!     ClientBuilder,
//!     consumer::StreamConsumerBuilder,
//! };
//! use std::sync::Arc;
//!
//! // get partition client
//! let connection = "localhost:9093".to_owned();
//! let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
//! let partition_client = Arc::new(
//!     client.partition_client("my_topic", 0).await.unwrap()
//! );
//!
//! // construct stream consumer
//! let mut stream = StreamConsumerBuilder::new(
//!         partition_client,
//!         0,  // start offset
//!     )
//!     .with_max_wait_ms(100)
//!     .build();
//!
//! // consume data
//! let (record, high_water_mark) = stream
//!     .next()
//!     .await
//!     .expect("some records")
//!     .expect("no error");
//! # }
//! ```
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::{BoxFuture, Fuse, FusedFuture, FutureExt};
use futures::Stream;
use pin_project_lite::pin_project;
use tracing::trace;

use crate::{
    client::{error::Result, partition::PartitionClient},
    record::RecordAndOffset,
};

#[derive(Debug)]
pub struct StreamConsumerBuilder {
    client: Arc<dyn FetchClient>,

    start_offset: i64,

    max_wait_ms: i32,

    min_batch_size: i32,

    max_batch_size: i32,
}

impl StreamConsumerBuilder {
    pub fn new(client: Arc<PartitionClient>, start_offset: i64) -> Self {
        Self::new_with_client(client, start_offset)
    }

    /// Internal API for creating with any `dyn FetchClient`
    fn new_with_client(client: Arc<dyn FetchClient>, start_offset: i64) -> Self {
        Self {
            client,
            start_offset,
            // Use same defaults as rdkafka:
            // - <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>
            max_wait_ms: 500,
            min_batch_size: 1,
            max_batch_size: 52428800,
        }
    }

    /// Will wait for at least `min_batch_size` bytes of data
    pub fn with_min_batch_size(self, min_batch_size: i32) -> Self {
        Self {
            min_batch_size,
            ..self
        }
    }

    /// The maximum amount of data to fetch in a single batch
    pub fn with_max_batch_size(self, max_batch_size: i32) -> Self {
        Self {
            max_batch_size,
            ..self
        }
    }

    /// The maximum amount of time to wait for data before returning
    pub fn with_max_wait_ms(self, max_wait_ms: i32) -> Self {
        Self {
            max_wait_ms,
            ..self
        }
    }

    pub fn build(self) -> StreamConsumer {
        StreamConsumer {
            client: self.client,
            max_wait_ms: self.max_wait_ms,
            min_batch_size: self.min_batch_size,
            max_batch_size: self.max_batch_size,
            next_offset: self.start_offset,
            terminated: false,
            last_high_watermark: -1,
            buffer: Default::default(),
            fetch_fut: Fuse::terminated(),
        }
    }
}

type FetchResult = Result<(Vec<RecordAndOffset>, i64)>;

/// A trait wrapper to allow mocking
trait FetchClient: std::fmt::Debug + Send + Sync {
    /// Fetch records.
    ///
    /// Arguments are identical to [`PartitionClient::fetch_records`].
    fn fetch_records(
        &self,
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    ) -> BoxFuture<'_, Result<(Vec<RecordAndOffset>, i64)>>;
}

impl FetchClient for PartitionClient {
    fn fetch_records(
        &self,
        offset: i64,
        bytes: Range<i32>,
        max_wait_ms: i32,
    ) -> BoxFuture<'_, Result<(Vec<RecordAndOffset>, i64)>> {
        Box::pin(self.fetch_records(offset, bytes, max_wait_ms))
    }
}

pin_project! {
    /// Stream consuming data from start offset.
    ///
    /// # Error Handling
    /// If an error is returned by [`fetch_records`](`FetchClient::fetch_records`) then the stream will emit this error
    /// once and will terminate afterwards.
    pub struct StreamConsumer {
        client: Arc<dyn FetchClient>,

        min_batch_size: i32,

        max_batch_size: i32,

        max_wait_ms: i32,

        next_offset: i64,

        terminated: bool,

        last_high_watermark: i64,

        buffer: VecDeque<RecordAndOffset>,

        fetch_fut: Fuse<BoxFuture<'static, FetchResult>>,
    }
}

impl Stream for StreamConsumer {
    type Item = Result<(RecordAndOffset, i64)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            if *this.terminated {
                return Poll::Ready(None);
            }
            if let Some(x) = this.buffer.pop_front() {
                return Poll::Ready(Some(Ok((x, *this.last_high_watermark))));
            }

            if this.fetch_fut.is_terminated() {
                let offset = *this.next_offset;
                let bytes = (*this.min_batch_size)..(*this.max_batch_size);
                let max_wait_ms = *this.max_wait_ms;
                let client = Arc::clone(this.client);

                trace!(offset, "Fetching records at offset");

                *this.fetch_fut = FutureExt::fuse(Box::pin(async move {
                    client.fetch_records(offset, bytes, max_wait_ms).await
                }));
            }

            let data: FetchResult = futures::ready!(this.fetch_fut.poll_unpin(cx));

            match data {
                Ok((mut v, watermark)) => {
                    trace!(
                        high_watermark = watermark,
                        n_records = v.len(),
                        "Received records and a high watermark",
                    );

                    // Sort records by offset in case they aren't in order
                    v.sort_by_key(|x| x.offset);
                    *this.last_high_watermark = watermark;
                    if let Some(x) = v.last() {
                        *this.next_offset = x.offset + 1;
                        this.buffer.extend(v.into_iter())
                    }
                    continue;
                }
                Err(e) => {
                    *this.terminated = true;

                    // report error once
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

impl std::fmt::Debug for StreamConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamConsumer")
            .field("client", &self.client)
            .field("min_batch_size", &self.min_batch_size)
            .field("max_batch_size", &self.max_batch_size)
            .field("max_wait_ms", &self.max_wait_ms)
            .field("next_offset", &self.next_offset)
            .field("terminated", &self.terminated)
            .field("last_high_watermark", &self.last_high_watermark)
            .field("buffer", &self.buffer)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert_matches::assert_matches;
    use futures::{pin_mut, StreamExt};
    use time::OffsetDateTime;
    use tokio::sync::{mpsc, Mutex};

    use crate::{
        client::error::{Error, ProtocolError},
        record::Record,
    };

    use super::*;

    #[derive(Debug)]
    struct MockFetch {
        inner: Arc<Mutex<MockFetchInner>>,
    }

    #[derive(Debug)]
    struct MockFetchInner {
        batch_sizes: Vec<usize>,
        stream: mpsc::Receiver<Record>,
        buffer: Vec<Record>,
    }

    impl MockFetch {
        fn new(stream: mpsc::Receiver<Record>) -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockFetchInner {
                    batch_sizes: vec![],
                    stream,
                    buffer: Default::default(),
                })),
            }
        }

        async fn batch_sizes(&self) -> Vec<usize> {
            self.inner.lock().await.batch_sizes.clone()
        }
    }

    impl FetchClient for MockFetch {
        fn fetch_records(
            &self,
            start_offset: i64,
            bytes: Range<i32>,
            max_wait_ms: i32,
        ) -> BoxFuture<'_, Result<(Vec<RecordAndOffset>, i64)>> {
            let inner = Arc::clone(&self.inner);
            Box::pin(async move {
                println!("MockFetch::fetch_records");
                let mut inner = inner.lock().await;
                println!("MockFetch::fetch_records locked");

                let mut buffer = vec![];
                let mut buffered = 0;

                // Drain input queue
                while let Ok(x) = inner.stream.try_recv() {
                    inner.buffer.push(x)
                }

                for (record_offset, record) in
                    inner.buffer.iter().enumerate().skip(start_offset as usize)
                {
                    let size = record.approximate_size();
                    if size + buffered > bytes.end as usize {
                        assert_ne!(buffered, 0, "record too large");
                        break;
                    }

                    buffer.push(RecordAndOffset {
                        record: record.clone(),
                        offset: record_offset as i64,
                    });
                    buffered += size;
                }

                println!("Waiting up to {} ms for more data", max_wait_ms);

                // Need to wait for more data
                let timeout = tokio::time::sleep(Duration::from_millis(max_wait_ms as u64)).fuse();
                pin_mut!(timeout);

                while buffered < bytes.start as usize && !timeout.is_terminated() {
                    futures::select! {
                        maybe_record = inner.stream.recv().fuse() => match maybe_record {
                            Some(record) => {
                                println!("Received a new record");
                                let size = record.approximate_size();
                                let record_offset = inner.buffer.len() as i64;

                                // Remember record for later
                                inner.buffer.push(record.clone());

                                if record_offset < start_offset {
                                    continue
                                }

                                if size + buffered > bytes.end as usize {
                                    assert_ne!(buffered, 0, "record too large");
                                    break;
                                }

                                buffer.push(RecordAndOffset {
                                    record,
                                    offset: record_offset as i64,
                                });
                                buffered += size
                            }
                            None => break,
                        },
                        _ = timeout => {
                            println!("Timeout receiving records");
                            break
                        },
                    }
                }

                inner.batch_sizes.push(buffer.len());

                Ok((buffer, inner.buffer.len() as i64 - 1))
            })
        }
    }

    #[derive(Debug)]
    struct MockErrFetch {
        inner: Arc<Mutex<Option<Error>>>,
    }

    impl MockErrFetch {
        fn new(e: Error) -> Self {
            Self {
                inner: Arc::new(Mutex::new(Some(e))),
            }
        }
    }

    impl FetchClient for MockErrFetch {
        fn fetch_records(
            &self,
            _offset: i64,
            _bytes: Range<i32>,
            _max_wait_ms: i32,
        ) -> BoxFuture<'_, Result<(Vec<RecordAndOffset>, i64)>> {
            let inner = Arc::clone(&self.inner);
            Box::pin(async move {
                match inner.lock().await.take() {
                    Some(e) => Err(e),
                    None => panic!("EOF"),
                }
            })
        }
    }

    #[tokio::test]
    async fn test_consumer() {
        let record = Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: OffsetDateTime::now_utc(),
        };

        let (sender, receiver) = mpsc::channel(10);
        let consumer = Arc::new(MockFetch::new(receiver));
        let mut stream =
            StreamConsumerBuilder::new_with_client(Arc::<MockFetch>::clone(&consumer), 2)
                .with_max_wait_ms(10)
                .build();

        tokio::time::timeout(Duration::from_micros(1), stream.next())
            .await
            .expect_err("timeout");

        // Write two records, nothing should happen as start offset is 2
        sender.send(record.clone()).await.unwrap();
        sender.send(record.clone()).await.unwrap();

        tokio::time::timeout(Duration::from_micros(10), stream.next())
            .await
            .expect_err("timeout");

        sender.send(record.clone()).await.unwrap();

        let unwrap = |e: Result<Option<Result<_, _>>, _>| e.unwrap().unwrap().unwrap();

        let (record_and_offset, high_watermark) =
            unwrap(tokio::time::timeout(Duration::from_micros(10), stream.next()).await);

        assert_eq!(record_and_offset.offset, 2);
        assert_eq!(high_watermark, 2);

        sender.send(record.clone()).await.unwrap();
        sender.send(record.clone()).await.unwrap();
        sender.send(record.clone()).await.unwrap();

        let (record_and_offset, high_watermark) =
            unwrap(tokio::time::timeout(Duration::from_micros(1), stream.next()).await);
        assert_eq!(record_and_offset.offset, 3);
        assert_eq!(high_watermark, 5);

        let (record_and_offset, high_watermark) =
            tokio::time::timeout(Duration::from_millis(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(record_and_offset.offset, 4);
        assert_eq!(high_watermark, 5);

        let (record_and_offset, high_watermark) =
            tokio::time::timeout(Duration::from_millis(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(record_and_offset.offset, 5);
        assert_eq!(high_watermark, 5);

        let received = consumer.batch_sizes().await;
        assert_eq!(&received, &[1, 3]);
    }

    #[tokio::test]
    async fn test_consumer_timeout() {
        let record = Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: OffsetDateTime::now_utc(),
        };

        let (sender, receiver) = mpsc::channel(10);
        let consumer = Arc::new(MockFetch::new(receiver));

        assert!(consumer.batch_sizes().await.is_empty());

        let mut stream =
            StreamConsumerBuilder::new_with_client(Arc::<MockFetch>::clone(&consumer), 0)
                .with_min_batch_size((record.approximate_size() * 2) as i32)
                .with_max_batch_size((record.approximate_size() * 3) as i32)
                .with_max_wait_ms(1)
                .build();

        // Should return nothing
        tokio::time::timeout(Duration::from_millis(10), stream.next())
            .await
            .expect_err("timeout");

        // Stream might be holding lock, so must poll it whilst trying to extract batch sizes
        // to allow timeouts to be serviced and the lock released
        let received = tokio::select! {
            _ = stream.next() => panic!("stream returned!"),
            x = consumer.batch_sizes() => x,
        };

        // Should have had some requests timeout returning no records
        assert!(!received.is_empty());
        assert!(received.iter().all(|x| *x == 0));

        sender.send(record.clone()).await.unwrap();

        // Should wait for max_wait_ms
        tokio::time::timeout(Duration::from_millis(10), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        sender.send(record.clone()).await.unwrap();
        sender.send(record.clone()).await.unwrap();

        // Should not wait for max_wait_ms
        tokio::time::timeout(Duration::from_micros(10), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // Should not wait for max_wait_ms
        tokio::time::timeout(Duration::from_micros(10), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_consumer_terminate() {
        let e = Error::ServerError(
            ProtocolError::OffsetOutOfRange,
            String::from("offset out of range"),
        );
        let consumer = Arc::new(MockErrFetch::new(e));

        let mut stream = StreamConsumerBuilder::new_with_client(consumer, 0).build();

        let error = stream.next().await.expect("stream not empty").unwrap_err();
        assert_matches!(
            error,
            Error::ServerError(ProtocolError::OffsetOutOfRange, _)
        );

        // stream ends
        assert!(stream.next().await.is_none());
    }
}
