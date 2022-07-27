//! Building blocks for more advanced consumer chains.
//!
//! # Usage
//! ```no_run
//! # async fn test() {
//! use futures::StreamExt;
//! use rskafka::client::{
//!     ClientBuilder,
//!     consumer::{
//!         StartOffset,
//!         StreamConsumerBuilder,
//!     },
//! };
//! use std::sync::Arc;
//!
//! // get partition client
//! let connection = "localhost:9093".to_owned();
//! let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
//! let partition_client = Arc::new(
//!     client.partition_client("my_topic", 0).unwrap()
//! );
//!
//! // construct stream consumer
//! let mut stream = StreamConsumerBuilder::new(
//!         partition_client,
//!         StartOffset::Earliest,
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
use std::time::Duration;

use futures::future::{BoxFuture, Fuse, FusedFuture, FutureExt};
use futures::Stream;
use pin_project_lite::pin_project;
use tracing::{trace, warn};

use crate::{
    client::{
        error::{Error, ProtocolError, Result},
        partition::PartitionClient,
    },
    record::RecordAndOffset,
};

use super::partition::OffsetAt;

/// At which position shall the stream start.
#[derive(Debug, Clone, Copy)]
pub enum StartOffset {
    /// At the earlist known offset.
    ///
    /// This might be larger than 0 if some records were already deleted due to a retention policy.
    Earliest,

    /// At the latest known offset.
    ///
    /// This is helpful if you only want ot process new data.
    Latest,

    /// At a specific offset.
    ///
    /// Note that specifying an offset that is unknown to the broker will result in a [`Error::ServerError`] with
    /// [`ProtocolError::OffsetOutOfRange`] and the stream will terminate right after the error.
    At(i64),
}

#[derive(Debug)]
pub struct StreamConsumerBuilder {
    client: Arc<dyn FetchClient>,

    start_offset: StartOffset,

    max_wait_ms: i32,

    min_batch_size: i32,

    max_batch_size: i32,
}

impl StreamConsumerBuilder {
    pub fn new(client: Arc<PartitionClient>, start_offset: StartOffset) -> Self {
        Self::new_with_client(client, start_offset)
    }

    /// Internal API for creating with any `dyn FetchClient`
    fn new_with_client(client: Arc<dyn FetchClient>, start_offset: StartOffset) -> Self {
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
            next_offset: None,
            next_backoff: None,
            start_offset: self.start_offset,
            terminated: false,
            last_high_watermark: -1,
            buffer: Default::default(),
            fetch_fut: Fuse::terminated(),
        }
    }
}

struct FetchResultOk {
    records_and_offsets: Vec<RecordAndOffset>,
    watermark: i64,
    used_offset: i64,
}

type FetchResult = Result<FetchResultOk>;

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

    /// Get offset.
    ///
    /// Arguments are identical to [`PartitionClient::get_offset`].
    fn get_offset(&self, at: OffsetAt) -> BoxFuture<'_, Result<i64>>;
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

    fn get_offset(&self, at: OffsetAt) -> BoxFuture<'_, Result<i64>> {
        Box::pin(self.get_offset(at))
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

        start_offset: StartOffset,

        next_offset: Option<i64>,

        next_backoff: Option<Duration>,

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
                let next_offset = *this.next_offset;
                let start_offset = *this.start_offset;
                let bytes = (*this.min_batch_size)..(*this.max_batch_size);
                let max_wait_ms = *this.max_wait_ms;
                let next_backoff = std::mem::take(this.next_backoff);
                let client = Arc::clone(this.client);

                trace!(?start_offset, ?next_offset, "Fetching records at offset");

                *this.fetch_fut = FutureExt::fuse(Box::pin(async move {
                    if let Some(backoff) = next_backoff {
                        tokio::time::sleep(backoff).await;
                    }

                    let offset = match next_offset {
                        Some(x) => x,
                        None => match start_offset {
                            StartOffset::Earliest => client.get_offset(OffsetAt::Earliest).await?,
                            StartOffset::Latest => client.get_offset(OffsetAt::Latest).await?,
                            StartOffset::At(x) => x,
                        },
                    };

                    let (records_and_offsets, watermark) =
                        client.fetch_records(offset, bytes, max_wait_ms).await?;
                    Ok(FetchResultOk {
                        records_and_offsets,
                        watermark,
                        used_offset: offset,
                    })
                }));
            }

            let data: FetchResult = futures::ready!(this.fetch_fut.poll_unpin(cx));

            match (data, *this.start_offset) {
                (Ok(inner), _) => {
                    let FetchResultOk {
                        mut records_and_offsets,
                        watermark,
                        used_offset,
                    } = inner;
                    trace!(
                        high_watermark = watermark,
                        n_records = records_and_offsets.len(),
                        "Received records and a high watermark",
                    );

                    // Remember used offset (might be overwritten if there was any data) so we don't refetch the
                    // earliest / latest offset for every try. Also fetching the latest offset might be racy otherwise,
                    // since we'll never be in a position where the latest one can actually be fetched.
                    *this.next_offset = Some(used_offset);

                    // Sort records by offset in case they aren't in order
                    records_and_offsets.sort_by_key(|x| x.offset);
                    *this.last_high_watermark = watermark;
                    if let Some(x) = records_and_offsets.last() {
                        *this.next_offset = Some(x.offset + 1);
                        this.buffer.extend(records_and_offsets.into_iter())
                    }
                    continue;
                }
                // if we don't have an offset, try again because fetching the offset is racy
                (
                    Err(Error::ServerError {
                        protocol_error: ProtocolError::OffsetOutOfRange,
                        ..
                    }),
                    StartOffset::Earliest | StartOffset::Latest,
                ) => {
                    // wipe offset and try again
                    *this.next_offset = None;

                    // This will only happen if retention / deletions happen after we've asked for the earliest/latest
                    // offset and our "fetch" request. This should be a rather rare event, but if something is horrible
                    // wrong in our cluster (e.g. some actor is spamming "delete" requests) then let's at least backoff
                    // a bit.
                    let backoff_secs = 1;
                    warn!(
                        start_offset=?this.start_offset,
                        backoff_secs,
                        "Records are gone between ListOffsets and Fetch, backoff a bit",
                    );
                    *this.next_backoff = Some(Duration::from_secs(backoff_secs));

                    continue;
                }
                // if we have an offset, terminate the stream
                (Err(e), _) => {
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
        next_err: Option<Error>,
        buffer: Vec<Record>,
        range: (i64, i64),
    }

    impl MockFetch {
        fn new(stream: mpsc::Receiver<Record>, next_err: Option<Error>, range: (i64, i64)) -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockFetchInner {
                    batch_sizes: vec![],
                    stream,
                    buffer: Default::default(),
                    next_err,
                    range,
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
                if let Some(err) = inner.lock().await.next_err.take() {
                    return Err(err);
                }

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

        fn get_offset(&self, at: OffsetAt) -> BoxFuture<'_, Result<i64>> {
            let inner = Arc::clone(&self.inner);

            Box::pin(async move {
                match at {
                    OffsetAt::Earliest => Ok(inner.lock().await.range.0),
                    OffsetAt::Latest => Ok(inner.lock().await.range.1),
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
        let consumer = Arc::new(MockFetch::new(receiver, None, (0, 1_000)));
        let mut stream = StreamConsumerBuilder::new_with_client(
            Arc::<MockFetch>::clone(&consumer),
            StartOffset::At(2),
        )
        .with_max_wait_ms(10)
        .build();

        assert_stream_pending(&mut stream).await;

        // Write two records, nothing should happen as start offset is 2
        sender.send(record.clone()).await.unwrap();
        sender.send(record.clone()).await.unwrap();

        assert_stream_pending(&mut stream).await;

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
        let consumer = Arc::new(MockFetch::new(receiver, None, (0, 1_000)));

        assert!(consumer.batch_sizes().await.is_empty());

        let mut stream = StreamConsumerBuilder::new_with_client(
            Arc::<MockFetch>::clone(&consumer),
            StartOffset::At(0),
        )
        .with_min_batch_size((record.approximate_size() * 2) as i32)
        .with_max_batch_size((record.approximate_size() * 3) as i32)
        .with_max_wait_ms(5)
        .build();

        // Should return nothing
        assert_stream_pending(&mut stream).await;

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
        tokio::time::timeout(Duration::from_micros(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // Should not wait for max_wait_ms
        tokio::time::timeout(Duration::from_micros(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_consumer_terminate() {
        let e = Error::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            error_message: None,
            context: None,
            payload: None,
            is_virtual: true,
        };
        let (_sender, receiver) = mpsc::channel(10);
        let consumer = Arc::new(MockFetch::new(receiver, Some(e), (0, 1_000)));

        let mut stream =
            StreamConsumerBuilder::new_with_client(consumer, StartOffset::At(0)).build();

        let error = stream.next().await.expect("stream not empty").unwrap_err();
        assert_matches!(
            error,
            Error::ServerError {
                protocol_error: ProtocolError::OffsetOutOfRange,
                ..
            }
        );

        // stream ends
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_consumer_earliest() {
        let record = Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: OffsetDateTime::now_utc(),
        };

        // Simulate an error on first fetch to encourage an offset update
        let e = Error::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            error_message: None,
            context: None,
            payload: None,
            is_virtual: true,
        };

        let (sender, receiver) = mpsc::channel(10);
        let consumer = Arc::new(MockFetch::new(receiver, Some(e), (2, 1_000)));
        let mut stream = StreamConsumerBuilder::new_with_client(
            Arc::<MockFetch>::clone(&consumer),
            StartOffset::Earliest,
        )
        .with_max_wait_ms(10)
        .build();

        assert_stream_pending(&mut stream).await;

        // Write two records, nothing should happen as start offset is 2 (via "earliest")
        sender.send(record.clone()).await.unwrap();
        sender.send(record.clone()).await.unwrap();

        assert_stream_pending(&mut stream).await;

        sender.send(record.clone()).await.unwrap();

        let unwrap = |e: Result<Option<Result<_, _>>, _>| e.unwrap().unwrap().unwrap();

        // need a solid timeout here because we have simulated an error that caused a backoff
        let (record_and_offset, high_watermark) =
            unwrap(tokio::time::timeout(Duration::from_secs(2), stream.next()).await);

        assert_eq!(record_and_offset.offset, 2);
        assert_eq!(high_watermark, 2);
    }

    #[tokio::test]
    async fn test_consumer_latest() {
        let record = Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: OffsetDateTime::now_utc(),
        };

        // Simulate an error on first fetch to encourage an offset update
        let e = Error::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            error_message: None,
            context: None,
            payload: None,
            is_virtual: true,
        };

        let (sender, receiver) = mpsc::channel(10);
        let consumer = Arc::new(MockFetch::new(receiver, Some(e), (0, 2)));
        let mut stream = StreamConsumerBuilder::new_with_client(
            Arc::<MockFetch>::clone(&consumer),
            StartOffset::Latest,
        )
        .with_max_wait_ms(10)
        .build();

        assert_stream_pending(&mut stream).await;

        // Write two records, nothing should happen as start offset is 2 (via "latest")
        sender.send(record.clone()).await.unwrap();
        sender.send(record.clone()).await.unwrap();

        assert_stream_pending(&mut stream).await;

        sender.send(record.clone()).await.unwrap();

        let unwrap = |e: Result<Option<Result<_, _>>, _>| e.unwrap().unwrap().unwrap();

        // need a solid timeout here because we have simulated an error that caused a backoff
        let (record_and_offset, high_watermark) =
            unwrap(tokio::time::timeout(Duration::from_secs(2), stream.next()).await);

        assert_eq!(record_and_offset.offset, 2);
        assert_eq!(high_watermark, 2);
    }

    /// Assert that given stream is pending.
    ///
    /// This will will try to poll the stream for a bit to ensure that async IO has a chance to catch up.
    async fn assert_stream_pending<S>(stream: &mut S)
    where
        S: Stream + Send + Unpin,
        S::Item: std::fmt::Debug,
    {
        tokio::select! {
            e = stream.next() => panic!("stream is not pending, yielded: {e:?}"),
            _ = tokio::time::sleep(Duration::from_millis(1)) => {},
        };
    }
}
