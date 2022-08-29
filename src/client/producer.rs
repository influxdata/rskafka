//! Building blocks for a more advanced producer chain.
//!
//! This module provides you:
//!
//! - **lingering:** Control how long your data should wait until being submitted.
//! - **aggregation:** Control how much data should be accumulated on the client side.
//! - **transformation:** Map your own data types to [`Record`]s after they have been aggregated.
//!
//! # Data Flow
//!
//! ```text
//!                 +--------------+            +-----------------+
//! ---(MyData)---->|              |            |                 |
//! <-(MyStatus)-o  |     impl     |-(Records)->| PartitionClient |
//!              ║  |  Aggregator  |            |                 |
//! ---(MyData)---->|              |            +-----------------+
//! <-(MyStatus)-o  |              |                     |
//!              ║  |              |                     |
//!      ...     ║  |              |                     |
//!              ║  |              |                     |
//! ---(MyData)---->|              |                     |
//! <-(MyStatus)-o  |              |                     |
//!              ║  +--------------+                     |
//!              ║         |                             |
//!              ║         V                             |
//!              ║  +--------------+                     |
//!              ║  |              |                     |
//!              o==|     impl     |<-(Offsets)----------o
//!                 |    Status-   |
//!                 | Deaggregator |
//!                 |              |
//!                 +--------------+
//! ```
//!
//! # Usage
//!
//! ## [`Record`] Batching
//! This example shows you how you can send [`Record`]s in batches:
//!
//! ```no_run
//! # async fn test() {
//! use rskafka::{
//!     client::{
//!         ClientBuilder,
//!         partition::UnknownTopicHandling,
//!         producer::{
//!             aggregator::RecordAggregator,
//!             BatchProducerBuilder,
//!         },
//!     },
//!     record::Record,
//! };
//! use time::OffsetDateTime;
//! use std::{
//!     collections::BTreeMap,
//!     sync::Arc,
//!     time::Duration,
//! };
//!
//! // get partition client
//! let connection = "localhost:9093".to_owned();
//! let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
//! let partition_client = Arc::new(
//!     client.partition_client(
//!         "my_topic",
//!         0,
//!         UnknownTopicHandling::Retry,
//!     ).await.unwrap()
//! );
//!
//! // construct batch producer
//! let producer = BatchProducerBuilder::new(partition_client)
//!     .with_linger(Duration::from_secs(2))
//!     .build(RecordAggregator::new(
//!         1024,  // maximum bytes
//!     ));
//!
//! // produce data
//! let record = Record {
//!     key: None,
//!     value: Some(b"hello kafka".to_vec()),
//!     headers: BTreeMap::from([
//!         ("foo".to_owned(), b"bar".to_vec()),
//!     ]),
//!     timestamp: OffsetDateTime::now_utc(),
//! };
//! producer.produce(record.clone()).await.unwrap();
//! # }
//! ```
//!
//! ## Custom Data Types
//! This example demonstrates the usage of a custom data type:
//!
//! ```no_run
//! # async fn test() {
//! use rskafka::{
//!     client::{
//!         ClientBuilder,
//!         partition::UnknownTopicHandling,
//!         producer::{
//!             aggregator::{
//!                 Aggregator,
//!                 Error as AggError,
//!                 StatusDeaggregator,
//!                 TryPush,
//!             },
//!             BatchProducerBuilder,
//!         },
//!     },
//!     record::Record,
//! };
//! use time::OffsetDateTime;
//! use std::{
//!     collections::BTreeMap,
//!     sync::Arc,
//!     time::Duration,
//! };
//!
//! // This is the custom data type that we want to aggregate
//! struct Payload {
//!     inner: Vec<u8>,
//! }
//!
//! // Define an aggregator
//! #[derive(Default)]
//! struct MyAggregator {
//!     data: Vec<u8>,
//! }
//!
//! impl Aggregator for MyAggregator {
//!     type Input = Payload;
//!     type Tag = ();
//!     type StatusDeaggregator = MyStatusDeagg;
//!
//!     fn try_push(
//!         &mut self,
//!         record: Self::Input,
//!     ) -> Result<TryPush<Self::Input, Self::Tag>, AggError> {
//!         // accumulate up to 1Kb of data
//!         if record.inner.len() + self.data.len() > 1024 {
//!             return Ok(TryPush::NoCapacity(record));
//!         }
//!
//!         let mut record = record;
//!         self.data.append(&mut record.inner);
//!
//!         Ok(TryPush::Aggregated(()))
//!     }
//!
//!     fn flush(&mut self) -> Result<(Vec<Record>, Self::StatusDeaggregator), AggError> {
//!         let data = std::mem::take(&mut self.data);
//!         let records = vec![
//!             Record {
//!                 key: None,
//!                 value: Some(data),
//!                 headers: BTreeMap::from([
//!                     ("foo".to_owned(), b"bar".to_vec()),
//!                 ]),
//!                 timestamp: OffsetDateTime::now_utc(),
//!             },
//!         ];
//!         Ok((
//!             records,
//!             MyStatusDeagg {}
//!         ))
//!     }
//! }
//!
//! #[derive(Debug)]
//! struct MyStatusDeagg {}
//!
//! impl StatusDeaggregator for MyStatusDeagg {
//!     type Status = ();
//!     type Tag = ();
//!
//!     fn deaggregate(&self, _input: &[i64], _tag: Self::Tag) -> Result<Self::Status, AggError> {
//!         // don't care about the offsets
//!         Ok(())
//!     }
//! }
//!
//! // get partition client
//! let connection = "localhost:9093".to_owned();
//! let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
//! let partition_client = Arc::new(
//!     client.partition_client(
//!         "my_topic",
//!         0,
//!         UnknownTopicHandling::Retry,
//!     ).await.unwrap()
//! );
//!
//! // construct batch producer
//! let producer = BatchProducerBuilder::new(partition_client)
//!     .with_linger(Duration::from_secs(2))
//!     .build(
//!         MyAggregator::default(),
//!     );
//!
//! // produce data
//! let payload = Payload {
//!     inner: b"hello kafka".to_vec(),
//! };
//! producer.produce(payload).await.unwrap();
//! # }
//! ```
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::*;

use self::{
    aggregator::Aggregator,
    batch::{BatchBuilder, FlushResult, ResultHandle},
};
use crate::{
    client::{
        error::Error as ClientError,
        partition::{Compression, PartitionClient},
        producer::aggregator::TryPush,
    },
    record::Record,
};

pub mod aggregator;
mod batch;
pub(crate) mod broadcast;

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("Aggregator error: {0}")]
    Aggregator(Arc<dyn std::error::Error + Send + Sync>),

    #[error("Client error: {0}")]
    Client(#[from] Arc<ClientError>),

    #[error("Flush error: {0}")]
    FlushError(String),

    #[error("Input too large for aggregator")]
    TooLarge,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builder for [`BatchProducer`].
#[derive(Debug)]
pub struct BatchProducerBuilder {
    client: Arc<dyn ProducerClient>,

    linger: Duration,

    compression: Compression,
}

impl BatchProducerBuilder {
    /// Build a new `BatchProducer`.
    pub fn new(client: Arc<PartitionClient>) -> Self {
        Self::new_with_client(client)
    }

    /// Construct a [`BatchProducer`] with a dynamically dispatched
    /// [`ProducerClient`] implementation.
    pub fn new_with_client(client: Arc<dyn ProducerClient>) -> Self {
        Self {
            client,
            linger: Duration::from_millis(5),
            compression: Compression::default(),
        }
    }

    /// Sets the minimum amount of time to wait for new data before flushing the batch
    pub fn with_linger(self, linger: Duration) -> Self {
        Self { linger, ..self }
    }

    /// Sets compression.
    pub fn with_compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    pub fn build<A>(self, aggregator: A) -> BatchProducer<A>
    where
        A: aggregator::Aggregator,
    {
        BatchProducer {
            linger: self.linger,
            inner: Arc::new(parking_lot::Mutex::new(ProducerInner::new(
                aggregator,
                self.client,
                self.compression,
            ))),
        }
    }
}

/// The [`ProducerClient`] provides an abstraction over a Kafka client than can
/// produce a record.
///
/// Implementing this trait allows user code to inspect the low-level Kafka
/// [`Record`] instances being published to Kafka, as well as the result of the
/// produce call.
///
/// Most users will want to use the [`BatchProducer`] implementation of this
/// trait.
pub trait ProducerClient: std::fmt::Debug + Send + Sync {
    /// Write the set of `records` to the Kafka broker, using the specified
    /// `compression` algorithm.
    fn produce(
        &self,
        records: Vec<Record>,
        compression: Compression,
    ) -> BoxFuture<'_, Result<Vec<i64>, ClientError>>;
}

impl ProducerClient for PartitionClient {
    fn produce(
        &self,
        records: Vec<Record>,
        compression: Compression,
    ) -> BoxFuture<'_, Result<Vec<i64>, ClientError>> {
        Box::pin(self.produce(records, compression))
    }
}

#[derive(Debug)]
struct ProducerInner<A>
where
    A: aggregator::Aggregator,
{
    /// A wrapper over an [`Aggregator`] implementation that represents a single
    /// aggregated batch of Records.
    ///
    /// This is wrapped in an [`Option`] to enable the
    /// [`ProducerInner::flush()`] call to take the buffer, flush it, and
    /// replace it with a new instance. Only the `flush()` method will ever
    /// observe this being `None`, and all other code can safely `unwrap()` on
    /// it.
    batch_builder: Option<BatchBuilder<A>>,

    /// A logical clock to enable a conditional flush of a specific
    /// [`BatchBuilder`] instance.
    ///
    /// Each time a new [`BatchBuilder`] is initialised, this counter increases
    /// by 1 (and wraps as necessary). Callers requested to flush the batch
    /// after the linger duration are given the current value of this LC in the
    /// [`CallerRole::Linger`] return variant, and once the linger fires, they
    /// present their LC value in the [`ProducerInner::flush()`] call. If the
    /// values match, the buffer if flushed. If the values do not match, the
    /// buffer the caller was responsible for has already been flushed, and the
    /// flush call becomes a NOP.
    flush_clock: usize,

    /// Used to track if a [`CallerRole::Linger`] has been returned for the
    /// current batch_builder.
    has_linger_waiter: bool,

    compression: Compression,
    client: Arc<dyn ProducerClient>,

    /// A list of (potentially) outstanding flush tasks.
    ///
    /// These may or may not yet be complete, and completed flush tasks are
    /// removed from this list when adding new flush tasks or manually flushing
    /// with a call to [`BatchProducer::flush()`].
    pending_flushes: Vec<JoinHandle<()>>,
}

impl<A> ProducerInner<A>
where
    A: aggregator::Aggregator,
{
    fn new(aggregator: A, client: Arc<dyn ProducerClient>, compression: Compression) -> Self {
        Self {
            batch_builder: Some(BatchBuilder::new(aggregator)),
            flush_clock: 0,
            has_linger_waiter: false,
            client,
            compression,
            pending_flushes: Vec::new(),
        }
    }

    /// Attempt to push `data` to the user-configured [`Aggregator`] impl within
    /// the [`BatchBuilder`].
    ///
    /// If the aggregator indicates it is full, [`Self::flush()`] is called and
    /// `data` is inserted into the new [`BatchBuilder`] instance. If this
    /// insert attempt also fails, [`Error::TooLarge`] is returned.
    ///
    /// Once the [`BatchBuilder`] has accepted `data`, a [`CallerRole`] is
    /// returned - if this was the first write to the [`Aggregator`], the
    /// variant [`CallerRole::Linger`] is returned and the caller should wait
    /// for the configured linger time (higher in the stack) before calling
    /// [`Self::flush()`] to ensure timely batch writes.
    ///
    /// If [`CallerRole::JustWait`] is returned, there is already an outstanding
    /// linger/flusher task, and this caller can wait on the provided
    /// [`ResultHandle`] for the write result.
    fn try_push(&mut self, data: A::Input) -> Result<CallerRole<A>, Error> {
        // Try and write data to the [`BatchBuilder`].
        let handle = match self.batch_builder.as_mut().unwrap().try_push(data)? {
            TryPush::Aggregated(handle) => handle,
            TryPush::NoCapacity(data) => {
                debug!(client=?self.client, "insufficient capacity in aggregator - flushing");

                // Perform an immediate flush of the buffer in the background,
                // returning without waiting for the flush to complete.
                //
                // This call sets a new BatchBuilder while the flush of the old
                // instance happens in the background, enabling the caller's
                // Mutex to be dropped, so that further produce() calls can
                // start aggregating into a new batch of writes.
                //
                // As a side effect, this invalidates any callers performing a
                // linger wait + flush, preventing them from flushing this new
                // batch.
                self.flush(None)?;

                match self.batch_builder.as_mut().unwrap().try_push(data)? {
                    TryPush::Aggregated(handle) => handle,
                    TryPush::NoCapacity(_) => {
                        error!(client=?self.client, "record too large for aggregator");
                        return Err(Error::TooLarge);
                    }
                }
            }
        };

        // If this is the first writer to this batch, has_linger_waiter will be
        // false, indicating this writer should wait for the linger time before
        // trying to flush this batch.
        if self.has_linger_waiter {
            // There is an existing caller handling the linger timeout.
            return Ok(CallerRole::JustWait(handle));
        }

        // This caller is the first writer to this batch, and it should wait for
        // the linger time before flushing THIS batch.
        //
        // While the writer is waiting to flush THIS batch, it is possible
        // another call this function results in a NoCapacity being returned by
        // the aggregator above, which would flush THIS batch.
        //
        // When this happens, the writer waiting for the linger time should NOT
        // flush the newly created batch, so it uses the flush clock as a token
        // to avoid flushing the wrong batch when calling flush().
        self.has_linger_waiter = true;
        Ok(CallerRole::Linger {
            handle,
            flush_token: self.flush_clock,
        })
    }

    /// Asynchronously write this batch of writes to Kafka, flushing the
    /// underlying [`Aggregator`].
    ///
    /// If the caller provides a `flusher_token`, the batch flush is conditional
    /// on the token matching. If the token does not match, the batch the caller
    /// is attempting to flush has already been flushed, and this call is a NOP.
    fn flush(&mut self, flusher_token: Option<usize>) -> Result<()> {
        // If this caller is is intending to conditionally flush a specific
        // batch, verify this BatchBuilder is the batch it is indenting to
        // flush.
        if let Some(token) = flusher_token {
            if token != self.flush_clock {
                debug!(client=?self.client, "spurious batch flush call");
                return Ok(());
            }
        }

        debug!(client=?self.client, "flushing batch");

        // Remove the batch, temporarily swapping it for a None until a new
        // batch is built.
        //
        // Nothing can observe a None in the batch field as it is always
        // immediately replaced with a new batch instance below.
        let batch = self.batch_builder.take().expect("no batch to flush");

        let (new_builder, flush_task, maybe_err) =
            match batch.background_flush(Arc::clone(&self.client), self.compression) {
                FlushResult::Ok(b, flush_task) => (b, flush_task, None),
                FlushResult::Error(b, e) => {
                    error!(client=?self.client, error=%e, "failed to write record batch");
                    (b, None, Some(e))
                }
            };

        // Replace the batch builder with the new instance.
        self.batch_builder = Some(new_builder);

        // Remove any completed flushes from the pending_flushes list.
        //
        // Ideally this would be a linked list so removing elements are cheap,
        // but LinkedList in stable cannot do that...
        self.pending_flushes.retain_mut(|t| !t.is_finished());

        // Retain a handle to the flush task.
        //
        // This enables a manual flush to wait for all outstanding flush tasks
        // to complete.
        if let Some(t) = flush_task {
            self.pending_flushes.push(t);
        }

        // The flush clock increments, so that any threads trying to flush the
        // last batch do not accidentally flush this new batch, leading to
        // undersized batches / higher overhead per batch.
        //
        // Wrapping add to accept uint rollover, which is not problematic for
        // this task.
        self.flush_clock = self.flush_clock.wrapping_add(1);

        // Reset the "need a flusher" bool so that the first write to this new
        // batch is indicated to wait for the linger period to expire, and then
        // trigger a flush.
        self.has_linger_waiter = false;

        match maybe_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

enum CallerRole<A>
where
    A: Aggregator,
{
    /// The caller has no additional book-keeping to perform, and can wait on
    /// the result handle for the batched write result.
    JustWait(ResultHandle<A>),

    /// This caller has been selected to perform the "linger" timeout to drive
    /// timely flushing of the batch.
    ///
    /// The caller MUST wait for the configured linger time before calling
    /// [`ProducerInner::flush()`] with the provided `flush_token`.
    ///
    /// Once the batch is flushed (either by this caller after the linger, or
    /// before if the batch was prematurely flushed) the results are made
    /// available through handle.
    Linger {
        handle: ResultHandle<A>,
        flush_token: usize,
    },
}

/// [`BatchProducer`] attempts to aggregate multiple produce requests together
/// using the provided [`Aggregator`].
///
/// It will buffer up records until either the linger time expires or
/// [`Aggregator`] cannot accommodate another record.
///
/// At this point it will flush the [`Aggregator`]
///
/// [`Aggregator`]: aggregator::Aggregator
#[derive(Debug)]
pub struct BatchProducer<A>
where
    A: aggregator::Aggregator,
{
    linger: Duration,
    inner: Arc<parking_lot::Mutex<ProducerInner<A>>>,
}

impl<A> BatchProducer<A>
where
    A: aggregator::Aggregator,
{
    /// Write `data` to this [`BatchProducer`]
    ///
    /// Returns when the data has been committed to Kafka or an unrecoverable
    /// error has been encountered.
    ///
    /// # Cancellation
    ///
    /// The returned future is cancellation safe in that it won't leave the
    /// [`BatchProducer`] in an inconsistent state, however, the provided data
    /// may or may not be produced.
    ///
    pub async fn produce(
        &self,
        data: A::Input,
    ) -> Result<<A as aggregator::AggregatorStatus>::Status> {
        let role = {
            // Try to add the record to the aggregator
            let mut inner = self.inner.lock();
            inner.try_push(data)?
        };

        match role {
            CallerRole::JustWait(mut handle) => {
                // Another caller is running the linger timer, and this caller
                // can wait for the write result.
                let status = handle.wait().await?;
                handle.result(status)
            }
            CallerRole::Linger {
                mut handle,
                flush_token,
            } => {
                // This caller has been selected to wait for the linger
                // duration, and then attempt to flush the batch of writes.
                //
                // Spawn a task for the linger to ensure cancellation safety.
                let linger: JoinHandle<Result<(), Error>> = tokio::spawn({
                    let linger = self.linger;
                    let inner = Arc::clone(&self.inner);
                    async move {
                        tokio::time::sleep(linger).await;

                        // The linger has expired, attempt to conditionally flush the
                        // batch using the provided token to ensure only the correct
                        // batch is flushed.
                        inner.lock().flush(Some(flush_token))?;
                        Ok(())
                    }
                });

                // The batch may be flushed before the linger period expires if
                // the aggregator becomes full, so watch for both outcomes.
                tokio::select! {
                    res = linger => res.expect("linger panic")?,
                    r = handle.wait() => return handle.result(r?),
                }

                // The linger expired & completed.
                //
                // Wait for the result of the flush to be published.
                let status = handle.wait().await?;
                // And demux the status for this caller.
                handle.result(status)
            }
        }
    }

    /// Flushed out data from aggregator.
    ///
    /// Blocks until all pending writes to Kafka complete (or fail).
    ///
    /// If this function returns an error, the flush may be incomplete.
    pub async fn flush(&self) -> Result<()> {
        let outstanding = {
            let mut inner = self.inner.lock();

            debug!("Manual flush");
            inner.flush(None)?;
            std::mem::take(&mut inner.pending_flushes)
        };

        // Wait for all pending flushes to complete outside of the mutex.
        for t in outstanding.into_iter() {
            if !t.is_finished() {
                t.await.expect("flush task panic");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::aggregator::{Aggregator, RecordAggregatorStatusDeaggregator, StatusDeaggregator};
    use super::*;
    use crate::client::error::RequestContext;
    use crate::{
        client::producer::aggregator::RecordAggregator, protocol::error::Error as ProtocolError,
    };
    use futures::stream::{FuturesOrdered, FuturesUnordered};
    use futures::{pin_mut, FutureExt, StreamExt};
    use time::OffsetDateTime;

    #[derive(Debug)]
    struct MockClient {
        error: Option<ProtocolError>,
        panic: Option<String>,
        delay: Duration,
        batch_sizes: parking_lot::Mutex<Vec<usize>>,
    }

    impl ProducerClient for MockClient {
        fn produce(
            &self,
            records: Vec<Record>,
            _compression: Compression,
        ) -> BoxFuture<'_, Result<Vec<i64>, ClientError>> {
            Box::pin(async move {
                tokio::time::sleep(self.delay).await;

                if let Some(e) = self.error {
                    return Err(ClientError::ServerError {
                        protocol_error: e,
                        error_message: None,
                        request: RequestContext::Partition("foo".into(), 1),
                        response: None,
                        is_virtual: false,
                    });
                }

                if let Some(p) = self.panic.as_ref() {
                    panic!("{}", p);
                }

                let mut batch_sizes = self.batch_sizes.lock();
                let offset_base = batch_sizes.iter().sum::<usize>();
                let offsets = (0..records.len())
                    .map(|x| (x + offset_base) as i64)
                    .collect();
                batch_sizes.push(records.len());
                Ok(offsets)
            })
        }
    }

    fn record() -> Record {
        Record {
            key: Some(vec![0; 4]),
            value: Some(vec![0; 6]),
            headers: Default::default(),
            timestamp: OffsetDateTime::from_unix_timestamp(320).unwrap(),
        }
    }

    #[tokio::test]
    async fn test_producer() {
        let record = record();
        let linger = Duration::from_millis(100);

        for delay in [Duration::from_secs(0), Duration::from_millis(1)] {
            let client = Arc::new(MockClient {
                error: None,
                panic: None,
                delay,
                batch_sizes: Default::default(),
            });

            let aggregator = RecordAggregator::new(record.approximate_size() * 2);
            let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
                .with_linger(linger)
                .build(aggregator);

            let mut futures = FuturesOrdered::new();

            futures.push_back(producer.produce(record.clone()));
            futures.push_back(producer.produce(record.clone()));
            futures.push_back(producer.produce(record.clone()));

            let assert_ok = |a: Result<Option<Result<_, _>>, _>, expected: i64| {
                let offset = a
                    .expect("no timeout")
                    .expect("Some future left")
                    .expect("no producer error");
                assert_eq!(offset, expected);
            };

            // First two publishes should be ok
            assert_ok(
                tokio::time::timeout(Duration::from_millis(10), futures.next()).await,
                0,
            );
            assert_ok(
                tokio::time::timeout(Duration::from_millis(10), futures.next()).await,
                1,
            );

            // Third should linger
            tokio::time::timeout(Duration::from_millis(10), futures.next())
                .await
                .expect_err("timeout");

            assert_eq!(client.batch_sizes.lock().as_slice(), &[2]);

            // Should publish third record after linger expires
            assert_ok(tokio::time::timeout(linger * 2, futures.next()).await, 2);
            assert_eq!(client.batch_sizes.lock().as_slice(), &[2, 1]);
        }
    }

    #[tokio::test]
    async fn test_manual_flush() {
        let record = record();
        let linger = Duration::from_secs(3600);

        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay: Duration::from_millis(1),
            batch_sizes: Default::default(),
        });

        let aggregator = RecordAggregator::new(usize::MAX);
        let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
            .with_linger(linger)
            .build(aggregator);

        let a = producer.produce(record.clone()).fuse();
        pin_mut!(a);

        let b = producer.produce(record).fuse();
        pin_mut!(b);

        futures::select! {
            _ = a => panic!("a finished!"),
            _ = b => panic!("b finished!"),
            _ = tokio::time::sleep(Duration::from_millis(100)).fuse() => {}
        };

        producer.flush().await.unwrap();

        let offset_a = tokio::time::timeout(Duration::from_millis(10), a)
            .await
            .unwrap()
            .unwrap();
        let offset_b = tokio::time::timeout(Duration::from_millis(10), b)
            .await
            .unwrap()
            .unwrap();
        assert!(((offset_a == 0) && (offset_b == 1)) || ((offset_a == 1) && (offset_b == 0)));
    }

    #[tokio::test]
    async fn test_producer_empty_aggregator_with_linger() {
        // this setting used to result in a panic
        let record = record();
        let linger = Duration::from_millis(2);

        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay: Duration::from_millis(0),
            batch_sizes: Default::default(),
        });

        struct EmptyAgg {}

        impl Aggregator for EmptyAgg {
            type Input = Record;

            type Tag = ();

            type StatusDeaggregator = EmptyDeagg;

            fn try_push(
                &mut self,
                _record: Self::Input,
            ) -> Result<TryPush<Self::Input, Self::Tag>, aggregator::Error> {
                Ok(TryPush::Aggregated(()))
            }

            fn flush(
                &mut self,
            ) -> Result<(Vec<Record>, Self::StatusDeaggregator), aggregator::Error> {
                Ok((vec![], EmptyDeagg {}))
            }
        }

        #[derive(Debug)]
        struct EmptyDeagg {}

        impl StatusDeaggregator for EmptyDeagg {
            type Status = ();

            type Tag = ();

            fn deaggregate(
                &self,
                _input: &[i64],
                _tag: Self::Tag,
            ) -> Result<Self::Status, aggregator::Error> {
                Ok(())
            }
        }

        let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
            .with_linger(linger)
            .build(EmptyAgg {});

        let mut futures: FuturesUnordered<_> = (0..10)
            .map(|_| async {
                producer.produce(record.clone()).await.unwrap();
            })
            .collect();
        while futures.next().await.is_some() {}
    }

    #[tokio::test]
    async fn test_producer_client_error() {
        let record = record();
        let linger = Duration::from_millis(5);
        let client = Arc::new(MockClient {
            error: Some(ProtocolError::NetworkException),
            panic: None,
            delay: Duration::from_millis(1),
            batch_sizes: Default::default(),
        });

        let aggregator = RecordAggregator::new(record.approximate_size() * 2);
        let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
            .with_linger(linger)
            .build(aggregator);

        let mut futures = FuturesUnordered::new();
        futures.push(producer.produce(record.clone()));
        futures.push(producer.produce(record.clone()));

        futures.next().await.unwrap().unwrap_err();
        futures.next().await.unwrap().unwrap_err();
    }

    #[tokio::test]
    async fn test_producer_aggregator_error_push() {
        let record = record();
        let linger = Duration::from_millis(5);
        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay: Duration::from_millis(1),
            batch_sizes: Default::default(),
        });

        let aggregator = MockAggregator {
            inner: RecordAggregator::new(record.approximate_size() * 2),
            push_errors: vec!["test".to_owned().into()],
            flush_errors: vec![],
            deagg_errors: vec![],
        };
        let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
            .with_linger(linger)
            .build(aggregator);

        let mut futures = FuturesUnordered::new();
        futures.push(producer.produce(record.clone()));
        futures.push(producer.produce(record.clone()));
        futures.push(producer.produce(record.clone()));

        futures.next().await.unwrap().unwrap_err();
        futures.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_producer_aggregator_error_flush() {
        let record = record();
        let linger = Duration::from_millis(5);
        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay: Duration::from_millis(1),
            batch_sizes: Default::default(),
        });

        let aggregator = MockAggregator {
            inner: RecordAggregator::new(record.approximate_size() * 2),
            push_errors: vec![],
            flush_errors: vec!["test".to_owned().into()],
            deagg_errors: vec![],
        };
        let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
            .with_linger(linger)
            .build(aggregator);

        let mut futures = FuturesUnordered::new();
        futures.push(producer.produce(record.clone()));
        futures.push(producer.produce(record.clone()));

        futures.next().await.unwrap().unwrap_err();
        futures.next().await.unwrap().unwrap_err();
    }

    #[tokio::test]
    async fn test_producer_aggregator_error_deagg() {
        let record = record();
        let linger = Duration::from_millis(5);
        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay: Duration::from_millis(1),
            batch_sizes: Default::default(),
        });

        let aggregator = MockAggregator {
            inner: RecordAggregator::new(record.approximate_size() * 2),
            push_errors: vec![],
            flush_errors: vec![],
            deagg_errors: vec![vec![Some("test".to_owned().into()), None]],
        };
        let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
            .with_linger(linger)
            .build(aggregator);

        let mut futures = FuturesUnordered::new();
        futures.push(producer.produce(record.clone()));
        futures.push(producer.produce(record.clone()));

        futures.next().await.unwrap().unwrap_err();
        futures.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_producer_aggregator_cancel() {
        let record = record();
        let linger = Duration::from_micros(100);
        let client = Arc::new(MockClient {
            error: None,
            panic: None,
            delay: Duration::from_millis(10),
            batch_sizes: Default::default(),
        });

        let aggregator = RecordAggregator::new(record.approximate_size() * 2);
        let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
            .with_linger(linger)
            .build(aggregator);

        let a = producer.produce(record.clone()).fuse();
        let b = producer.produce(record).fuse();
        pin_mut!(b);

        {
            // Cancel a when it exits this block
            pin_mut!(a);

            // Select biased to encourage a to be the one with the linger that
            // expires first and performs the produce operation
            futures::select_biased! {
                _ = &mut a => panic!("a should not have flushed"),
                _ = &mut b => panic!("b should not have flushed"),
                _ = tokio::time::sleep(Duration::from_millis(1)).fuse() => {},
            }
        }

        // But b should still complete successfully
        tokio::time::timeout(Duration::from_secs(1), b)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(client.batch_sizes.lock().as_slice(), &[2]);
    }

    #[tokio::test]
    async fn test_producer_aggregator_panic() {
        let record = record();
        let linger = Duration::from_millis(100);
        let client = Arc::new(MockClient {
            error: None,
            panic: Some("test panic".into()),
            delay: Duration::from_millis(0),
            batch_sizes: Default::default(),
        });

        let aggregator = RecordAggregator::new(record.approximate_size() * 2);
        let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
            .with_linger(linger)
            .build(aggregator);

        let a = producer.produce(record.clone());
        let b = producer.produce(record);

        let (a, b) = futures::future::join(a, b).await;

        assert!(matches!(&a, Err(Error::FlushError(_))));
        assert!(matches!(&b, Err(Error::FlushError(_))));
    }

    #[derive(Debug)]
    struct MockAggregator {
        inner: RecordAggregator,
        push_errors: Vec<aggregator::Error>,
        flush_errors: Vec<aggregator::Error>,
        deagg_errors: Vec<Vec<Option<aggregator::Error>>>,
    }

    impl Aggregator for MockAggregator {
        type Input = Record;

        type Tag = usize;

        type StatusDeaggregator = MockDeaggregator;

        fn try_push(
            &mut self,
            record: Self::Input,
        ) -> Result<TryPush<Self::Input, Self::Tag>, aggregator::Error> {
            if !self.push_errors.is_empty() {
                return Err(self.push_errors.remove(0));
            }

            Ok(self.inner.try_push(record).unwrap())
        }

        fn flush(&mut self) -> Result<(Vec<Record>, Self::StatusDeaggregator), aggregator::Error> {
            if !self.flush_errors.is_empty() {
                return Err(self.flush_errors.remove(0));
            }

            let deagg_errors = if self.deagg_errors.is_empty() {
                vec![]
            } else {
                self.deagg_errors.remove(0)
            };
            let (records, deagg) = self.inner.flush().unwrap();

            Ok((
                records,
                MockDeaggregator {
                    inner: deagg,
                    errors: std::sync::Mutex::new(deagg_errors),
                },
            ))
        }
    }

    #[derive(Debug)]
    struct MockDeaggregator {
        inner: RecordAggregatorStatusDeaggregator,
        errors: std::sync::Mutex<Vec<Option<aggregator::Error>>>,
    }

    impl StatusDeaggregator for MockDeaggregator {
        type Status = i64;

        type Tag = usize;

        fn deaggregate(
            &self,
            input: &[i64],
            tag: Self::Tag,
        ) -> Result<Self::Status, aggregator::Error> {
            let mut errors = self.errors.lock().unwrap();
            if let Some(e) = errors.get_mut(tag) {
                if let Some(e) = std::mem::take(e) {
                    return Err(e);
                }
            }

            Ok(self.inner.deaggregate(input, tag).unwrap())
        }
    }
}
