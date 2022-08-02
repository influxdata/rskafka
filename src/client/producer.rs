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
//!     client.partition_client("my_topic", 0).unwrap()
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
//!     client.partition_client("my_topic", 0).unwrap()
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
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{debug, error};

use crate::client::producer::aggregator::TryPush;
use crate::client::producer::broadcast::BroadcastOnce;
use crate::client::{error::Error as ClientError, partition::PartitionClient};
use crate::record::Record;

use super::partition::Compression;

pub mod aggregator;
mod broadcast;

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
    /// Build a new `BatchProducer`
    pub fn new(client: Arc<PartitionClient>) -> Self {
        Self::new_with_client(client)
    }

    /// Internal API for creating with any `dyn ProducerClient`
    fn new_with_client(client: Arc<dyn ProducerClient>) -> Self {
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
            compression: self.compression,
            client: self.client,
            inner: Arc::new(Mutex::new(ProducerInner {
                aggregator,
                result_slot: Default::default(),
            })),
        }
    }
}

// A trait wrapper to allow mocking
trait ProducerClient: std::fmt::Debug + Send + Sync {
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

/// [`BatchProducer`] attempts to aggregate multiple produce requests together
/// using the provided [`Aggregator`].
///
/// It will buffer up records until either the linger time expires or [`Aggregator`]
/// cannot accommodate another record.
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

    compression: Compression,

    client: Arc<dyn ProducerClient>,

    inner: Arc<Mutex<ProducerInner<A>>>,
}

#[derive(Debug)]
struct AggregatedStatus<A>
where
    A: aggregator::Aggregator,
{
    aggregated_status: Vec<i64>,
    status_deagg: <A as aggregator::Aggregator>::StatusDeaggregator,
}

type AggregatedResult<A> = Result<Arc<AggregatedStatus<A>>, Error>;

fn extract<A>(
    result: broadcast::Result<AggregatedResult<A>>,
    tag: A::Tag,
) -> Result<<A as aggregator::AggregatorStatus>::Status, Error>
where
    A: aggregator::Aggregator,
{
    use self::aggregator::StatusDeaggregator;
    let status = result.map_err(|e| Error::FlushError(e.to_string()))??;

    status
        .status_deagg
        .deaggregate(&status.aggregated_status, tag)
        .map_err(|e| Error::Aggregator(e.into()))
}

#[derive(Debug)]
struct ProducerInner<A>
where
    A: aggregator::Aggregator,
{
    result_slot: BroadcastOnce<AggregatedResult<A>>,

    aggregator: A,
}

impl<A> BatchProducer<A>
where
    A: aggregator::Aggregator,
{
    /// Write `data` to this [`BatchProducer`]
    ///
    /// Returns when the data has been committed to Kafka or
    /// an unrecoverable error has been encountered
    ///
    /// # Cancellation
    ///
    /// The returned future is cancellation safe in that it won't leave the [`BatchProducer`]
    /// in an inconsistent state, however, the provided data may or may not be produced
    ///
    pub async fn produce(
        &self,
        data: A::Input,
    ) -> Result<<A as aggregator::AggregatorStatus>::Status> {
        let (result_slot, tag) = {
            // Try to add the record to the aggregator
            let mut inner = self.lock().await;

            match inner
                .aggregator
                .try_push(data)
                .map_err(|e| Error::Aggregator(e.into()))?
            {
                TryPush::Aggregated(tag) => (inner.result_slot.receiver(), tag),
                TryPush::NoCapacity(data) => {
                    debug!(client=?self.client, "Insufficient capacity in aggregator - flushing");

                    let mut inner =
                        Self::flush_impl(inner, Arc::clone(&self.client), self.compression).await?;

                    match inner
                        .aggregator
                        .try_push(data)
                        .map_err(|e| Error::Aggregator(e.into()))?
                    {
                        TryPush::Aggregated(tag) => (inner.result_slot.receiver(), tag),
                        TryPush::NoCapacity(_) => {
                            error!(client=?self.client, "Record too large for aggregator");
                            return Err(Error::TooLarge);
                        }
                    }
                }
            }
        };

        tokio::select! {
            _ = tokio::time::sleep(self.linger) => {},
            r = result_slot.receive() => return extract(r, tag),
        }

        // Linger expired - reacquire lock
        let inner = self.lock().await;

        // Whilst holding lock - check hasn't been flushed already
        //
        // This covers two scenarios:
        // - the linger expired "simultaneously" with the publish
        // - the linger expired but another thread triggered the flush
        if let Some(r) = result_slot.peek() {
            debug!(client=?self.client, ?tag, "Already flushed");
            return extract(r, tag);
        }

        debug!(client=?self.client, ?tag, "Linger expired - flushing");

        // Flush data
        Self::flush_impl(inner, Arc::clone(&self.client), self.compression).await?;

        extract(result_slot.peek().expect("just flushed"), tag)
    }

    /// Flushed out data from aggregator.
    pub async fn flush(&self) -> Result<()> {
        let inner = self.lock().await;
        debug!(client=?self.client, "Manual flush");
        Self::flush_impl(inner, Arc::clone(&self.client), self.compression).await?;
        Ok(())
    }

    /// Flushes out the data from the aggregator, publishes the result to the result slot,
    /// and creates a fresh result slot for future writes to use
    async fn flush_impl(
        mut inner: OwnedMutexGuard<ProducerInner<A>>,
        client: Arc<dyn ProducerClient>,
        compression: Compression,
    ) -> Result<OwnedMutexGuard<ProducerInner<A>>> {
        debug!(?client, "Flushing batch producer");

        // Spawn a task to provide cancellation safety
        let handle = tokio::spawn(async move {
            // Swap the result slot first, this ensures that if the task panics the Drop
            // implementation will still signal the result slot preventing flushing twice
            let slot = std::mem::take(&mut inner.result_slot);

            let (output, status_deagg) = match inner.aggregator.flush() {
                Ok(x) => x,
                Err(e) => {
                    debug!(?client, error=?e, "Failed to flush aggregator");
                    let e = Error::Aggregator(e.into());
                    slot.broadcast(Err(e.clone()));
                    return Err(e);
                }
            };

            let r = if output.is_empty() {
                debug!(?client, "No data aggregated, skipping client request");

                // A custom aggregator might have produced no records, but the the calls to `.produce` are still waiting for
                // their slot values, so we need to provide them some result, otherwise we might flush twice or panic with
                // "just flushed" because no data is available right after flushing.
                Ok(vec![])
            } else {
                client.produce(output, compression).await
            };

            let result = match r {
                Ok(status) => {
                    debug!(?client, ?status, "Successfully produced records");

                    let aggregated_status = AggregatedStatus {
                        aggregated_status: status,
                        status_deagg,
                    };
                    Ok(Arc::new(aggregated_status))
                }
                Err(e) => {
                    debug!(?client, error=?e, "Failed to produce records");

                    Err(Error::Client(Arc::new(e)))
                }
            };

            slot.broadcast(result);

            Ok(inner)
        });

        match handle.await {
            Ok(r) => r,
            Err(e) => Err(Error::FlushError(e.to_string())),
        }
    }

    async fn lock(&self) -> OwnedMutexGuard<ProducerInner<A>> {
        Arc::clone(&self.inner).lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::aggregator::{Aggregator, RecordAggregatorStatusDeaggregator, StatusDeaggregator};
    use super::*;
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
                        request: None,
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

            futures.push(producer.produce(record.clone()));
            futures.push(producer.produce(record.clone()));
            futures.push(producer.produce(record.clone()));

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
