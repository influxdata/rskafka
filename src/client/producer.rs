use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{pin_mut, FutureExt, TryFutureExt};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::client::{error::Error as ClientError, partition::PartitionClient};
use crate::record::Record;

pub mod aggregator;
mod broadcast;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Aggregator error: {0}")]
    Aggregator(#[from] aggregator::Error),

    #[error("Client error: {0}")]
    Client(#[from] Arc<ClientError>),

    #[error("Input too large for aggregator")]
    TooLarge,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builder for [`BatchProducer`].
#[derive(Debug)]
pub struct BatchProducerBuilder {
    client: Arc<dyn ProducerClient>,

    linger: Duration,
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
        }
    }

    /// Sets the minimum amount of time to wait for new data before flushing the batch
    pub fn with_linger(self, linger: Duration) -> Self {
        Self { linger, ..self }
    }

    pub fn build<A>(self, aggregator: A) -> BatchProducer<A> {
        BatchProducer {
            linger: self.linger,
            client: self.client,
            inner: Mutex::new(ProducerInner {
                aggregator,
                result_slot: Default::default(),
            }),
        }
    }
}

// A trait wrapper to allow mocking
trait ProducerClient: std::fmt::Debug + Send + Sync {
    fn produce(&self, records: Vec<Record>) -> BoxFuture<'_, Result<(), ClientError>>;
}

impl ProducerClient for PartitionClient {
    fn produce(&self, records: Vec<Record>) -> BoxFuture<'_, Result<(), ClientError>> {
        Box::pin(self.produce(records).map_ok(|_| ()))
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
pub struct BatchProducer<A> {
    linger: Duration,

    client: Arc<dyn ProducerClient>,

    inner: Mutex<ProducerInner<A>>,
}

#[derive(Debug)]
struct ProducerInner<A> {
    result_slot: broadcast::BroadcastOnce<Result<(), Arc<ClientError>>>,

    aggregator: A,
}

impl<A> BatchProducer<A>
where
    A: aggregator::Aggregator<Output = Vec<Record>> + Send,
    <A as aggregator::Aggregator>::Input: Send,
{
    /// Write `data` to this [`BatchProducer`]
    ///
    /// Returns when the data has been committed to Kafka or
    /// an unrecoverable error has been encountered
    ///
    /// # Cancellation
    ///
    /// The returned future is not cancellation safe, if it is dropped the record
    /// may or may not be published
    ///
    pub async fn produce(&self, data: A::Input) -> Result<()> {
        let result_slot = {
            // Try to add the record to the aggregator
            let mut inner = self.inner.lock().await;
            if let Some(data) = inner.aggregator.try_push(data)? {
                println!("Insufficient capacity in aggregator - flushing");

                Self::flush(&mut inner, self.client.as_ref()).await;
                if inner.aggregator.try_push(data)?.is_some() {
                    println!("Record too large for aggregator");
                    return Err(Error::TooLarge);
                }
            }
            // Get a future that completes when the record is published
            inner.result_slot.receive()
        };

        let linger = tokio::time::sleep(self.linger).fuse();
        pin_mut!(linger);
        pin_mut!(result_slot);

        futures::select! {
            r = result_slot => return Ok(r?),
            _ = linger => {}
        }

        // Linger expired - reacquire lock
        let mut inner = self.inner.lock().await;

        // Whilst holding lock - check hasn't been flushed already
        //
        // This covers two scenarios:
        // - the linger expired "simultaneously" with the publish
        // - the linger expired but another thread triggered the flush
        if let Some(r) = result_slot.peek() {
            return Ok(r.clone()?);
        }

        println!("Linger expired - flushing");

        // Flush data
        Self::flush(&mut inner, self.client.as_ref()).await;

        Ok(result_slot.now_or_never().expect("just flushed")?)
    }

    /// Flushes out the data from the aggregator, publishes the result to the result slot,
    /// and creates a fresh result slot for future writes to use
    async fn flush(inner: &mut ProducerInner<A>, client: &dyn ProducerClient) {
        println!("Flushing batch producer");

        let output = inner.aggregator.flush();
        if output.is_empty() {
            return;
        }

        let r = client.produce(output).await;

        // Reset result slot
        let slot = std::mem::take(&mut inner.result_slot);

        match r {
            Ok(_) => slot.broadcast(Ok(())),
            Err(e) => slot.broadcast(Err(Arc::new(e))),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::producer::aggregator::RecordAggregator;
    use crate::ProtocolError;
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;
    use time::OffsetDateTime;

    #[derive(Debug)]
    struct MockClient {
        error: Option<ProtocolError>,
        delay: Duration,
        batch_sizes: parking_lot::Mutex<Vec<usize>>,
    }

    impl ProducerClient for MockClient {
        fn produce(&self, records: Vec<Record>) -> BoxFuture<'_, Result<(), ClientError>> {
            Box::pin(async move {
                tokio::time::sleep(self.delay).await;

                if let Some(e) = self.error {
                    return Err(ClientError::ServerError(e, "".to_string()));
                }

                self.batch_sizes.lock().push(records.len());
                Ok(())
            })
        }
    }

    fn record() -> Record {
        Record {
            key: vec![0; 4],
            value: vec![0; 6],
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
                delay,
                batch_sizes: Default::default(),
            });

            let aggregator = RecordAggregator::new(record.approximate_size() * 2);
            let producer = BatchProducerBuilder::new_with_client(Arc::<MockClient>::clone(&client))
                .with_linger(linger)
                .build(aggregator);

            let mut futures = FuturesUnordered::new();

            futures.push(producer.produce(record.clone()));
            futures.push(producer.produce(record.clone()));
            futures.push(producer.produce(record.clone()));

            let assert_ok = |a: Result<Option<Result<_, _>>, _>| a.unwrap().unwrap().unwrap();

            // First two publishes should be ok
            assert_ok(tokio::time::timeout(Duration::from_millis(10), futures.next()).await);
            assert_ok(tokio::time::timeout(Duration::from_millis(10), futures.next()).await);

            // Third should linger
            tokio::time::timeout(Duration::from_millis(10), futures.next())
                .await
                .expect_err("timeout");

            assert_eq!(client.batch_sizes.lock().as_slice(), &[2]);

            // Should publish third record after linger expires
            assert_ok(tokio::time::timeout(linger * 2, futures.next()).await);
            assert_eq!(client.batch_sizes.lock().as_slice(), &[2, 1]);
        }
    }

    #[tokio::test]
    async fn test_producer_error() {
        let record = record();
        let linger = Duration::from_millis(5);
        let client = Arc::new(MockClient {
            error: Some(ProtocolError::NetworkException),
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
}
