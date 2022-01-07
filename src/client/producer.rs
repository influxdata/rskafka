use std::sync::Arc;
use std::time::Duration;

use futures::{pin_mut, FutureExt};
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

/// Builder for `BatchProducer`
pub struct BatchProducerBuilder {
    client: Arc<PartitionClient>,

    linger: Duration,
}

impl BatchProducerBuilder {
    /// Build a new `BatchProducer`
    pub fn new(client: Arc<PartitionClient>) -> Self {
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

/// [`BatchProducer`] attempts to aggregate multiple produce requests together
/// using the provided [`Aggregator`]
///
/// It will buffer up records until either the linger time expires or [`Aggregator`]
/// cannot accommodate another record.
///
/// At this point it will flush the [`Aggregator`]
#[derive(Debug)]
pub struct BatchProducer<A> {
    linger: Duration,

    client: Arc<PartitionClient>,

    inner: Mutex<ProducerInner<A>>,
}

#[derive(Debug)]
struct ProducerInner<A> {
    result_slot: broadcast::BroadcastOnce,

    aggregator: A,
}

impl<A: aggregator::Aggregator<Output = Vec<Record>>> BatchProducer<A> {
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
            // Get a copy of the result slot for the next produce operation
            inner.result_slot.receive().fuse()
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
        if let Some(r) = result_slot.now_or_never() {
            return Ok(r?);
        }

        println!("Linger expired - flushing");

        // Flush data
        Self::flush(&mut inner, &self.client).await;
        Ok(())
    }

    /// Flushes out the data from the aggregator, publishes the result to the result slot,
    /// and creates a fresh result slot for future writes to use
    async fn flush(inner: &mut ProducerInner<A>, client: &PartitionClient) {
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
