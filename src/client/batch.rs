use std::sync::Arc;

use tokio::task::JoinHandle;
use tracing::*;

use super::{
    partition::Compression,
    producer::{
        aggregator::{self, Aggregator, StatusDeaggregator, TryPush},
        broadcast::{BroadcastOnce, BroadcastOnceReceiver},
        Error, ProducerClient,
    },
};

pub(super) type BatchWriteResult<A> = Result<Arc<AggregatedStatus<A>>, Error>;

/// The result of a batch Kafka write, and the deaggregator implementation to
/// demux the batch of responses to individual results produce() call.
#[derive(Debug)]
pub(super) struct AggregatedStatus<A>
where
    A: Aggregator,
{
    aggregated_status: Vec<i64>,
    status_deagg: <A as Aggregator>::StatusDeaggregator,
}

/// A result handle obtained by adding an input to the aggregator.
///
/// Holders of this handle can use it to obtain the produce result once the
/// aggregated batch is wrote to Kafka.
#[derive(Debug)]
pub(crate) struct ResultHandle<A>
where
    A: Aggregator,
{
    receiver: BroadcastOnceReceiver<BatchWriteResult<A>>,
    tag: A::Tag,
}

impl<A> ResultHandle<A>
where
    A: Aggregator,
{
    /// Construct a new [`ResultHandle`] that waits on `receiver` and presents
    /// `tag` to demux the caller's response from the batched result.
    fn new(receiver: BroadcastOnceReceiver<BatchWriteResult<A>>, tag: A::Tag) -> Self {
        Self { receiver, tag }
    }

    /// Wait for the aggregated batch to be wrote to Kafka (or fail).
    pub(super) async fn wait(&mut self) -> Result<BatchWriteResult<A>, Error> {
        self.receiver
            .receive()
            .await
            .map_err(|e| Error::FlushError(e.to_string()))
    }

    /// Return the demuxed result of the produce() call.
    pub(super) fn result(
        self,
        status: BatchWriteResult<A>,
    ) -> Result<<A as aggregator::AggregatorStatus>::Status, Error> {
        let status = status?;
        status
            .status_deagg
            .deaggregate(&status.aggregated_status, self.tag)
            .map_err(|e| Error::Aggregator(e.into()))
    }
}

/// A call to [`BatchBuilder::background_flush()`] can either succeed or fail,
/// and a new [`BatchBuilder`] is always returned for the next set of writes.
pub(crate) enum FlushResult<T> {
    Ok(T, Option<JoinHandle<()>>),
    Error(T, Error),
}

/// A [`BatchBuilder`] uses an [`Aggregator`] to construct maximally large batch
/// of writes, and returning a [`ResultHandle`] for callers to demux the result.
///
/// Once flushed by a caller, the [`BatchBuilder`] spawns a task to
/// asynchronously write the batch to Kafka, immediately returning a new
/// instance of [`BatchBuilder`] to begin buffering up a new batch.
#[derive(Debug)]
pub(crate) struct BatchBuilder<A>
where
    A: Aggregator,
{
    aggregator: A,
    results: BroadcastOnce<BatchWriteResult<A>>,
}

impl<A> BatchBuilder<A>
where
    A: Aggregator,
{
    pub(crate) fn new(aggregator: A) -> Self {
        Self {
            aggregator,
            results: Default::default(),
        }
    }

    /// Attempt to place this `data` into the underlying [`Aggregator`] impl.
    ///
    /// Returns a handle to obtain the write result if successful. If
    /// [`TryPush::NoCapacity`] is returned, subsequent calls to
    /// [`Self::try_push()`] will likely fail and this batch should be flushed
    /// by calling [`Self::background_flush()`].
    pub(super) fn try_push(
        &mut self,
        data: A::Input,
    ) -> Result<TryPush<A::Input, ResultHandle<A>>, Error> {
        match self
            .aggregator
            .try_push(data)
            .map_err(|e| Error::Aggregator(e.into()))?
        {
            TryPush::NoCapacity(data) => Ok(TryPush::NoCapacity(data)),
            TryPush::Aggregated(tag) => Ok(TryPush::Aggregated(ResultHandle::new(
                self.results.receiver(),
                tag,
            ))),
        }
    }

    /// Perform an asynchronous flush of this buffer.
    ///
    /// Returns a handle to the async flush task if a flush was necessary.
    pub(super) fn background_flush(
        mut self,
        client: Arc<dyn ProducerClient>,
        compression: Compression,
    ) -> FlushResult<Self> {
        let (batch, status_deagg) = match self.aggregator.flush() {
            Ok(v) => v,
            Err(e) => {
                return FlushResult::Error(Self::new(self.aggregator), Error::Aggregator(e.into()))
            }
        };

        if batch.is_empty() {
            debug!(?client, "No data aggregated, skipping client request");
            // A custom aggregator might have produced no records, but the
            // the `produce()` callers are still waiting for their
            // responses.
            //
            // Broadcast an empty result set to satisfy the aggregation
            // contract.
            self.results.broadcast(Ok(Arc::new(AggregatedStatus {
                aggregated_status: vec![],
                status_deagg,
            })));
            return FlushResult::Ok(Self::new(self.aggregator), None);
        }

        let handle = tokio::spawn({
            let broadcast = self.results;
            async move {
                let res = match client.produce(batch, compression).await {
                    Ok(status) => Ok(Arc::new(AggregatedStatus {
                        aggregated_status: status,
                        status_deagg,
                    })),
                    Err(e) => {
                        error!(?client, error=?e, "Failed to produce records");
                        Err(Error::Client(Arc::new(e)))
                    }
                };

                broadcast.broadcast(res);
            }
        });

        FlushResult::Ok(Self::new(self.aggregator), Some(handle))
    }
}
