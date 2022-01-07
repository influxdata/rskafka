use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{pin_mut, FutureExt};
use pin_project::pin_project;
use thiserror::Error;
use tokio::sync::{oneshot, Mutex};

use crate::client::{error::Error as ClientError, partition::PartitionClient};
use crate::record::Record;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Reducer error: {0}")]
    Reducer(#[from] ReducerError),

    #[error("Client error: {0}")]
    Client(#[from] Arc<ClientError>),

    #[error("Input too large for reducer")]
    TooLarge,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The error returned by [`Reducer`] implementations
pub type ReducerError = Box<dyn std::error::Error + Send + Sync>;

/// A type that receives one or more input and returns a single output
pub trait Reducer {
    type Input;

    type Output;

    /// Try to append `record` implementations should return
    ///
    /// - `Ok(None)` on success
    /// - `Ok(Some(record))` if there is insufficient capacity in the `Reducer`
    /// - `Err(_)` if an error is encountered
    ///
    /// [`Reducer`] must only be modified if this method returns `Ok(None)`
    ///
    fn try_push(&mut self, record: Self::Input) -> Result<Option<Self::Input>, ReducerError>;

    /// Flush the contents of this aggregator to Kafka
    fn flush(&mut self) -> Self::Output;
}

/// a [`Reducer`] that batches up to a certain number of bytes of [`Record`]
pub struct RecordReducer {
    max_batch_size: usize,
    batch_size: usize,
    records: Vec<Record>,
}

impl Reducer for RecordReducer {
    type Input = Record;
    type Output = Vec<Record>;

    fn try_push(&mut self, record: Self::Input) -> Result<Option<Self::Input>, ReducerError> {
        let record_size: usize = record.approximate_size();

        if self.batch_size + record_size > self.max_batch_size {
            return Ok(Some(record));
        }

        self.batch_size += record_size;
        self.records.push(record);

        Ok(None)
    }

    fn flush(&mut self) -> Self::Output {
        self.batch_size = 0;
        std::mem::take(&mut self.records)
    }
}

impl RecordReducer {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            batch_size: 0,
            records: vec![],
        }
    }
}

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

    pub fn build<A>(self, reducer: A) -> BatchProducer<A> {
        BatchProducer {
            linger: self.linger,
            client: self.client,
            inner: Mutex::new(ProducerInner {
                reducer,
                result_slot: Default::default(),
            }),
        }
    }
}

/// [`BatchProducer`] attempts to aggregate multiple produce requests together
/// using the provided [`Reducer`]
///
/// It will buffer up records until either the linger time expires or [`Reducer`]
/// cannot accommodate another record.
///
/// At this point it will flush the [`Reducer`]
#[derive(Debug)]
pub struct BatchProducer<R> {
    linger: Duration,

    client: Arc<PartitionClient>,

    inner: Mutex<ProducerInner<R>>,
}

#[derive(Debug)]
struct ProducerInner<R> {
    result_slot: ResultSlot,

    reducer: R,
}

impl<R: Reducer<Output = Vec<Record>>> BatchProducer<R> {
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
    pub async fn produce(&self, data: R::Input) -> Result<()> {
        let result_slot = {
            // Try to add the record to the reducer
            let mut inner = self.inner.lock().await;
            if let Some(data) = inner.reducer.try_push(data)? {
                println!("Insufficient capacity in reducer - flushing");

                Self::flush(&mut inner, self.client.as_ref()).await;
                if inner.reducer.try_push(data)?.is_some() {
                    println!("Record too large for reducer");
                    return Err(Error::TooLarge);
                }
            }
            // Get a copy of the result slot for the next produce operation
            inner.result_slot.receiver.clone().fuse()
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
        if let Some(r) = result_slot.now_or_never() {
            return Ok(r?);
        }

        println!("Linger expired - flushing");

        // Flush data
        Self::flush(&mut inner, &self.client).await;
        Ok(())
    }

    /// Flushes out the data from the reducer, publishes the result to the result slot,
    /// and creates a fresh result slot for future writes to use
    async fn flush(inner: &mut ProducerInner<R>, client: &PartitionClient) {
        println!("Flushing batch producer");

        let output = inner.reducer.flush();
        if output.is_empty() {
            return;
        }

        let r = client.produce(output).await;

        // Reset result slot
        let slot = std::mem::take(&mut inner.result_slot);

        // Not concerned if receivers hung up
        let _ = match r {
            Ok(_) => slot.sender.send(Ok(())),
            Err(e) => slot.sender.send(Err(Arc::new(e))),
        };
    }
}

/// A future for the eventual production of a record
#[pin_project]
struct ProduceFut(#[pin] oneshot::Receiver<Result<(), Arc<ClientError>>>);

impl std::future::Future for ProduceFut {
    type Output = Result<(), Arc<ClientError>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(
            // This most likely means that the producer panicked
            // In such a case we don't know the outcome - so panic
            futures::ready!(self.project().0.poll(cx))
                .expect("producer dropped without signalling"),
        )
    }
}

struct ResultSlot {
    receiver: futures::future::Shared<ProduceFut>,
    sender: oneshot::Sender<Result<(), Arc<ClientError>>>,
}

impl std::fmt::Debug for ResultSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultSlot")
    }
}

impl Default for ResultSlot {
    fn default() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self {
            receiver: ProduceFut(receiver).shared(),
            sender,
        }
    }
}
