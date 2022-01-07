use crate::{
    client::{error::Result, partition::PartitionClient},
    record::RecordAndOffset,
};
use futures::future::{BoxFuture, Fuse, FusedFuture, FutureExt};
use futures::Stream;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct StreamConsumerBuilder {
    client: Arc<PartitionClient>,

    start_offset: i64,

    max_wait_ms: i32,

    min_batch_size: i32,

    max_batch_size: i32,
}

impl StreamConsumerBuilder {
    pub fn new(client: Arc<PartitionClient>, start_offset: i64) -> Self {
        Self {
            client,
            start_offset,
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
    pub fn max_wait_ms(self, max_wait_ms: i32) -> Self {
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
            last_high_watermark: -1,
            buffer: Default::default(),
            fetch_fut: Fuse::terminated(),
        }
    }
}

type FetchResult = Result<(Vec<RecordAndOffset>, i64)>;

#[pin_project]
pub struct StreamConsumer {
    client: Arc<PartitionClient>,

    min_batch_size: i32,

    max_batch_size: i32,

    max_wait_ms: i32,

    next_offset: i64,

    last_high_watermark: i64,

    buffer: VecDeque<RecordAndOffset>,

    fetch_fut: Fuse<BoxFuture<'static, FetchResult>>,
}

impl Stream for StreamConsumer {
    type Item = Result<(RecordAndOffset, i64)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            if let Some(x) = this.buffer.pop_front() {
                return Poll::Ready(Some(Ok((x, *this.last_high_watermark))));
            }

            if this.fetch_fut.is_terminated() {
                let offset = *this.next_offset;
                let bytes = (*this.min_batch_size)..(*this.max_batch_size);
                let max_wait_ms = *this.max_wait_ms;
                let client = Arc::clone(this.client);

                println!("Fetching records at offset: {}", offset);

                *this.fetch_fut = FutureExt::fuse(Box::pin(async move {
                    client.fetch_records(offset, bytes, max_wait_ms).await
                }));
            }

            let data: FetchResult = futures::ready!(this.fetch_fut.poll_unpin(cx));

            match data {
                Ok((mut v, watermark)) => {
                    println!(
                        "Received {} records and a high watermark of {}",
                        v.len(),
                        watermark
                    );

                    v.sort_by_key(|x| x.offset);
                    *this.last_high_watermark = watermark;
                    if let Some(x) = v.last() {
                        *this.next_offset = x.offset + 1;
                        this.buffer.extend(v.into_iter())
                    }
                    continue;
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}
