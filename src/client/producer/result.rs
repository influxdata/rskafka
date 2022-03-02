use crate::client::producer::broadcast::{BroadcastOnce, ReceiveFut};
use futures::future::FusedFuture;
use futures::prelude::*;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::debug;

/// A reusable [`BroadcastOnce`] that can be used to notify a group of producers
#[derive(Debug)]
pub struct ResultSlot<T> {
    current_generation: usize,
    flushed_generation: usize,
    broadcast: BroadcastOnce<T>,
}

impl<T: Clone> Default for ResultSlot<T> {
    fn default() -> Self {
        Self {
            current_generation: 1,
            flushed_generation: 0,
            broadcast: Default::default(),
        }
    }
}

impl<T: Clone> ResultSlot<T> {
    /// Returns the current generation of this result slot
    ///
    /// This is a number starting from 1 that is incremented on each call to `ResultSlot::produce`
    pub fn generation(&self) -> usize {
        self.current_generation
    }

    /// Returns a future that resolves with the result of the next call to `ResultSlot::produce`
    pub fn receive(&self) -> ResultSlotFuture<T> {
        ResultSlotFuture {
            inner: self.broadcast.receive(),
            generation: self.current_generation,
        }
    }

    /// Send a value to any consumers that have registered interest with `ResultSlot::receive`
    pub fn produce(&mut self, output: T) {
        assert_eq!(
            self.flushed_generation + 1,
            self.current_generation,
            "out of order flush"
        );

        debug!(
            generation = self.current_generation,
            "producing to result slot"
        );

        std::mem::take(&mut self.broadcast).broadcast(output);
        self.flushed_generation = self.current_generation;
        self.current_generation += 1;
    }
}

pin_project! {
    pub struct ResultSlotFuture<T> {
        #[pin]
        inner: ReceiveFut<T>,
        generation: usize,
    }
}

impl<T: Clone> ResultSlotFuture<T> {
    /// Returns the generation this future is waiting on
    pub fn generation(&self) -> usize {
        self.generation
    }

    /// Returns the value if it has already been computed
    pub fn peek(&self) -> Option<&T> {
        self.inner.peek()
    }
}

impl<T: Clone> Future for ResultSlotFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

impl<T: Clone> FusedFuture for ResultSlotFuture<T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}
