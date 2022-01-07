use crate::client::error::Error as ClientError;
use futures::future::{Future, FutureExt};
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// A broadcast channel that can send a single value
///
/// - Receivers can be created with `BroadcastOnce::receiver`
/// - The value can be produced with `BroadcastOnce::broadcast`
pub struct BroadcastOnce {
    receiver: futures::future::Shared<ReceiveFut>,
    sender: oneshot::Sender<Result<(), Arc<ClientError>>>,
}

impl std::fmt::Debug for BroadcastOnce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultSlot")
    }
}

impl Default for BroadcastOnce {
    fn default() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self {
            receiver: ReceiveFut(receiver).shared(),
            sender,
        }
    }
}

impl BroadcastOnce {
    /// Returns a [`Future`] that completes when [`BroadcastOnce::broadcast`] is called
    pub fn receive(&self) -> impl Future<Output = Result<(), Arc<ClientError>>> {
        self.receiver.clone()
    }

    /// Broadcast a value to all [`BroadcastOnce::receive`] handles
    pub fn broadcast(self, r: Result<(), Arc<ClientError>>) {
        // Don't care if no receivers
        let _ = self.sender.send(r);
    }
}

/// A future that completes when [`BroadcastOnce::broadcast`] is called
#[pin_project]
struct ReceiveFut(#[pin] oneshot::Receiver<Result<(), Arc<ClientError>>>);

impl std::future::Future for ReceiveFut {
    type Output = Result<(), Arc<ClientError>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(
            // Panic if the receiver returns an error as we don't know the
            // outcome of the publish. Most likely the producer panicked
            futures::ready!(self.project().0.poll(cx))
                .expect("producer dropped without signalling"),
        )
    }
}
