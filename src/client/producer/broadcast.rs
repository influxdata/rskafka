use futures::future::{Future, FutureExt};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;
use tracing::warn;

/// A broadcast channel that can send a single value
///
/// - Receivers can be created with `BroadcastOnce::receiver`
/// - The value can be produced with `BroadcastOnce::broadcast`
pub struct BroadcastOnce<T: Clone> {
    receiver: futures::future::Shared<ReceiveFut<T>>,
    sender: oneshot::Sender<T>,
}

impl<T: Clone> std::fmt::Debug for BroadcastOnce<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultSlot")
    }
}

impl<T: Clone> Default for BroadcastOnce<T> {
    fn default() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self {
            receiver: ReceiveFut { inner: receiver }.shared(),
            sender,
        }
    }
}

impl<T: Clone> BroadcastOnce<T> {
    /// Returns a [`Future`] that completes when [`BroadcastOnce::broadcast`] is called
    pub fn receive(&self) -> futures::future::Shared<impl Future<Output = T>> {
        self.receiver.clone()
    }

    /// Broadcast a value to all [`BroadcastOnce::receive`] handles
    pub fn broadcast(self, r: T) {
        // Don't care if no receivers
        let _ = self.sender.send(r);
    }
}

pin_project! {
    /// A future that completes when [`BroadcastOnce::broadcast`] is called
    struct ReceiveFut<T>{
        #[pin]
        inner: oneshot::Receiver<T>,
    }
}

impl<T: Clone> std::future::Future for ReceiveFut<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match futures::ready!(self.project().inner.poll(cx)) {
            Ok(x) => Poll::Ready(x),
            Err(_) => {
                warn!("producer dropped without signalling result");
                // We don't know the outcome of the publish, most likely
                // the producer panicked, and so return Pending
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_broadcast_once() {
        // Test no receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        broadcast.broadcast(1);

        // Test simple receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        let receiver = broadcast.receive();
        broadcast.broadcast(2);
        assert_eq!(receiver.await, 2);

        // Test multiple receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        let r1 = broadcast.receive();
        let r2 = broadcast.receive();
        broadcast.broadcast(4);
        assert_eq!(r1.await, 4);
        assert_eq!(r2.await, 4);

        // Test drop
        let broadcast: BroadcastOnce<usize> = Default::default();
        let r1 = broadcast.receive();
        let r2 = broadcast.receive();
        assert!(r1.now_or_never().is_none());
        broadcast.broadcast(5);
        assert_eq!(r2.await, 5);
    }
}
