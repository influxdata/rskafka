use futures::future::FutureExt;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;
use tracing::warn;

/// A broadcast channel that can send a single value
///
/// - Receivers can be created with `BroadcastOnce::receiver`
/// - The value can be produced with `BroadcastOnce::broadcast`
pub struct BroadcastOnce<T> {
    receiver: ReceiveFut<T>,
    sender: oneshot::Sender<T>,
}

impl<T> std::fmt::Debug for BroadcastOnce<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultSlot")
    }
}

impl<T: Clone> Default for BroadcastOnce<T> {
    fn default() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self {
            receiver: OneshotFut { inner: receiver }.shared(),
            sender,
        }
    }
}

impl<T: Clone> BroadcastOnce<T> {
    /// Returns a [`Future`] that completes when [`BroadcastOnce::broadcast`] is called
    pub fn receive(&self) -> ReceiveFut<T> {
        self.receiver.clone()
    }

    /// Broadcast a value to all [`BroadcastOnce::receive`] handles
    pub fn broadcast(self, r: T) {
        // Don't care if no receivers
        let _ = self.sender.send(r);
    }
}

pub type ReceiveFut<T> = futures::future::Shared<OneshotFut<T>>;

pin_project! {
    /// A future that completes when [`BroadcastOnce::broadcast`] is called
    pub struct OneshotFut<T>{
        #[pin]
        inner: oneshot::Receiver<T>,
    }
}

impl<T> std::future::Future for OneshotFut<T> {
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
    use std::future::Future;
    use std::time::Duration;

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
        let mut r1 = broadcast.receive();
        let r2 = broadcast.receive();
        assert_is_pending(&mut r1).await;
        broadcast.broadcast(5);
        assert_eq!(r2.await, 5);
    }

    async fn assert_is_pending<F, T>(f: &mut F)
    where
        F: Future<Output = T> + Send + Unpin,
        T: std::fmt::Debug,
    {
        tokio::select! {
            x = f => panic!("future is not pending, yielded: {x:?}"),
            _ = tokio::time::sleep(Duration::from_millis(1)) => {},
        };
    }
}
