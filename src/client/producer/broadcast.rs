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
pub struct BroadcastOnce<T>
where
    T: Clone + Send + Sync,
{
    receiver: futures::future::Shared<ReceiveFut<T>>,
    sender: oneshot::Sender<T>,
}

impl<T> std::fmt::Debug for BroadcastOnce<T>
where
    T: Clone + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultSlot")
    }
}

impl<T> Default for BroadcastOnce<T>
where
    T: Clone + Send + Sync,
{
    fn default() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self {
            receiver: ReceiveFut { inner: receiver }.shared(),
            sender,
        }
    }
}

impl<T> BroadcastOnce<T>
where
    T: Clone + Send + Sync,
{
    /// Returns a [`Future`] that completes when [`BroadcastOnce::broadcast`] is called
    pub fn receive(&self) -> futures::future::Shared<impl Future<Output = T>> {
        self.receiver.clone()
    }

    /// Broadcast a value to all [`BroadcastOnce::receive`] handles.
    ///
    /// You may [`peek`](futures::future::Shared::peek) the [`receive`](Self::receive) future instantly after calling
    /// this method and get a result back.
    pub async fn broadcast(self, r: T) {
        // Don't care if no receivers
        {
            self.sender.send(r).ok();
        }

        // We need to poll the result slot here to make the result available via `peek`, otherwise the next thread in
        // this critical section will flush the producer a 2nd time. We are using an ordinary `.await` call here instead
        // of `now_or_never` because tokio might preempt us (or to be precise: might preempt polling from the result
        // slot).
        self.receiver.await;
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
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_broadcast_once() {
        // Test no receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        broadcast.broadcast(1).await;

        // Test simple receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        let receiver = broadcast.receive();
        broadcast.broadcast(2).await;
        assert_eq!(receiver.await, 2);

        // Test multiple receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        let r1 = broadcast.receive();
        let r2 = broadcast.receive();
        broadcast.broadcast(4).await;
        assert_eq!(r1.await, 4);
        assert_eq!(r2.await, 4);

        // Test drop
        let broadcast: BroadcastOnce<usize> = Default::default();
        let mut r1 = broadcast.receive();
        let r2 = broadcast.receive();
        assert_is_pending(&mut r1).await;
        broadcast.broadcast(5).await;
        assert_eq!(r2.await, 5);
    }

    #[tokio::test]
    async fn test_peek() {
        let broadcast: BroadcastOnce<usize> = Default::default();
        let r1 = broadcast.receive();
        let r2 = broadcast.receive();
        broadcast.broadcast(1).await;
        assert_eq!(r1.peek().unwrap(), &1);
        assert_eq!(r2.peek().unwrap(), &1);
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
