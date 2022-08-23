use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Notify;
use tracing::warn;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("BroadcastOnce dropped")]
    Dropped,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A broadcast channel that can send a single value
///
/// - Receivers can be created with [`BroadcastOnce::receiver`]
/// - The value can be produced with [`BroadcastOnce::broadcast`]
#[derive(Debug)]
pub struct BroadcastOnce<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Default for BroadcastOnce<T> {
    fn default() -> Self {
        Self {
            shared: Arc::new(Shared {
                data: Default::default(),
                notify: Default::default(),
            }),
        }
    }
}

#[derive(Debug)]
struct Shared<T> {
    data: Mutex<Option<Result<T>>>,
    notify: Notify,
}

impl<T: Clone> BroadcastOnce<T> {
    /// Returns a [`BroadcastOnceReceiver`] that can be used to wait on
    /// a call to [`BroadcastOnce::broadcast`] on this instance
    pub fn receiver(&self) -> BroadcastOnceReceiver<T> {
        BroadcastOnceReceiver {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Broadcast a value to all [`BroadcastOnceReceiver`] handles
    pub fn broadcast(self, r: T) {
        let mut locked = self.shared.data.lock();
        assert!(locked.is_none(), "double publish");

        *locked = Some(Ok(r));
        self.shared.notify.notify_waiters();
    }
}

impl<T> Drop for BroadcastOnce<T> {
    fn drop(&mut self) {
        let mut data = self.shared.data.lock();
        if data.is_none() {
            warn!("BroadcastOnce dropped without producing");
            *data = Some(Err(Error::Dropped));
            self.shared.notify.notify_waiters();
        }
    }
}

#[derive(Debug, Clone)]
pub struct BroadcastOnceReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T: Clone + Send> BroadcastOnceReceiver<T> {
    /// Returns `Some(_)` if data has been produced
    pub fn peek(&self) -> Option<Result<T>> {
        self.shared.data.lock().clone()
    }

    /// Waits for [`BroadcastOnce::broadcast`] to be called or returns an error
    /// if the [`BroadcastOnce`] is dropped without a value being published
    pub async fn receive(&self) -> Result<T> {
        let notified = self.shared.notify.notified();

        if let Some(v) = self.peek() {
            return v;
        }

        notified.await;

        self.peek().expect("just got notified")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_broadcast_once() {
        // Test no receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        broadcast.broadcast(1);

        // Test simple receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        let receiver = broadcast.receiver();
        assert!(receiver.peek().is_none());

        // Should timeout as no values produced
        tokio::time::timeout(Duration::from_millis(1), receiver.receive())
            .await
            .unwrap_err();

        // Produce to slot
        broadcast.broadcast(2);

        assert_eq!(receiver.peek().unwrap(), Ok(2));
        assert_eq!(receiver.peek().unwrap(), Ok(2)); // can peek multiple times
        assert_eq!(receiver.receive().await, Ok(2));
        assert_eq!(receiver.receive().await, Ok(2)); // can receive multiple times

        // Test multiple receiver
        let broadcast: BroadcastOnce<usize> = Default::default();
        let r1 = broadcast.receiver();
        let r2 = broadcast.receiver();
        broadcast.broadcast(4);
        assert_eq!(r1.receive().await, Ok(4));
        assert_eq!(r2.receive().await, Ok(4));

        // Test drop
        let broadcast: BroadcastOnce<usize> = Default::default();
        let r1 = broadcast.receiver();
        let r2 = broadcast.receiver();
        assert!(r1.peek().is_none());
        std::mem::drop(broadcast);

        assert_eq!(r1.receive().await, Err(Error::Dropped));
        assert_eq!(r2.receive().await, Err(Error::Dropped));
    }
}
