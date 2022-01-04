use crate::connection::BrokerPool;

pub struct Client {
    #[allow(dead_code)]
    brokers: BrokerPool,
}

impl Client {
    /// Create a new [`Client`] with the list of bootstrap brokers
    pub async fn new<A, I>(boostrap_brokers: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Into<String>,
    {
        let mut brokers = BrokerPool::new(boostrap_brokers.into_iter().map(Into::into).collect());
        brokers.refresh_metadata().await.unwrap();

        Self { brokers }
    }
}
