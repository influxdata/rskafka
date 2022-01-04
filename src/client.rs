use crate::connection::BrokerPool;

pub struct Client {
    #[allow(dead_code)]
    brokers: BrokerPool,
}

impl Client {
    /// Create a new [`Client`] with the list of bootstrap brokers
    pub async fn new(boostrap_brokers: Vec<String>) -> Self {
        let mut brokers = BrokerPool::new(boostrap_brokers);
        brokers.refresh_metadata().await.unwrap();

        Self { brokers }
    }
}
