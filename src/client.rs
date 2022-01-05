use crate::connection::BrokerPool;
use std::sync::Arc;

pub struct Client {
    #[allow(dead_code)]
    brokers: BrokerPool,
}

impl Client {
    /// Create a new [`Client`] with the list of bootstrap brokers
    pub async fn new_plain(boostrap_brokers: Vec<String>) -> Self {
        Self::new(boostrap_brokers, None).await
    }

    /// Create a new [`Client`] using TLS
    pub async fn new_with_tls(
        boostrap_brokers: Vec<String>,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Self {
        Self::new(boostrap_brokers, Some(tls_config)).await
    }

    async fn new(
        boostrap_brokers: Vec<String>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Self {
        let mut brokers = BrokerPool::new(boostrap_brokers, tls_config);
        brokers.refresh_metadata().await.unwrap();

        Self { brokers }
    }
}
