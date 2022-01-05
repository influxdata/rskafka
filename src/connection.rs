use std::sync::Arc;

use thiserror::Error;
use tokio::io::BufStream;
use tokio::sync::Mutex;

use crate::backoff::{Backoff, BackoffConfig};
use crate::connection::transport::Transport;
use crate::messenger::{Messenger, RequestError};
use crate::protocol::messages::MetadataRequest;

mod transport;

/// A connection to a broker
pub type BrokerConnection = Arc<Messenger<BufStream<transport::Transport>>>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error getting cluster metadata: {0}")]
    MetadataError(RequestError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Maintains a list of brokers within the cluster and caches a connection to a broker
pub struct BrokerPool {
    /// Brokers used to boostrap this pool
    bootstrap_brokers: Vec<String>,
    /// Discovered brokers in the cluster, including bootstrap brokers
    discovered_brokers: Vec<String>,
    /// The current cached broker
    current_broker: Mutex<Option<BrokerConnection>>,
    /// The backoff configuration on error
    backoff_config: BackoffConfig,
    /// TLS configuration if any
    tls_config: Option<Arc<rustls::ClientConfig>>,
}

impl BrokerPool {
    pub fn new(
        bootstrap_brokers: Vec<String>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Self {
        Self {
            bootstrap_brokers,
            discovered_brokers: vec![],
            current_broker: Mutex::new(None),
            backoff_config: Default::default(),
            tls_config,
        }
    }

    /// Fetch and cache broker metadata
    pub async fn refresh_metadata(&mut self) -> Result<()> {
        let broker = self.get_cached_broker().await?;

        let response = broker
            .request(MetadataRequest {
                topics: vec![],
                allow_auto_topic_creation: None,
            })
            .await
            .map_err(Error::MetadataError)?;

        self.discovered_brokers = response
            .brokers
            .into_iter()
            .map(|broker| format!("{}:{}", broker.host.0, broker.port.0))
            .collect();
        println!("Found brokers: {:?}", self.discovered_brokers);

        Ok(())
    }

    /// Invalidates the current cached broker
    ///
    /// The next call to `[BrokerPool::get_cached_broker]` will get a new connection
    #[allow(dead_code)]
    pub async fn invalidate_cached_broker(&self) {
        self.current_broker.lock().await.take();
    }

    /// Gets a cached [`BrokerConnection`] to any broker
    pub async fn get_cached_broker(&self) -> Result<BrokerConnection> {
        let mut current_broker = self.current_broker.lock().await;
        if let Some(broker) = &*current_broker {
            return Ok(Arc::clone(broker));
        }

        let brokers = if self.discovered_brokers.is_empty() {
            &self.bootstrap_brokers
        } else {
            &self.discovered_brokers
        };

        let mut backoff = Backoff::new(&self.backoff_config);

        loop {
            for broker in brokers {
                let transport = match Transport::connect(broker, self.tls_config.clone()).await {
                    Ok(transport) => transport,
                    Err(e) => {
                        println!("Error connecting to broker {}: {}", broker, e);
                        continue;
                    }
                };

                let messenger = Arc::new(Messenger::new(BufStream::new(transport)));
                messenger.sync_versions().await;

                *current_broker = Some(Arc::clone(&messenger));

                return Ok(messenger);
            }

            let backoff = backoff.next();
            println!(
                "Failed to connect to any broker, backing off for {} seconds",
                backoff.as_secs()
            );
            tokio::time::sleep(backoff).await;
        }
    }
}
