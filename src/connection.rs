use rand::prelude::*;
use std::sync::Arc;
use tracing::{debug, warn};

use thiserror::Error;
use tokio::io::BufStream;
use tokio::sync::Mutex;

use crate::backoff::{Backoff, BackoffConfig};
use crate::connection::topology::BrokerTopology;
use crate::connection::transport::Transport;
use crate::messenger::{Messenger, RequestError};
use crate::protocol::messages::{MetadataRequest, MetadataRequestTopic, MetadataResponse};
use crate::protocol::primitives::String_;

mod topology;
mod transport;

/// A connection to a broker
pub type BrokerConnection = Arc<Messenger<BufStream<transport::Transport>>>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error getting cluster metadata: {0}")]
    Metadata(RequestError),

    #[error("error connecting to broker \"{broker}\": {error}")]
    Transport {
        broker: String,
        error: transport::Error,
    },

    #[error(transparent)]
    SyncVersions(#[from] crate::messenger::SyncVersionsError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Caches the broker topology and provides the ability to
///
/// * Get a cached connection to an arbitrary broker
/// * Obtain a connection to a specific broker
///
/// Maintains a list of brokers within the cluster and caches a connection to a broker
pub struct BrokerConnector {
    /// Brokers used to boostrap this pool
    bootstrap_brokers: Vec<String>,

    /// Discovered brokers in the cluster, including bootstrap brokers
    topology: BrokerTopology,

    /// The current cached broker
    current_broker: Mutex<Option<BrokerConnection>>,

    /// The backoff configuration on error
    backoff_config: BackoffConfig,

    /// TLS configuration if any
    tls_config: Option<Arc<rustls::ClientConfig>>,

    /// Maximum message size for framing protocol.
    max_message_size: usize,
}

impl BrokerConnector {
    pub fn new(
        bootstrap_brokers: Vec<String>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
        max_message_size: usize,
    ) -> Self {
        Self {
            bootstrap_brokers,
            topology: Default::default(),
            current_broker: Mutex::new(None),
            backoff_config: Default::default(),
            tls_config,
            max_message_size,
        }
    }

    /// Fetch and cache broker metadata
    pub async fn refresh_metadata(&self) -> Result<()> {
        // Not interested in topic metadata
        self.request_metadata(Some(vec![])).await?;
        Ok(())
    }

    /// Requests metadata for the provided topics, updating the cached broker information
    ///
    /// Requests data for all topics if `topics` is `None`
    pub async fn request_metadata(&self, topics: Option<Vec<String>>) -> Result<MetadataResponse> {
        let mut backoff = Backoff::new(&self.backoff_config);
        let request = MetadataRequest {
            topics: topics.map(|t| {
                t.into_iter()
                    .map(|x| MetadataRequestTopic { name: String_(x) })
                    .collect()
            }),
            allow_auto_topic_creation: None,
        };

        let response = loop {
            let broker = self.get_arbitrary_cached_broker().await?;
            let error = match broker.request(&request).await {
                Ok(response) => break response,
                Err(e @ RequestError::Poisoned(_) | e @ RequestError::IO(_)) => {
                    self.invalidate_cached_arbitrary_broker().await;
                    e
                }
                Err(error) => {
                    println!("metadata request encountered fatal error: {}", error);
                    return Err(Error::Metadata(error));
                }
            };

            let backoff = backoff.next();
            println!(
                "metadata request encountered non-fatal error \"{}\" - backing off for {} seconds",
                error,
                backoff.as_secs()
            );
            tokio::time::sleep(backoff).await;
        };

        self.topology.update(&response.brokers);
        Ok(response)
    }

    /// Invalidates the current cached broker
    ///
    /// The next call to `[BrokerPool::get_cached_broker]` will get a new connection
    #[allow(dead_code)]
    pub async fn invalidate_cached_arbitrary_broker(&self) {
        self.current_broker.lock().await.take();
    }

    /// Returns a new connection to the broker with the provided id
    pub async fn connect(&self, broker_id: i32) -> Result<Option<BrokerConnection>> {
        match self.topology.get_broker_url(broker_id).await {
            Some(url) => Ok(Some(self.connect_impl(&url).await?)),
            None => Ok(None),
        }
    }

    async fn connect_impl(&self, url: &str) -> Result<BrokerConnection> {
        debug!(url, "Establishing new connection");
        let transport = Transport::connect(url, self.tls_config.clone())
            .await
            .map_err(|error| Error::Transport {
                broker: url.to_string(),
                error,
            })?;

        let messenger = Arc::new(Messenger::new(
            BufStream::new(transport),
            self.max_message_size,
        ));
        messenger.sync_versions().await?;
        Ok(messenger)
    }

    /// Gets a cached [`BrokerConnection`] to any broker
    pub async fn get_arbitrary_cached_broker(&self) -> Result<BrokerConnection> {
        let mut current_broker = self.current_broker.lock().await;
        if let Some(broker) = &*current_broker {
            return Ok(Arc::clone(broker));
        }

        let mut brokers = if self.topology.is_empty() {
            self.bootstrap_brokers.clone()
        } else {
            self.topology.get_broker_urls()
        };

        // Randomise search order to encourage different clients to choose different brokers
        brokers.shuffle(&mut thread_rng());

        let mut backoff = Backoff::new(&self.backoff_config);

        loop {
            for broker in &brokers {
                let connection = match self.connect_impl(broker).await {
                    Ok(transport) => transport,
                    Err(e) => {
                        warn!(%e, "Failed to connect to broker");
                        continue;
                    }
                };

                *current_broker = Some(Arc::clone(&connection));

                return Ok(connection);
            }

            let backoff = backoff.next();
            warn!(
                backoff_secs = backoff.as_secs(),
                "Failed to connect to any broker, backing off",
            );
            tokio::time::sleep(backoff).await;
        }
    }
}

impl std::fmt::Debug for BrokerConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tls_config: &'static str = match self.tls_config {
            Some(_) => "Some",
            None => "None",
        };
        f.debug_struct("BrokerConnector")
            .field("bootstrap_brokers", &self.bootstrap_brokers)
            .field("topology", &self.topology)
            .field("current_broker", &self.current_broker)
            .field("backoff_config", &self.backoff_config)
            .field("tls_config", &tls_config)
            .field("max_message_size", &self.max_message_size)
            .finish()
    }
}
