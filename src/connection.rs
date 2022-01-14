use async_trait::async_trait;
use rand::prelude::*;
use std::sync::Arc;
use thiserror::Error;
use tokio::{io::BufStream, sync::Mutex};
use tracing::{debug, error, info, warn};

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

    #[error("cannot sync versions: {0}")]
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

    /// The cached arbitrary broker.
    ///
    /// This one is used for metadata queries.
    cached_arbitrary_broker: Mutex<Option<BrokerConnection>>,

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
            cached_arbitrary_broker: Mutex::new(None),
            backoff_config: Default::default(),
            tls_config,
            max_message_size,
        }
    }

    /// Fetch and cache broker metadata
    pub async fn refresh_metadata(&self) -> Result<()> {
        // Not interested in topic metadata
        self.request_metadata(None, Some(vec![])).await?;
        Ok(())
    }

    /// Requests metadata for the provided topics, updating the cached broker information
    ///
    /// Requests data for all topics if `topics` is `None`
    ///
    /// If `broker_override` is provided this will request metadata from that specific broker.
    ///
    /// Note: overriding the broker prevents automatic handling of connection-invalidating errors,
    /// which will instead be returned to the caller
    pub async fn request_metadata(
        &self,
        broker_override: Option<BrokerConnection>,
        topics: Option<Vec<String>>,
    ) -> Result<MetadataResponse> {
        let backoff = Backoff::new(&self.backoff_config);
        let request = MetadataRequest {
            topics: topics.map(|t| {
                t.into_iter()
                    .map(|x| MetadataRequestTopic { name: String_(x) })
                    .collect()
            }),
            allow_auto_topic_creation: None,
        };

        let response = metadata_request_loop(
            broker_override,
            &request,
            backoff,
            || self.get_cached_arbitrary_broker(),
            || self.invalidate_cached_arbitrary_broker(),
        )
        .await?;

        // Since the metadata request contains information about the cluster state, use it to update our view.
        self.topology.update(&response.brokers);

        Ok(response)
    }

    /// Invalidates the cached arbitrary broker.
    ///
    /// The next call to `[BrokerConnector::get_cached_arbitrary_broker]` will get a new connection
    pub async fn invalidate_cached_arbitrary_broker(&self) {
        debug!("Invalidating cached arbitrary broker");
        self.cached_arbitrary_broker.lock().await.take();
    }

    /// Returns a new connection to the broker with the provided id
    pub async fn connect(&self, broker_id: i32) -> Result<Option<BrokerConnection>> {
        match self.topology.get_broker_url(broker_id).await {
            Some(url) => Ok(Some(self.connect_impl(Some(broker_id), &url).await?)),
            None => Ok(None),
        }
    }

    async fn connect_impl(&self, broker_id: Option<i32>, url: &str) -> Result<BrokerConnection> {
        info!(broker = broker_id, url, "Establishing new connection",);
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
    pub async fn get_cached_arbitrary_broker(&self) -> Result<BrokerConnection> {
        let mut current_broker = self.cached_arbitrary_broker.lock().await;
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
                let connection = match self.connect_impl(None, broker).await {
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
            .field("cached_arbitrary_broker", &self.cached_arbitrary_broker)
            .field("backoff_config", &self.backoff_config)
            .field("tls_config", &tls_config)
            .field("max_message_size", &self.max_message_size)
            .finish()
    }
}

#[async_trait]
trait Connect {
    async fn metadata_request(
        &self,
        request_params: &MetadataRequest,
    ) -> Result<MetadataResponse, RequestError>;
}

#[async_trait]
impl Connect for Messenger<BufStream<transport::Transport>> {
    async fn metadata_request(
        &self,
        request_params: &MetadataRequest,
    ) -> Result<MetadataResponse, RequestError> {
        self.request(request_params).await
    }
}

async fn metadata_request_loop<G, G1, C, H, H1>(
    broker_override: Option<Arc<C>>,
    request_params: &MetadataRequest,
    mut backoff: Backoff,
    get_cached_arbitrary_broker: G,
    invalidate_cached_arbitrary_broker: H,
) -> Result<MetadataResponse>
where
    G: Fn() -> G1,
    G1: std::future::Future<Output = Result<Arc<C>>>,
    C: Connect,
    H: Fn() -> H1,
    H1: std::future::Future<Output = ()>,
{
    loop {
        // Retrieve the broker within the loop, in case it is invalidated
        let broker = match broker_override.as_ref() {
            Some(b) => Arc::clone(b),
            None => get_cached_arbitrary_broker().await?,
        };

        let error = match broker.metadata_request(&request_params).await {
            Ok(response) => break Ok(response),
            Err(e @ RequestError::Poisoned(_) | e @ RequestError::IO(_))
                if broker_override.is_none() =>
            {
                invalidate_cached_arbitrary_broker().await;
                e
            }
            Err(error) => {
                error!(
                    e=%error,
                    "metadata request encountered fatal error",
                );
                return Err(Error::Metadata(error));
            }
        };

        let backoff = backoff.next();
        info!(
            e=%error,
            backoff_secs = backoff.as_secs(),
            "metadata request encountered non-fatal error - backing off",
        );
        tokio::time::sleep(backoff).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeBroker {}

    #[async_trait]
    impl Connect for FakeBroker {
        async fn metadata_request(
            &self,
            _request_params: &MetadataRequest,
        ) -> Result<MetadataResponse, RequestError> {
            Ok(arbitrary_metadata_response())
        }
    }

    #[tokio::test]
    async fn happy_cached_broker() {
        let metadata_request = arbitrary_metadata_request();
        let success_response = arbitrary_metadata_response();

        let result = metadata_request_loop(
            None,
            &metadata_request,
            Backoff::new(&Default::default()),
            || async { Ok(Arc::new(FakeBroker {})) },
            || async {},
        )
        .await
        .unwrap();

        assert_eq!(success_response, result)
    }

    fn arbitrary_metadata_request() -> MetadataRequest {
        MetadataRequest {
            topics: Default::default(),
            allow_auto_topic_creation: Default::default(),
        }
    }

    fn arbitrary_metadata_response() -> MetadataResponse {
        MetadataResponse {
            throttle_time_ms: Default::default(),
            brokers: Default::default(),
            cluster_id: Default::default(),
            controller_id: Default::default(),
            topics: Default::default(),
        }
    }
}
