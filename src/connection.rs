use async_trait::async_trait;
use rand::prelude::*;
use std::ops::ControlFlow;
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

pub use self::transport::TlsConfig;

mod topology;
mod transport;

/// A connection to a broker
pub type BrokerConnection = Arc<MessengerTransport>;
type MessengerTransport = Messenger<BufStream<transport::Transport>>;

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
    tls_config: TlsConfig,

    /// SOCKS5 proxy.
    socks5_proxy: Option<String>,

    /// Maximum message size for framing protocol.
    max_message_size: usize,
}

impl BrokerConnector {
    pub fn new(
        bootstrap_brokers: Vec<String>,
        tls_config: TlsConfig,
        socks5_proxy: Option<String>,
        max_message_size: usize,
    ) -> Self {
        Self {
            bootstrap_brokers,
            topology: Default::default(),
            cached_arbitrary_broker: Mutex::new(None),
            backoff_config: Default::default(),
            tls_config,
            socks5_proxy,
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

        let response =
            metadata_request_with_retry(broker_override, &request, backoff, self).await?;

        // Since the metadata request contains information about the cluster state, use it to update our view.
        self.topology.update(&response.brokers);

        Ok(response)
    }

    /// Returns a new connection to the broker with the provided id
    pub async fn connect(&self, broker_id: i32) -> Result<Option<BrokerConnection>> {
        match self.topology.get_broker(broker_id).await {
            Some(broker) => {
                let connection = new_broker_connection(
                    self.tls_config.clone(),
                    self.socks5_proxy.clone(),
                    self.max_message_size,
                    Some(broker_id),
                    &broker.to_string(),
                )
                .await?;
                Ok(Some(connection))
            }
            None => Ok(None),
        }
    }
}

async fn new_broker_connection(
    tls_config: TlsConfig,
    socks5_proxy: Option<String>,
    max_message_size: usize,
    broker_id: Option<i32>,
    url: &str,
) -> Result<BrokerConnection> {
    info!(broker = broker_id, url, "Establishing new connection",);
    let transport = Transport::connect(url, tls_config, socks5_proxy)
        .await
        .map_err(|error| Error::Transport {
            broker: url.to_string(),
            error,
        })?;

    let messenger = Arc::new(Messenger::new(BufStream::new(transport), max_message_size));
    messenger.sync_versions().await?;
    Ok(messenger)
}

impl std::fmt::Debug for BrokerConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerConnector")
            .field("bootstrap_brokers", &self.bootstrap_brokers)
            .field("topology", &self.topology)
            .field("cached_arbitrary_broker", &self.cached_arbitrary_broker)
            .field("backoff_config", &self.backoff_config)
            .field("tls_config", &"...")
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
impl Connect for MessengerTransport {
    async fn metadata_request(
        &self,
        request_params: &MetadataRequest,
    ) -> Result<MetadataResponse, RequestError> {
        self.request(request_params).await
    }
}

#[async_trait]
trait ArbitraryBrokerCache: Send + Sync {
    type C: Connect + Send + Sync;

    async fn get(&self) -> Result<Arc<Self::C>>;

    async fn invalidate(&self);
}

#[async_trait]
impl ArbitraryBrokerCache for &BrokerConnector {
    type C = MessengerTransport;

    async fn get(&self) -> Result<Arc<Self::C>> {
        let mut current_broker = self.cached_arbitrary_broker.lock().await;
        if let Some(broker) = &*current_broker {
            return Ok(Arc::clone(broker));
        }

        let mut brokers = if self.topology.is_empty() {
            self.bootstrap_brokers.clone()
        } else {
            self.topology
                .get_brokers()
                .iter()
                .map(ToString::to_string)
                .collect()
        };

        // Randomise search order to encourage different clients to choose different brokers
        brokers.shuffle(&mut thread_rng());

        let mut backoff = Backoff::new(&self.backoff_config);
        let connection = backoff
            .retry_with_backoff("broker_connect", || async {
                for broker in &brokers {
                    let conn = new_broker_connection(
                        self.tls_config.clone(),
                        self.socks5_proxy.clone(),
                        self.max_message_size,
                        None,
                        broker,
                    )
                    .await;

                    let connection = match conn {
                        Ok(transport) => transport,
                        Err(e) => {
                            warn!(%e, "Failed to connect to broker");
                            continue;
                        }
                    };

                    return ControlFlow::Break(connection);
                }
                ControlFlow::Continue("Failed to connect to any broker, backing off")
            })
            .await;

        *current_broker = Some(Arc::clone(&connection));
        Ok(connection)
    }

    async fn invalidate(&self) {
        debug!("Invalidating cached arbitrary broker");
        self.cached_arbitrary_broker.lock().await.take();
    }
}

async fn metadata_request_with_retry<A>(
    broker_override: Option<Arc<A::C>>,
    request_params: &MetadataRequest,
    mut backoff: Backoff,
    arbitrary_broker_cache: A,
) -> Result<MetadataResponse>
where
    A: ArbitraryBrokerCache,
{
    backoff
        .retry_with_backoff("metadata", || async {
            // Retrieve the broker within the loop, in case it is invalidated
            let broker = match broker_override.as_ref() {
                Some(b) => Arc::clone(b),
                None => match arbitrary_broker_cache.get().await {
                    Ok(inner) => inner,
                    Err(e) => return ControlFlow::Break(Err(e)),
                },
            };

            match broker.metadata_request(request_params).await {
                Ok(response) => ControlFlow::Break(Ok(response)),
                Err(e @ RequestError::Poisoned(_) | e @ RequestError::IO(_))
                    if broker_override.is_none() =>
                {
                    arbitrary_broker_cache.invalidate().await;
                    ControlFlow::Continue(e)
                }
                Err(error) => {
                    error!(
                        e=%error,
                        "metadata request encountered fatal error",
                    );
                    ControlFlow::Break(Err(Error::Metadata(error)))
                }
            }
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::api_key::ApiKey;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FakeBroker(Box<dyn Fn() -> Result<MetadataResponse, RequestError> + Send + Sync>);

    impl FakeBroker {
        fn success() -> Self {
            Self(Box::new(|| Ok(arbitrary_metadata_response())))
        }

        fn fatal_error() -> Self {
            Self(Box::new(|| Err(arbitrary_fatal_error())))
        }

        fn recoverable() -> Self {
            Self(Box::new(|| Err(arbitrary_recoverable_error())))
        }
    }

    #[async_trait]
    impl Connect for FakeBroker {
        async fn metadata_request(
            &self,
            _request_params: &MetadataRequest,
        ) -> Result<MetadataResponse, RequestError> {
            self.0()
        }
    }

    struct FakeBrokerCache {
        get: Box<dyn Fn() -> Result<Arc<FakeBroker>> + Send + Sync>,
        invalidate: Box<dyn Fn() + Send + Sync>,
    }

    #[async_trait]
    impl ArbitraryBrokerCache for FakeBrokerCache {
        type C = FakeBroker;

        async fn get(&self) -> Result<Arc<Self::C>> {
            (self.get)()
        }

        async fn invalidate(&self) {
            (self.invalidate)()
        }
    }

    #[tokio::test]
    async fn happy_cached_broker() {
        let metadata_request = arbitrary_metadata_request();
        let success_response = arbitrary_metadata_response();

        let broker_cache = FakeBrokerCache {
            get: Box::new(|| Ok(Arc::new(FakeBroker::success()))),
            invalidate: Box::new(|| {}),
        };

        let result = metadata_request_with_retry(
            None,
            &metadata_request,
            Backoff::new(&Default::default()),
            broker_cache,
        )
        .await
        .unwrap();

        assert_eq!(success_response, result)
    }

    #[tokio::test]
    async fn fatal_error_cached_broker() {
        let metadata_request = arbitrary_metadata_request();

        let broker_cache = FakeBrokerCache {
            get: Box::new(|| Ok(Arc::new(FakeBroker::fatal_error()))),
            invalidate: Box::new(|| {}),
        };

        let result = metadata_request_with_retry(
            None,
            &metadata_request,
            Backoff::new(&Default::default()),
            broker_cache,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            result,
            Error::Metadata(RequestError::NoVersionMatch { .. })
        ));
    }

    #[tokio::test]
    async fn sad_cached_broker() {
        // Start with always returning a broker that fails.
        let succeed = Arc::new(AtomicBool::new(false));

        let metadata_request = arbitrary_metadata_request();
        let success_response = arbitrary_metadata_response();

        let broker_cache = FakeBrokerCache {
            get: Box::new({
                let succeed = Arc::clone(&succeed);
                move || {
                    Ok(Arc::new(if succeed.load(Ordering::SeqCst) {
                        FakeBroker::success()
                    } else {
                        FakeBroker::recoverable()
                    }))
                }
            }),
            // If invalidate is called, switch to returning a broker that always succeeds.
            invalidate: Box::new({
                let succeed = Arc::clone(&succeed);
                move || succeed.store(true, Ordering::SeqCst)
            }),
        };

        let result = metadata_request_with_retry(
            None,
            &metadata_request,
            Backoff::new(&Default::default()),
            broker_cache,
        )
        .await
        .unwrap();

        assert_eq!(success_response, result)
    }

    #[tokio::test]
    async fn happy_broker_override() {
        let broker_override = Some(Arc::new(FakeBroker::success()));
        let metadata_request = arbitrary_metadata_request();
        let success_response = arbitrary_metadata_response();

        let broker_cache = FakeBrokerCache {
            get: Box::new(|| unreachable!()),
            invalidate: Box::new(|| unreachable!()),
        };

        let result = metadata_request_with_retry(
            broker_override,
            &metadata_request,
            Backoff::new(&Default::default()),
            broker_cache,
        )
        .await
        .unwrap();

        assert_eq!(success_response, result)
    }

    #[tokio::test]
    async fn sad_broker_override() {
        let broker_override = Some(Arc::new(FakeBroker::recoverable()));
        let metadata_request = arbitrary_metadata_request();

        let broker_cache = FakeBrokerCache {
            get: Box::new(|| unreachable!()),
            invalidate: Box::new(|| unreachable!()),
        };

        let result = metadata_request_with_retry(
            broker_override,
            &metadata_request,
            Backoff::new(&Default::default()),
            broker_cache,
        )
        .await
        .unwrap_err();

        assert!(matches!(result, Error::Metadata(RequestError::IO { .. })));
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

    fn arbitrary_fatal_error() -> RequestError {
        RequestError::NoVersionMatch {
            api_key: ApiKey::Metadata,
        }
    }

    fn arbitrary_recoverable_error() -> RequestError {
        RequestError::IO(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
    }
}
