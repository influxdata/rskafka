use async_trait::async_trait;
use rand::prelude::*;
use std::ops::ControlFlow;
use std::sync::Arc;
use thiserror::Error;
use tokio::{io::BufStream, sync::Mutex};
use tracing::{debug, error, info, warn};

use crate::backoff::{Backoff, BackoffConfig, BackoffError};
use crate::connection::topology::{Broker, BrokerTopology};
use crate::connection::transport::Transport;
use crate::messenger::{Messenger, RequestError};
use crate::protocol::messages::{MetadataRequest, MetadataRequestTopic, MetadataResponse};
use crate::protocol::primitives::String_;

pub use self::transport::SaslConfig;
pub use self::transport::TlsConfig;

mod topology;
mod transport;

/// A connection to a broker
pub type BrokerConnection = Arc<MessengerTransport>;
pub type MessengerTransport = Messenger<BufStream<transport::Transport>>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error getting cluster metadata: {0}")]
    Metadata(#[from] RequestError),

    #[error("error connecting to broker \"{broker}\": {error}")]
    Transport {
        broker: String,
        error: transport::Error,
    },

    #[error("cannot sync versions: {0}")]
    SyncVersions(#[from] crate::messenger::SyncVersionsError),

    #[error("all retries failed: {0}")]
    RetryFailed(BackoffError),

    #[error("Sasl handshake failed: {0}")]
    SaslFailed(#[from] crate::messenger::SaslError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// How to connect to a `Transport`
#[async_trait]
trait ConnectionHandler {
    type R: RequestHandler + Send + Sync;

    async fn connect(
        &self,
        tls_config: TlsConfig,
        socks5_proxy: Option<String>,
        sasl_config: Option<SaslConfig>,
        max_message_size: usize,
    ) -> Result<Arc<Self::R>>;
}

/// Info needed to connect to a broker, with [optional broker ID](Self::id) for debugging
enum BrokerRepresentation {
    /// URL specified as a bootstrap broker
    Bootstrap(String),

    /// Broker received from the cluster broker topology
    Topology(Broker),
}

impl BrokerRepresentation {
    fn id(&self) -> Option<i32> {
        match self {
            BrokerRepresentation::Bootstrap(_) => None,
            BrokerRepresentation::Topology(broker) => Some(broker.id),
        }
    }

    fn url(&self) -> String {
        match self {
            BrokerRepresentation::Bootstrap(inner) => inner.clone(),
            BrokerRepresentation::Topology(broker) => broker.to_string(),
        }
    }
}

#[async_trait]
impl ConnectionHandler for BrokerRepresentation {
    type R = MessengerTransport;

    async fn connect(
        &self,
        tls_config: TlsConfig,
        socks5_proxy: Option<String>,
        sasl_config: Option<SaslConfig>,
        max_message_size: usize,
    ) -> Result<Arc<Self::R>> {
        let url = self.url();
        info!(
            broker = self.id(),
            url = url.as_str(),
            "Establishing new connection",
        );
        let transport = Transport::connect(&url, tls_config, socks5_proxy)
            .await
            .map_err(|error| Error::Transport {
                broker: url.to_string(),
                error,
            })?;

        let messenger = Arc::new(Messenger::new(BufStream::new(transport), max_message_size));
        messenger.sync_versions().await?;
        if let Some(sasl_config) = sasl_config {
            messenger
                .sasl_handshake(sasl_config.mechanism(), sasl_config.auth_bytes())
                .await?;
        }
        Ok(messenger)
    }
}

/// Caches the broker topology and provides the ability to
///
/// * Get a cached connection to an arbitrary broker
/// * Obtain a connection to a specific broker
///
/// Maintains a list of brokers within the cluster and caches a connection to a broker
pub struct BrokerConnector {
    /// Broker URLs used to boostrap this pool
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

    /// SASL Configuration
    sasl_config: Option<SaslConfig>,

    /// Maximum message size for framing protocol.
    max_message_size: usize,
}

impl BrokerConnector {
    pub fn new(
        bootstrap_brokers: Vec<String>,
        tls_config: TlsConfig,
        socks5_proxy: Option<String>,
        sasl_config: Option<SaslConfig>,
        max_message_size: usize,
    ) -> Self {
        Self {
            bootstrap_brokers,
            topology: Default::default(),
            cached_arbitrary_broker: Mutex::new(None),
            backoff_config: Default::default(),
            tls_config,
            socks5_proxy,
            sasl_config,
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
                let connection = BrokerRepresentation::Topology(broker)
                    .connect(
                        self.tls_config.clone(),
                        self.socks5_proxy.clone(),
                        self.sasl_config.clone(),
                        self.max_message_size,
                    )
                    .await?;
                Ok(Some(connection))
            }
            None => Ok(None),
        }
    }

    /// Either the topology or the bootstrap brokers to be used as a connection
    fn brokers(&self) -> Vec<BrokerRepresentation> {
        if self.topology.is_empty() {
            self.bootstrap_brokers
                .iter()
                .cloned()
                .map(BrokerRepresentation::Bootstrap)
                .collect()
        } else {
            self.topology
                .get_brokers()
                .iter()
                .cloned()
                .map(BrokerRepresentation::Topology)
                .collect()
        }
    }
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
trait RequestHandler {
    async fn metadata_request(
        &self,
        request_params: &MetadataRequest,
    ) -> Result<MetadataResponse, RequestError>;
}

#[async_trait]
impl RequestHandler for MessengerTransport {
    async fn metadata_request(
        &self,
        request_params: &MetadataRequest,
    ) -> Result<MetadataResponse, RequestError> {
        self.request(request_params).await
    }
}

#[async_trait]
pub trait BrokerCache: Send + Sync {
    type R: Send + Sync;
    type E: std::error::Error + Send + Sync;

    async fn get(&self) -> Result<Arc<Self::R>, Self::E>;

    async fn invalidate(&self);
}

/// BrokerConnector caches an arbitrary broker that can successfully connect.
#[async_trait]
impl BrokerCache for &BrokerConnector {
    type R = MessengerTransport;
    type E = Error;

    async fn get(&self) -> Result<Arc<Self::R>, Self::E> {
        let mut current_broker = self.cached_arbitrary_broker.lock().await;
        if let Some(broker) = &*current_broker {
            return Ok(Arc::clone(broker));
        }

        let connection = connect_to_a_broker_with_retry(
            self.brokers(),
            &self.backoff_config,
            self.tls_config.clone(),
            self.socks5_proxy.clone(),
            self.sasl_config.clone(),
            self.max_message_size,
        )
        .await?;

        *current_broker = Some(Arc::clone(&connection));
        Ok(connection)
    }

    async fn invalidate(&self) {
        debug!("Invalidating cached arbitrary broker");
        self.cached_arbitrary_broker.lock().await.take();
    }
}

async fn connect_to_a_broker_with_retry<B>(
    mut brokers: Vec<B>,
    backoff_config: &BackoffConfig,
    tls_config: TlsConfig,
    socks5_proxy: Option<String>,
    sasl_config: Option<SaslConfig>,
    max_message_size: usize,
) -> Result<Arc<B::R>>
where
    B: ConnectionHandler + Send + Sync,
{
    // Randomise search order to encourage different clients to choose different brokers
    brokers.shuffle(&mut thread_rng());

    let mut backoff = Backoff::new(backoff_config);
    backoff
        .retry_with_backoff("broker_connect", || async {
            for broker in &brokers {
                let conn = broker
                    .connect(
                        tls_config.clone(),
                        socks5_proxy.clone(),
                        sasl_config.clone(),
                        max_message_size,
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

            let err = Box::<dyn std::error::Error + Send + Sync>::from(
                "Failed to connect to any broker, backing off".to_string(),
            );
            let err: Arc<dyn std::error::Error + Send + Sync> = err.into();
            ControlFlow::Continue(err)
        })
        .await
        .map_err(Error::RetryFailed)
}

async fn metadata_request_with_retry<A>(
    broker_override: Option<Arc<A::R>>,
    request_params: &MetadataRequest,
    mut backoff: Backoff,
    arbitrary_broker_cache: A,
) -> Result<MetadataResponse>
where
    A: BrokerCache,
    A::R: RequestHandler,
    Error: From<A::E>,
{
    backoff
        .retry_with_backoff("metadata", || async {
            // Retrieve the broker within the loop, in case it is invalidated
            let broker = match broker_override.as_ref() {
                Some(b) => Arc::clone(b),
                None => match arbitrary_broker_cache.get().await {
                    Ok(inner) => inner,
                    Err(e) => return ControlFlow::Break(Err(e.into())),
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
                    ControlFlow::Break(Err(error.into()))
                }
            }
        })
        .await
        .map_err(Error::RetryFailed)?
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
    impl RequestHandler for FakeBroker {
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
    impl BrokerCache for FakeBrokerCache {
        type R = FakeBroker;
        type E = Error;

        async fn get(&self) -> Result<Arc<Self::R>> {
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

    struct FakeBrokerRepresentation {
        conn: Box<dyn Fn() -> Result<Arc<FakeConn>> + Send + Sync>,
    }

    #[derive(Debug, PartialEq)]
    struct FakeConn;

    #[async_trait]
    impl RequestHandler for FakeConn {
        async fn metadata_request(
            &self,
            _request_params: &MetadataRequest,
        ) -> Result<MetadataResponse, RequestError> {
            unreachable!();
        }
    }

    #[async_trait]
    impl ConnectionHandler for FakeBrokerRepresentation {
        type R = FakeConn;

        async fn connect(
            &self,
            _tls_config: TlsConfig,
            _socks5_proxy: Option<String>,
            _sasl_config: Option<SaslConfig>,
            _max_message_size: usize,
        ) -> Result<Arc<Self::R>> {
            (self.conn)()
        }
    }

    #[tokio::test]
    async fn connect_picks_successful_broker() {
        let brokers = vec![
            // One broker where `connection` always succceeds
            FakeBrokerRepresentation {
                conn: Box::new(|| Ok(Arc::new(FakeConn))),
            },
            // One broker where `connection` always fails (recoverable/fatal doesn't matter)
            FakeBrokerRepresentation {
                conn: Box::new(|| Err(Error::Metadata(arbitrary_recoverable_error()))),
            },
        ];

        // No matter what order the brokers are tried in, this call should find the broker that
        // connects successfully.
        let conn = connect_to_a_broker_with_retry(
            brokers,
            &Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();

        assert_eq!(*conn, FakeConn);
    }
}
