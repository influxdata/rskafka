use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::{
    backoff::{Backoff, BackoffConfig, ErrorOrThrottle},
    client::{Error, Result},
    connection::{
        BrokerCache, BrokerCacheGeneration, BrokerConnection, BrokerConnector, MessengerTransport,
        MetadataLookupMode,
    },
    messenger::RequestError,
    protocol::{
        error::Error as ProtocolError,
        messages::{CreateTopicRequest, CreateTopicsRequest, DeleteTopicsRequest},
        primitives::{Array, Int16, Int32, String_},
    },
    throttle::maybe_throttle,
    validation::ExactlyOne,
};

use super::error::RequestContext;

#[derive(Debug)]
pub struct ControllerClient {
    brokers: Arc<BrokerConnector>,

    backoff_config: Arc<BackoffConfig>,

    /// Current broker connection if any
    current_broker: Mutex<(Option<BrokerConnection>, BrokerCacheGeneration)>,
}

impl ControllerClient {
    pub(super) fn new(brokers: Arc<BrokerConnector>, backoff_config: Arc<BackoffConfig>) -> Self {
        Self {
            brokers,
            backoff_config,
            current_broker: Mutex::new((None, BrokerCacheGeneration::START)),
        }
    }

    /// Create a topic
    pub async fn create_topic(
        &self,
        name: impl Into<String> + Send,
        num_partitions: i32,
        replication_factor: i16,
        timeout_ms: i32,
    ) -> Result<()> {
        let request = &CreateTopicsRequest {
            topics: vec![CreateTopicRequest {
                name: String_(name.into()),
                num_partitions: Int32(num_partitions),
                replication_factor: Int16(replication_factor),
                assignments: vec![],
                configs: vec![],
                tagged_fields: None,
            }],
            timeout_ms: Int32(timeout_ms),
            validate_only: None,
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "create_topic", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
            let response = broker
                .request(request)
                .await
                .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;

            maybe_throttle(response.throttle_time_ms)?;

            let topic = response
                .topics
                .exactly_one()
                .map_err(|e| ErrorOrThrottle::Error((Error::exactly_one_topic(e), Some(gen))))?;

            match topic.error {
                None => Ok(()),
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: topic.error_message.and_then(|s| s.0),
                        request: RequestContext::Topic(topic.name.0),
                        response: None,
                        is_virtual: false,
                    },
                    Some(gen),
                ))),
            }
        })
        .await?;

        // Refresh the cache now there is definitely a new topic to observe.
        let _ = self.brokers.refresh_metadata().await;

        Ok(())
    }

    /// Delete a topic
    pub async fn delete_topic(
        &self,
        name: impl Into<String> + Send,
        timeout_ms: i32,
    ) -> Result<()> {
        let request = &DeleteTopicsRequest {
            topic_names: Array(Some(vec![String_(name.into())])),
            timeout_ms: Int32(timeout_ms),
            tagged_fields: None,
        };

        maybe_retry(&self.backoff_config, self, "delete_topic", || async move {
            let (broker, gen) = self
                .get()
                .await
                .map_err(|e| ErrorOrThrottle::Error((e, None)))?;
            let response = broker
                .request(request)
                .await
                .map_err(|e| ErrorOrThrottle::Error((e.into(), Some(gen))))?;

            maybe_throttle(response.throttle_time_ms)?;

            let topic = response
                .responses
                .exactly_one()
                .map_err(|e| ErrorOrThrottle::Error((Error::exactly_one_topic(e), Some(gen))))?;

            match topic.error {
                None => Ok(()),
                Some(protocol_error) => Err(ErrorOrThrottle::Error((
                    Error::ServerError {
                        protocol_error,
                        error_message: topic.error_message.and_then(|s| s.0),
                        request: RequestContext::Topic(topic.name.0),
                        response: None,
                        is_virtual: false,
                    },
                    Some(gen),
                ))),
            }
        })
        .await?;

        // Refresh the cache now there is definitely a new topic to observe.
        let _ = self.brokers.refresh_metadata().await;

        Ok(())
    }

    /// Retrieve the broker ID of the controller
    async fn get_controller_id(&self) -> Result<i32> {
        // Request an uncached, fresh copy of the metadata.
        let (metadata, _gen) = self
            .brokers
            .request_metadata(&MetadataLookupMode::ArbitraryBroker, Some(vec![]))
            .await?;

        let controller_id = metadata
            .controller_id
            .ok_or_else(|| Error::InvalidResponse("Leader is NULL".to_owned()))?
            .0;

        Ok(controller_id)
    }
}

/// Caches the cluster controller broker.
impl BrokerCache for &ControllerClient {
    type R = MessengerTransport;
    type E = Error;

    async fn get(&self) -> Result<(Arc<Self::R>, BrokerCacheGeneration)> {
        let mut current_broker = self.current_broker.lock().await;
        if let Some(broker) = &current_broker.0 {
            return Ok((Arc::clone(broker), current_broker.1));
        }

        info!("Creating new controller broker connection",);

        let controller_id = self.get_controller_id().await?;
        let broker = self.brokers.connect(controller_id).await?.ok_or_else(|| {
            Error::InvalidResponse(format!(
                "Controller {} not found in metadata response",
                controller_id
            ))
        })?;

        current_broker.0 = Some(Arc::clone(&broker));
        current_broker.1.bump();

        Ok((broker, current_broker.1))
    }

    async fn invalidate(&self, reason: &'static str, gen: BrokerCacheGeneration) {
        let mut guard = self.current_broker.lock().await;

        if guard.1 != gen {
            // stale request
            debug!(
                reason,
                current_gen = guard.1.get(),
                request_gen = gen.get(),
                "stale invalidation request for arbitrary broker cache",
            );
            return;
        }

        info!(reason, "Invalidating cached controller broker",);
        guard.0.take();
    }
}

/// Takes a `request_name` and a function yielding a fallible future
/// and handles certain classes of error
async fn maybe_retry<B, R, F, T>(
    backoff_config: &BackoffConfig,
    broker_cache: B,
    request_name: &str,
    f: R,
) -> Result<T>
where
    B: BrokerCache,
    R: (Fn() -> F) + Send + Sync,
    F: std::future::Future<
            Output = Result<T, ErrorOrThrottle<(Error, Option<BrokerCacheGeneration>)>>,
        > + Send,
{
    let mut backoff = Backoff::new(backoff_config);

    backoff
        .retry_with_backoff(request_name, || async {
            let (error, cache_gen) = match f().await {
                Ok(v) => {
                    return ControlFlow::Break(Ok(v));
                }
                Err(ErrorOrThrottle::Throttle(t)) => {
                    return ControlFlow::Continue(ErrorOrThrottle::Throttle(t));
                }
                Err(ErrorOrThrottle::Error(e)) => e,
            };

            match error {
                // broken connection
                Error::Request(RequestError::Poisoned(_) | RequestError::IO(_))
                | Error::Connection(_) => {
                    if let Some(cache_gen) = cache_gen {
                        broker_cache
                            .invalidate("controller client: connection broken", cache_gen)
                            .await
                    }
                }

                // our broker is actually not the controller
                Error::ServerError {
                    protocol_error: ProtocolError::NotController,
                    ..
                } => {
                    if let Some(cache_gen) = cache_gen {
                        broker_cache
                            .invalidate(
                                "controller client: server error: not controller",
                                cache_gen,
                            )
                            .await;
                    }
                }

                // fatal
                _ => {
                    error!(
                        e=%error,
                        request_name,
                        "request encountered fatal error",
                    );
                    return ControlFlow::Break(Err(error));
                }
            }
            ControlFlow::Continue(ErrorOrThrottle::Error(error))
        })
        .await
        .map_err(Error::RetryFailed)?
}
