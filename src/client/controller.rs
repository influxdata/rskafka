use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::{
    backoff::{Backoff, BackoffConfig},
    client::{Error, Result},
    connection::{BrokerConnection, BrokerConnector},
    messenger::RequestError,
    protocol::{
        error::Error as ProtocolError,
        messages::{CreateTopicRequest, CreateTopicsRequest},
        primitives::{Int16, Int32, NullableString, String_},
    },
};

#[derive(Debug)]
pub struct ControllerClient {
    brokers: Arc<BrokerConnector>,

    backoff_config: BackoffConfig,

    /// Current broker connection if any
    current_broker: Mutex<Option<BrokerConnection>>,
}

impl ControllerClient {
    pub(super) fn new(brokers: Arc<BrokerConnector>) -> Self {
        Self {
            brokers,
            backoff_config: Default::default(),
            current_broker: Mutex::new(None),
        }
    }

    /// Create a topic
    pub async fn create_topic(
        &self,
        name: impl Into<String> + Send,
        num_partitions: i32,
        replication_factor: i16,
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
            // TODO: Expose as configuration parameter
            timeout_ms: Int32(5_000),
            validate_only: None,
            tagged_fields: None,
        };

        self.maybe_retry("create_topic", || async move {
            let broker = self.get_cached_controller_broker().await?;
            let response = broker.request(request).await?;

            if response.topics.len() != 1 {
                return Err(Error::InvalidResponse(format!(
                    "Expected a single topic in response, got {}",
                    response.topics.len()
                )));
            }

            let topic = response.topics.into_iter().next().unwrap();

            match topic.error {
                None => Ok(()),
                Some(protocol_error) => match topic.error_message {
                    Some(NullableString(Some(msg))) => Err(Error::ServerError(protocol_error, msg)),
                    _ => Err(Error::ServerError(protocol_error, Default::default())),
                },
            }
        })
        .await
    }

    /// Takes a `request_name` and a function yielding a fallible future
    /// and handles certain classes of error
    async fn maybe_retry<R, F, T>(&self, request_name: &str, f: R) -> Result<T>
    where
        R: (Fn() -> F) + Send + Sync,
        F: std::future::Future<Output = Result<T>> + Send,
    {
        let mut backoff = Backoff::new(&self.backoff_config);

        loop {
            let error = match f().await {
                Ok(v) => return Ok(v),
                Err(e) => e,
            };

            match error {
                Error::Request(RequestError::Poisoned(_) | RequestError::IO(_))
                | Error::Connection(_) => self.invalidate_cached_controller_broker().await,
                Error::ServerError(ProtocolError::LeaderNotAvailable, _) => {}
                Error::ServerError(ProtocolError::OffsetNotAvailable, _) => {}
                Error::ServerError(ProtocolError::NotController, _) => {
                    self.invalidate_cached_controller_broker().await;
                }
                _ => {
                    error!(
                        e=%error,
                        request_name,
                        "request encountered fatal error",
                    );
                    return Err(error);
                }
            }

            let backoff = backoff.next();
            info!(
                e=%error,
                request_name,
                backoff_secs=backoff.as_secs(),
                "request encountered non-fatal error - backing off",
            );
            tokio::time::sleep(backoff).await;
        }
    }

    /// Gets a cached [`BrokerConnection`] to any cluster controller.
    async fn get_cached_controller_broker(&self) -> Result<BrokerConnection> {
        let mut current_broker = self.current_broker.lock().await;
        if let Some(broker) = &*current_broker {
            return Ok(Arc::clone(broker));
        }

        info!("Creating new controller broker connection",);

        let controller_id = self.get_controller_id().await?;
        let broker = self.brokers.connect(controller_id).await?.ok_or_else(|| {
            Error::InvalidResponse(format!(
                "Controller {} not found in metadata response",
                controller_id
            ))
        })?;

        *current_broker = Some(Arc::clone(&broker));
        Ok(broker)
    }

    /// Invalidates the cached controller broker.
    ///
    /// The next call to `[ContollerClient::get_cached_controller_broker]` will get a new connection
    pub async fn invalidate_cached_controller_broker(&self) {
        debug!("Invalidating cached controller broker");
        self.current_broker.lock().await.take();
    }

    /// Retrieve the broker ID of the controller
    async fn get_controller_id(&self) -> Result<i32> {
        let metadata = self.brokers.request_metadata(None, Some(vec![])).await?;

        let controller_id = metadata
            .controller_id
            .ok_or_else(|| Error::InvalidResponse("Leader is NULL".to_owned()))?
            .0;

        Ok(controller_id)
    }
}
